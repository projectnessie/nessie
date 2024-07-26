/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.projectnessie.quarkus.config.QuarkusStoreConfig.CONFIG_CACHE_INVALIDATIONS_SERVICE_NAMES;
import static org.projectnessie.quarkus.config.QuarkusStoreConfig.CONFIG_CACHE_INVALIDATIONS_VALID_TOKENS;
import static org.projectnessie.quarkus.config.QuarkusStoreConfig.NESSIE_VERSION_STORE_PERSIST;
import static org.projectnessie.server.distcache.CacheInvalidationReceiver.NESSIE_CACHE_INVALIDATION_TOKEN_HEADER;
import static org.projectnessie.server.distcache.CacheInvalidations.CacheInvalidationEvictObj.cacheInvalidationEvictObj;
import static org.projectnessie.server.distcache.CacheInvalidations.cacheInvalidations;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.quarkus.config.QuarkusStoreConfig;
import org.projectnessie.quarkus.providers.ServerInstanceId;
import org.projectnessie.server.distcache.CacheInvalidations;
import org.projectnessie.server.distcache.CacheInvalidations.CacheInvalidation;
import org.projectnessie.server.distcache.CacheInvalidations.CacheInvalidationEvictReference;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

@QuarkusTest
@TestProfile(value = TestCacheInvalidation.Profile.class)
public class TestCacheInvalidation {

  @Inject QuarkusStoreConfig storeConfig;
  @Inject @ServerInstanceId String serverInstanceId;
  @Inject Persist persist;

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  @AfterEach
  public void afterEachAssert() {
    soft.assertAll();
  }

  URI invalidationUri(String serverInstanceId) throws Exception {
    int managementPort = Integer.getInteger("quarkus.management.port");
    return new URI(
        "http",
        null,
        "localhost",
        managementPort,
        storeConfig.cacheInvalidationUri(),
        serverInstanceId != null ? "sender=" + serverInstanceId : null,
        null);
  }

  /** Verify that remote cache invalidation endpoints are invoked. */
  @Test
  @EnabledOnOs(OS.LINUX) // We bind a port to a 127.0.0.x IP address, which only works on Linux
  public void cacheInvalidationSubmission() throws Exception {
    AtomicReference<String> body = new AtomicReference<>();
    AtomicReference<URI> reqUri = new AtomicReference<>();

    // Although the "real" management server binds to the expected random port, as exposed via the
    // 'quarkus.management.port' system property, the configuration value that is passed to
    // CacheInvalidationSender's c'tor is 9000. Since we expect that all instances listen to the
    // same port, we have to bind our "test server" below to port 9000.
    int managementPort = 9000; // Integer.getInteger("quarkus.management.port");

    Semaphore sem = new Semaphore(0);
    try (HttpTestServer ignored =
        new HttpTestServer(
            // 242 ... just because
            new InetSocketAddress("127.0.0.242", managementPort),
            storeConfig.cacheInvalidationUri(),
            exchange -> {
              body.set(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
              reqUri.set(exchange.getRequestURI());
              exchange.sendResponseHeaders(204, 0);
              sem.release();
            })) {

      persist.addReference(
          Reference.reference("foo/bar/baz", ObjId.randomObjId(), false, 42L, null));

      soft.assertThat(sem.tryAcquire(30, TimeUnit.SECONDS)).isTrue();

      soft.assertThat(reqUri.get())
          .extracting(URI::getPath)
          .isEqualTo(storeConfig.cacheInvalidationUri());
      soft.assertThat(reqUri.get())
          .extracting(URI::getQuery)
          .isEqualTo("sender=" + serverInstanceId);

      soft.assertThat(new ObjectMapper().readValue(body.get(), CacheInvalidations.class))
          .extracting(CacheInvalidations::invalidations, list(CacheInvalidation.class))
          .first()
          .extracting(CacheInvalidationEvictReference.class::cast)
          .extracting(
              CacheInvalidations.CacheInvalidationEvictReference::refName,
              CacheInvalidations.CacheInvalidationEvictReference::repoId,
              CacheInvalidations.CacheInvalidationEvictReference::type)
          .containsExactly("foo/bar/baz", "", "ref");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "token-invalid"})
  public void cacheInvalidationInvalidToken(String token) throws Exception {
    HttpURLConnection urlConn = (HttpURLConnection) invalidationUri(null).toURL().openConnection();
    urlConn.setRequestMethod("POST");
    if (!token.isEmpty()) {
      urlConn.addRequestProperty(NESSIE_CACHE_INVALIDATION_TOKEN_HEADER, token);
    }
    urlConn.connect();

    soft.assertThat(urlConn.getResponseCode()).isEqualTo(400);
    soft.assertThat(urlConn.getResponseMessage()).isEqualTo("Invalid token");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "text/plain"})
  public void cacheInvalidationNoContentType(String contentType) throws Exception {
    HttpURLConnection urlConn = (HttpURLConnection) invalidationUri(null).toURL().openConnection();
    urlConn.setRequestMethod("POST");
    urlConn.addRequestProperty(
        NESSIE_CACHE_INVALIDATION_TOKEN_HEADER,
        storeConfig.cacheInvalidationValidTokens().get().get(0));
    if (!contentType.isEmpty()) {
      urlConn.addRequestProperty("content-type", contentType);
    }
    urlConn.connect();

    soft.assertThat(urlConn.getResponseCode()).isEqualTo(415);
    soft.assertThat(urlConn.getResponseMessage()).isEqualTo("Unsupported media type");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "{}"})
  public void cacheInvalidationEmptyBody(String body) throws Exception {
    HttpURLConnection urlConn = (HttpURLConnection) invalidationUri(null).toURL().openConnection();
    urlConn.setRequestMethod("POST");
    urlConn.addRequestProperty(
        NESSIE_CACHE_INVALIDATION_TOKEN_HEADER,
        storeConfig.cacheInvalidationValidTokens().get().get(0));
    urlConn.addRequestProperty("content-type", "application/json");
    urlConn.setDoOutput(true);
    urlConn.connect();
    try (OutputStream outputStream = urlConn.getOutputStream()) {
      outputStream.write(body.getBytes(StandardCharsets.UTF_8));
    }

    soft.assertThat(urlConn.getResponseCode()).isEqualTo(204);
    soft.assertThat(urlConn.getResponseMessage()).isEqualTo("No content");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void cacheInvalidationSomeContent(boolean sameSender) throws Exception {
    StringObj obj1 =
        stringData(
            "foo",
            Compression.NONE,
            null,
            Collections.emptyList(),
            ByteString.copyFromUtf8("hello"));
    StringObj obj2 =
        stringData(
            "foo",
            Compression.NONE,
            null,
            Collections.emptyList(),
            ByteString.copyFromUtf8("world"));
    persist.storeObj(obj1);
    persist.storeObj(obj2);

    ObjId id1 = requireNonNull(obj1.id());
    ObjId id2 = requireNonNull(obj2.id());

    persist.fetchObj(id1);
    persist.fetchObj(id2);

    soft.assertThat(persist.getImmediate(id1)).isEqualTo(obj1);
    soft.assertThat(persist.getImmediate(id2)).isEqualTo(obj2);

    HttpURLConnection urlConn =
        (HttpURLConnection)
            invalidationUri(sameSender ? serverInstanceId : null).toURL().openConnection();
    urlConn.setRequestMethod("POST");
    urlConn.addRequestProperty(
        NESSIE_CACHE_INVALIDATION_TOKEN_HEADER,
        storeConfig.cacheInvalidationValidTokens().get().get(0));
    urlConn.addRequestProperty("content-type", "application/json");
    urlConn.setDoOutput(true);
    urlConn.connect();
    try (OutputStream outputStream = urlConn.getOutputStream()) {
      CacheInvalidations obj =
          cacheInvalidations(
              asList(
                  cacheInvalidationEvictObj("", id1.asByteArray()),
                  cacheInvalidationEvictObj("", id2.asByteArray())));
      new ObjectMapper().writeValue(outputStream, obj);
    }

    soft.assertThat(urlConn.getResponseCode()).isEqualTo(204);
    soft.assertThat(urlConn.getResponseMessage()).isEqualTo("No content");

    if (sameSender) {
      soft.assertThat(persist.getImmediate(id1)).isEqualTo(obj1);
      soft.assertThat(persist.getImmediate(id2)).isEqualTo(obj2);
    } else {
      soft.assertThat(persist.getImmediate(id1)).isNull();
      soft.assertThat(persist.getImmediate(id2)).isNull();
    }
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put(
              NESSIE_VERSION_STORE_PERSIST + '.' + CONFIG_CACHE_INVALIDATIONS_VALID_TOKENS,
              "mytoken")
          .put("quarkus.management.host", "127.0.0.1") // binds to 0.0.0.0 by default
          .put(
              NESSIE_VERSION_STORE_PERSIST + '.' + CONFIG_CACHE_INVALIDATIONS_SERVICE_NAMES,
              "=127.0.0.1,=127.0.0.242")
          .build();
    }
  }
}
