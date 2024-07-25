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
package org.projectnessie.server.distcache;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Collections.emptyList;
import static org.projectnessie.quarkus.config.QuarkusStoreConfig.CONFIG_CACHE_INVALIDATIONS_SERVICE_NAMES;
import static org.projectnessie.quarkus.config.QuarkusStoreConfig.CONFIG_CACHE_INVALIDATIONS_VALID_TOKENS;
import static org.projectnessie.quarkus.config.QuarkusStoreConfig.NESSIE_VERSION_STORE_PERSIST;
import static org.projectnessie.server.distcache.CacheInvalidationReceiver.NESSIE_CACHE_INVALIDATION_TOKEN_HEADER;
import static org.projectnessie.server.distcache.CacheInvalidations.CacheInvalidationEvictObj.cacheInvalidationEvictObj;
import static org.projectnessie.server.distcache.CacheInvalidations.CacheInvalidationEvictReference.cacheInvalidationEvictReference;
import static org.projectnessie.server.distcache.CacheInvalidations.cacheInvalidations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.quarkus.runtime.Startup;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.nessie.networktools.AddressResolver;
import org.projectnessie.quarkus.config.QuarkusStoreConfig;
import org.projectnessie.quarkus.providers.ServerInstanceId;
import org.projectnessie.server.distcache.CacheInvalidations.CacheInvalidation;
import org.projectnessie.versioned.storage.cache.DistributedCacheInvalidation;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Startup
public class CacheInvalidationSender implements DistributedCacheInvalidation {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheInvalidationSender.class);

  private final Vertx vertx;
  private final long serviceNameLookupIntervalMillis;

  private final HttpClient httpClient;
  private final AddressResolver addressResolver;

  private final List<String> serviceNames;
  private final int httpPort;
  private final String invalidationUri;
  private final long requestTimeout;

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Lock lock = new ReentrantLock();
  private final int batchSize;
  private final BlockingQueue<CacheInvalidation> invalidations = new LinkedBlockingQueue<>();
  private boolean triggered;
  private final String token;

  /** Contains the IPv4/6 addresses resolved from {@link #serviceNames}. */
  private volatile List<String> resolvedAddresses = emptyList();

  @Inject
  public CacheInvalidationSender(
      Vertx vertx,
      QuarkusStoreConfig config,
      @ConfigProperty(name = "quarkus.management.port") int httpPort,
      @ServerInstanceId String serverInstanceId) {
    this.vertx = vertx;

    this.addressResolver = new AddressResolver(vertx);
    this.requestTimeout =
        config
            .cacheInvalidationRequestTimeout()
            .orElse(Duration.of(30, ChronoUnit.SECONDS))
            .toMillis();
    this.httpClient = vertx.createHttpClient();
    this.serviceNames = config.cacheInvalidationServiceNames().orElse(emptyList());
    this.httpPort = httpPort;
    this.invalidationUri = config.cacheInvalidationUri() + "?sender=" + serverInstanceId;
    this.serviceNameLookupIntervalMillis =
        config.cacheInvalidationServiceNameLookupInterval().toMillis();
    this.batchSize = config.cacheInvalidationBatchSize();
    this.token = config.cacheInvalidationValidTokens().map(l -> l.get(0)).orElse(null);
    if (!serviceNames.isEmpty()) {
      try {
        LOGGER.info("Sending remote cache invalidations to service name(s) {}", serviceNames);
        updateServiceNames().toCompletionStage().toCompletableFuture().get();
        if (config.cacheInvalidationValidTokens().isEmpty()) {
          LOGGER.warn(
              "No token configured for cache invalidation messages - will not send any invalidation message. You need to configure the token(s) via {}.{}",
              NESSIE_VERSION_STORE_PERSIST,
              CONFIG_CACHE_INVALIDATIONS_VALID_TOKENS);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to resolve service names " + serviceNames + " for remote cache invalidations",
            (e instanceof ExecutionException) ? e.getCause() : e);
      }
    } else if (token != null) {
      LOGGER.warn(
          "No service names are configured to send cache invalidation messages to - will not send any invalidation message. You need to configure the service name(s) via {}.{}",
          NESSIE_VERSION_STORE_PERSIST,
          CONFIG_CACHE_INVALIDATIONS_SERVICE_NAMES);
    }
  }

  private Future<List<String>> updateServiceNames() {
    Set<String> previous = new HashSet<>(resolvedAddresses);
    return resolveServiceNames(serviceNames)
        .map(
            all ->
                all.stream().filter(adr -> !AddressResolver.LOCAL_ADDRESSES.contains(adr)).toList())
        .onSuccess(
            all -> {
              // refresh addresses regularly
              scheduleServiceNameResolution();

              Set<String> resolved = new HashSet<>(all);
              if (!resolved.equals(previous)) {
                LOGGER.info(
                    "Service names for remote cache invalidations {} now resolve to {}",
                    serviceNames,
                    all);
              }

              updateResolvedAddresses(all);
            })
        .onFailure(
            t -> {
              // refresh addresses regularly
              scheduleServiceNameResolution();

              LOGGER.warn("Failed to resolve service names: {}", t.toString());
            });
  }

  @VisibleForTesting
  void updateResolvedAddresses(List<String> all) {
    resolvedAddresses = all;
  }

  private void scheduleServiceNameResolution() {
    vertx.setTimer(serviceNameLookupIntervalMillis, x -> updateServiceNames());
  }

  @VisibleForTesting
  Future<List<String>> resolveServiceNames(List<String> serviceNames) {
    return addressResolver.resolveAll(serviceNames);
  }

  void enqueue(CacheInvalidation invalidation) {
    if (serviceNames.isEmpty() || token == null) {
      // Don't do anything if there are no targets to send invalidations to or whether no token has
      // been configured.
      return;
    }

    lock.lock();
    try {
      invalidations.add(invalidation);

      if (!triggered) {
        LOGGER.trace("Triggered invalidation submission");
        vertx.executeBlocking(this::sendInvalidations);
        triggered = true;
      }
    } finally {
      lock.unlock();
    }
  }

  private Void sendInvalidations() {
    List<CacheInvalidation> batch = new ArrayList<>(batchSize);
    try {
      while (true) {
        lock.lock();
        try {
          invalidations.drainTo(batch, 100);
          if (batch.isEmpty()) {
            LOGGER.trace("Done sending invalidations");
            triggered = false;
            break;
          }
        } finally {
          lock.unlock();
        }
        submit(batch, resolvedAddresses);
        batch = new ArrayList<>(batchSize);
      }
    } finally {
      // Handle the very unlikely case that the call to submit() failed and we cannot be sure that
      // the current batch was submitted.
      if (!batch.isEmpty()) {
        lock.lock();
        try {
          invalidations.addAll(batch);
          triggered = false;
        } finally {
          lock.unlock();
        }
      }
    }
    return null;
  }

  @VisibleForTesting
  List<Future<Map.Entry<HttpClientResponse, Buffer>>> submit(
      List<CacheInvalidation> batch, List<String> resolvedAddresses) {
    LOGGER.trace("Submitting {} invalidations", batch.size());

    String json;
    try {
      json = objectMapper.writeValueAsString(cacheInvalidations(batch));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    List<Future<Map.Entry<HttpClientResponse, Buffer>>> futures =
        new ArrayList<>(resolvedAddresses.size());
    for (String address : resolvedAddresses) {
      futures.add(
          httpClient
              .request(HttpMethod.POST, httpPort, address, invalidationUri)
              .compose(
                  req ->
                      req.putHeader("Content-Type", APPLICATION_JSON)
                          .putHeader(NESSIE_CACHE_INVALIDATION_TOKEN_HEADER, token)
                          .send(json))
              .compose(resp -> resp.body().map(b -> Map.entry(resp, b)))
              .timeout(requestTimeout, TimeUnit.MILLISECONDS)
              .onComplete(
                  success -> {
                    HttpClientResponse resp = success.getKey();
                    int statusCode = resp.statusCode();
                    if (statusCode != 200 && statusCode != 204) {
                      LOGGER.warn(
                          "{} cache invalidations could not be sent to {}:{}{} - HTTP {}/{} - body: {}",
                          batch.size(),
                          address,
                          httpPort,
                          invalidationUri,
                          statusCode,
                          resp.statusMessage(),
                          success.getValue());
                    } else {
                      LOGGER.trace(
                          "{} cache invalidations sent to {}:{}", batch.size(), address, httpPort);
                    }
                  },
                  failure -> {
                    if (failure instanceof SocketException
                        || failure instanceof UnknownHostException) {
                      LOGGER.warn(
                          "Technical network issue sending cache invalidations to {}:{}{} : {}",
                          address,
                          httpPort,
                          invalidationUri,
                          failure.getMessage());
                    } else {
                      LOGGER.error(
                          "Technical failure sending cache invalidations to {}:{}{}",
                          address,
                          httpPort,
                          invalidationUri,
                          failure);
                    }
                  }));
    }
    return futures;
  }

  @Override
  public void evictReference(String repositoryId, String refName) {
    enqueue(cacheInvalidationEvictReference(repositoryId, refName));
  }

  @Override
  public void evictObj(String repositoryId, ObjId objId) {
    enqueue(cacheInvalidationEvictObj(repositoryId, objId.asByteArray()));
  }
}
