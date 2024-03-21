/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.testing.gcs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

/**
 * Provides a test container for Google Cloud Storage, using the {@code
 * docker.io/fsouza/fake-gcs-server} image.
 *
 * <p>This container must use a static TCP port, see below. To at least mitigate the chance that two
 * concurrently running containers share the same IP:PORT combination, this implementation uses a
 * random IP in the range 127.0.0.1 to 127.255.255.254. This does not work on Windows and Mac.
 *
 * <p>It would be really nice to test GCS using the Fake GCS server and dynamic ports, but that's
 * impossible, because of the GCS upload protocol itself. GCS uploads are initiated using one HTTP
 * request, which returns a {@code Location} header, which contains the URL for the upload that the
 * client must use. Problem is that a Fake server running inside a Docker container cannot infer the
 * dynamically mapped port for this test, because the initial HTTP request does not have a {@code
 * Host} header.
 */
public class GCSContainer extends GenericContainer<GCSContainer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSContainer.class);
  public static final int PORT = 4443;

  private static final String DEFAULT_IMAGE;
  private static final String DEFAULT_TAG;

  static {
    URL resource = GCSContainer.class.getResource("Dockerfile-fake-gcs-server-version");
    Objects.requireNonNull(resource, "Dockerfile-fake-gcs-server-version not found");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      DEFAULT_IMAGE =
          System.getProperty(
              "nessie.testing.fake-gcs-server.image",
              Optional.ofNullable(System.getenv("FAKE_GCS_SERVER_DOCKER_IMAGE"))
                  .orElse(imageTag[0]));
      DEFAULT_TAG =
          System.getProperty(
              "nessie.testing.fake-gcs-server.tag",
              Optional.ofNullable(System.getenv("FAKE_GCS_SERVER_DOCKER_TAG")).orElse(imageTag[1]));
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }

  private final String localAddress;
  private final String oauth2token;
  private final String bucket;
  private final String projectId;

  public GCSContainer() {
    this(null);
  }

  @SuppressWarnings("resource")
  public GCSContainer(String image) {
    super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);

    ThreadLocalRandom rand = ThreadLocalRandom.current();
    boolean isMac = System.getProperty("os.name").toLowerCase().contains("mac");
    boolean isWindows = System.getProperty("os.name").toLowerCase().contains("windows");
    localAddress =
        isMac || isWindows
            ? "127.0.0.1"
            : String.format(
                "127.%d.%d.%d", rand.nextInt(256), rand.nextInt(256), rand.nextInt(1, 255));
    oauth2token = randomString("token");
    bucket = randomString("bucket");
    projectId = randomString("project");

    withNetworkAliases(randomString("fake-gcs"));
    withLogConsumer(c -> LOGGER.info("[FAKE-GCS] {}", c.getUtf8StringWithoutLineEnding()));
    withCommand("-scheme", "http", "-public-host", localAddress, "-log-level", "warn");
    setWaitStrategy(
        new HttpWaitStrategy()
            .forPath("/storage/v1/b")
            .forPort(PORT)
            .withStartupTimeout(Duration.ofMinutes(2)));

    addExposedPort(PORT);

    // STATIC PORT BINDING!
    setPortBindings(singletonList(PORT + ":" + PORT));
  }

  @Override
  public void start() {
    super.start();

    try (Storage storage =
        StorageOptions.http()
            .setHost(baseUri())
            .setCredentials(OAuth2Credentials.create(new AccessToken(oauth2token, null)))
            .setProjectId(projectId)
            .build()
            .getService()) {
      storage.create(BucketInfo.newBuilder(bucket).build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String baseUri() {
    return String.format("http://%s:%d", localAddress(), gcsPort());
  }

  public String localAddress() {
    return localAddress;
  }

  public String oauth2token() {
    return oauth2token;
  }

  public String bucket() {
    return bucket;
  }

  public String bucketUri() {
    return String.format("gs://%s/", bucket);
  }

  public String projectId() {
    return projectId;
  }

  public Storage newStorage() {
    return StorageOptions.http()
        .setHost(baseUri())
        .setCredentials(OAuth2Credentials.create(new AccessToken(oauth2token, null)))
        .setProjectId(projectId)
        .build()
        .getService();
  }

  private int gcsPort() {
    return getMappedPort(PORT);
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }

  public Map<String, String> hadoopConfig() {
    Map<String, String> r = new HashMap<>();
    // See https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md
    r.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    r.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    r.put("fs.gs.storage.root.url", baseUri());
    r.put("fs.gs.project.id", projectId);
    r.put("fs.gs.auth.type", "USER_CREDENTIALS");
    r.put("fs.gs.auth.client.id", "foo");
    r.put("fs.gs.auth.client.secret", "bar");
    r.put("fs.gs.auth.refresh.token", "baz");
    return r;
  }

  public Map<String, String> icebergProperties() {
    Map<String, String> r = new HashMap<>();
    r.put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO");
    r.put("gcs.project-id", projectId);
    r.put("gcs.service.host", baseUri());
    r.put("gcs.oauth2.token", oauth2token);
    return r;
  }
}
