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
package org.projectnessie.nessie.testing.trino;

import static java.lang.String.format;

import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.client.StatementClientFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.Base58;

public final class TrinoContainer extends GenericContainer<TrinoContainer> implements TrinoAccess {

  private static final Logger LOGGER = LoggerFactory.getLogger(TrinoContainer.class);

  private static final int TRINO_PORT = 8080;
  private static final String STATUS_ENDPOINT = "/v1/info";

  public static final String HOST_FROM_CONTAINER = "host.nessie-testing.internal";

  private volatile OkHttpClient httpClient;

  private final Properties logProperties = new Properties();
  private final Properties trinoConfig = new Properties();

  @SuppressWarnings("unused")
  public TrinoContainer() {
    this(null);
  }

  @SuppressWarnings("resource")
  public TrinoContainer(String image) {
    super(
        ContainerSpecHelper.builder()
            .name("trino")
            .containerClass(TrinoContainer.class)
            .build()
            .dockerImageName(image));

    trinoConfig.put("coordinator", "true");
    trinoConfig.put("node-scheduler.include-coordinator", "true");
    trinoConfig.put("http-server.http.port", Integer.toString(TRINO_PORT));
    trinoConfig.put("discovery.uri", format("http://localhost:%d", TRINO_PORT));
    trinoConfig.put("catalog.management", "${ENV:CATALOG_MANAGEMENT}");

    withNetworkAliases(randomString("trino"));
    withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("Trino"));
    addExposedPort(TRINO_PORT);
    withExtraHost(HOST_FROM_CONTAINER, "host-gateway");
    withCopyToContainer(new PropertiesTransferable(logProperties), "/etc/trino/log.properties");
    withCopyToContainer(new PropertiesTransferable(trinoConfig), "/etc/trino/config.properties");
    withStartupAttempts(3);
    setWaitStrategy(
        new WaitAllStrategy()
            .withStrategy(
                new HttpWaitStrategy()
                    .forPort(TRINO_PORT)
                    .forPath(STATUS_ENDPOINT)
                    .withStartupTimeout(Duration.ofMinutes(2)))
            .withStrategy(
                new LogMessageWaitStrategy()
                    .withRegEx(".*SERVER STARTED.*")
                    .withStartupTimeout(Duration.ofMinutes(2))));
  }

  static final class PropertiesTransferable implements Transferable {
    private final Properties props;

    PropertiesTransferable(Properties props) {
      this.props = props;
    }

    @Override
    public byte[] getBytes() {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        props.store(out, "");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return out.toByteArray();
    }

    @Override
    public long getSize() {
      return getBytes().length;
    }
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }

  /**
   * Functionality to add a catalog to Trino via a properties in the Trino container's {@code
   * /etc/trino/catalogs/} folder.
   */
  public TrinoContainer withCatalog(String name, Map<String, String> properties) {
    Properties props = new Properties();
    props.putAll(properties);

    return withCatalog(name, props);
  }

  public TrinoContainer withCatalog(String name, Properties properties) {
    return withCopyToContainer(
        new PropertiesTransferable(properties), format("/etc/trino/catalog/%s.properties", name));
  }

  public TrinoContainer withGlobalConfig(Map<String, String> config) {
    trinoConfig.putAll(config);
    return this;
  }

  public TrinoContainer withLogProperty(String key, String value) {
    logProperties.put(key, value);
    return this;
  }

  public TrinoContainer withLogProperty(Map<String, String> logProperties) {
    this.logProperties.putAll(logProperties);
    return this;
  }

  @Override
  public String hostPort() {
    return format("%s:%d", getHost(), getMappedPort(TRINO_PORT));
  }

  @Override
  public StatementClient statementClient(String query) {
    if (httpClient == null) {
      synchronized (this) {
        if (httpClient == null) {
          httpClient = new OkHttpClient();
        }
      }
    }
    ClientSession session =
        ClientSession.builder()
            .server(URI.create(format("http://%s:%d/", getHost(), getMappedPort(TRINO_PORT))))
            .timeZone(ZoneId.of("UTC"))
            .user(Optional.of("user"))
            .source("foo")
            .build();

    return StatementClientFactory.newStatementClient(httpClient, session, query, Optional.empty());
  }

  @Override
  public TrinoResults query(String query) {
    return new TrinoResultsImpl(statementClient(query));
  }

  @Override
  public List<List<Object>> queryResults(String query) {
    try (TrinoResults trinoResults = query(query)) {
      return trinoResults.allRows();
    }
  }

  @Override
  public void close() {
    try {
      try {
        try {
          if (httpClient != null) {
            try {
              httpClient.connectionPool().evictAll();
            } finally {
              Cache cache = httpClient.cache();
              if (cache != null) {
                cache.close();
              }
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } finally {
        stop();
      }
    } finally {
      httpClient = null;
    }
  }
}
