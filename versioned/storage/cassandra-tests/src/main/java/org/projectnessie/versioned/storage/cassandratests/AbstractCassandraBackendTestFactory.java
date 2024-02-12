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
package org.projectnessie.versioned.storage.cassandratests;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testcontainers.containers.CassandraContainer.CQL_PORT;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.cassandra.CassandraBackend;
import org.projectnessie.versioned.storage.cassandra.CassandraBackendConfig;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractCassandraBackendTestFactory implements BackendTestFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractCassandraBackendTestFactory.class);
  public static final String KEYSPACE_FOR_TEST = "nessie";

  private final String dbName;
  private final List<String> args;

  private CassandraContainer<?> container;
  private InetSocketAddress hostAndPort;
  private String localDc;

  AbstractCassandraBackendTestFactory(String dbName, List<String> args) {
    this.dbName = dbName;
    this.args = args;
  }

  @Override
  public CassandraBackend createNewBackend() {
    CqlSession client = buildNewClient();
    maybeCreateKeyspace(client);
    return new CassandraBackend(
        CassandraBackendConfig.builder().client(client).keyspace(KEYSPACE_FOR_TEST).build(), true);
  }

  public void maybeCreateKeyspace() {
    try (CqlSession client = buildNewClient()) {
      maybeCreateKeyspace(client);
    }
  }

  private void maybeCreateKeyspace(CqlSession session) {
    int replicationFactor = 1;

    Metadata metadata = session.getMetadata();

    String datacenters =
        metadata.getNodes().values().stream()
            .map(Node::getDatacenter)
            .distinct()
            .map(dc -> format("'%s': %d", dc, replicationFactor))
            .collect(Collectors.joining(", "));

    session.execute(
        format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', %s};",
            KEYSPACE_FOR_TEST, datacenters));
    session.refreshSchema();
  }

  public CqlSession buildNewClient() {
    return CassandraClientProducer.builder()
        .addContactPoints(hostAndPort)
        .localDc(localDc)
        .build()
        .createClient();
  }

  @SuppressWarnings("resource")
  public void startTestNode(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    String image = dockerImage(dbName);

    DockerImageName dockerImageName =
        DockerImageName.parse(image).asCompatibleSubstituteFor("cassandra");
    for (int retry = 0; ; retry++) {
      CassandraContainer<?> c =
          new CassandraContainer<>(dockerImageName)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withCommand(args.toArray(new String[0]));
      configureContainer(c);
      containerNetworkId.ifPresent(c::withNetworkMode);
      try {
        c.start();
        container = c;
        break;
      } catch (ContainerLaunchException e) {
        c.close();
        if (e.getCause() != null && retry < 3) {
          LOGGER.warn("Launch of container {} failed, will retry...", c.getDockerImageName(), e);
          continue;
        }
        LOGGER.error("Launch of container {} failed", c.getDockerImageName(), e);
        throw new RuntimeException(e);
      }
    }

    int port = containerNetworkId.isPresent() ? CQL_PORT : container.getMappedPort(CQL_PORT);
    String host =
        containerNetworkId.isPresent()
            ? container.getCurrentContainerInfo().getConfig().getHostName()
            : container.getHost();

    localDc = container.getLocalDatacenter();

    hostAndPort = InetSocketAddress.createUnresolved(host, port);
  }

  protected static String dockerImage(String dbName) {
    URL resource =
        AbstractCassandraBackendTestFactory.class.getResource("Dockerfile-" + dbName + "-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      String imageName = imageTag[0];
      String version =
          System.getProperty("it.nessie.container." + dbName + "-local.tag", imageTag[1]);
      return imageName + ':' + version;
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }

  public String getKeyspace() {
    return KEYSPACE_FOR_TEST;
  }

  public InetSocketAddress getHostAndPort() {
    return hostAndPort;
  }

  public String getLocalDc() {
    return localDc;
  }

  protected abstract void configureContainer(CassandraContainer<?> c);

  @Override
  public void start() {
    startTestNode(Optional.empty());
  }

  @Override
  public void stop() {
    try {
      if (container != null) {
        container.stop();
      }
    } finally {
      container = null;
    }
  }
}
