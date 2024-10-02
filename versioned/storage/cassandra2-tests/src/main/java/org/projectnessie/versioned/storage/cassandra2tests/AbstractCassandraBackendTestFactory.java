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
package org.projectnessie.versioned.storage.cassandra2tests;

import static java.lang.String.format;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.projectnessie.versioned.storage.cassandra2.Cassandra2Backend;
import org.projectnessie.versioned.storage.cassandra2.Cassandra2BackendConfig;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractCassandraBackendTestFactory implements BackendTestFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractCassandraBackendTestFactory.class);
  public static final String KEYSPACE_FOR_TEST = "nessie";
  public static final Integer CQL_PORT = 9042;

  private final String dbName;
  private final List<String> args;

  private CassandraContainer container;
  private InetSocketAddress hostAndPort;
  private String localDc;

  AbstractCassandraBackendTestFactory(String dbName, List<String> args) {
    this.dbName = dbName;
    this.args = args;
  }

  @Override
  public Cassandra2Backend createNewBackend() {
    CqlSession client = buildNewClient();
    maybeCreateKeyspace(client);
    return new Cassandra2Backend(
        Cassandra2BackendConfig.builder().client(client).keyspace(KEYSPACE_FOR_TEST).build(), true);
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

    // Disable tablets in ScyllaDB, because those are not compatible with LWTs (yet?). See
    // https://opensource.docs.scylladb.com/stable/architecture/tablets.html#limitations-and-unsupported-features
    String scyllaClause = "scylladb".equals(dbName) ? " AND tablets = {'enabled': false}" : "";

    session.execute(
        format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', %s}%s;",
            KEYSPACE_FOR_TEST, datacenters, scyllaClause));

    session.refreshSchema();
  }

  public CqlSession buildNewClient() {
    return CassandraClientProducer.builder()
        .addContactPoints(hostAndPort)
        .localDc(localDc)
        .build()
        .createClient();
  }

  @Override
  @SuppressWarnings("resource")
  public void start(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    DockerImageName dockerImageName =
        ContainerSpecHelper.builder()
            .name(dbName)
            .containerClass(AbstractCassandraBackendTestFactory.class)
            .build()
            .dockerImageName(null)
            .asCompatibleSubstituteFor("cassandra");

    for (int retry = 0; ; retry++) {
      CassandraContainer c =
          new CassandraContainer(dockerImageName)
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

  public String getKeyspace() {
    return KEYSPACE_FOR_TEST;
  }

  public InetSocketAddress getHostAndPort() {
    return hostAndPort;
  }

  public String getLocalDc() {
    return localDc;
  }

  protected abstract void configureContainer(CassandraContainer c);

  @Override
  public void start() {
    start(Optional.empty());
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

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of(
        "quarkus.cassandra.contact-points",
        String.format("%s:%d", getHostAndPort().getHostName(), getHostAndPort().getPort()),
        "quarkus.cassandra.local-datacenter",
        getLocalDc(),
        "quarkus.cassandra.keyspace",
        getKeyspace());
  }
}
