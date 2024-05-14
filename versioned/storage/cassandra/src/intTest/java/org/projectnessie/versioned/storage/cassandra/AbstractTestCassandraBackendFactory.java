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
package org.projectnessie.versioned.storage.cassandra;

import static java.lang.String.format;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.TABLE_REFS;
import static org.projectnessie.versioned.storage.cassandratests.AbstractCassandraBackendTestFactory.KEYSPACE_FOR_TEST;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.time.Duration;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.cassandratests.AbstractCassandraBackendTestFactory;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractTestCassandraBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void productionLike() throws Exception {
    AbstractCassandraBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try {
      BackendFactory<CassandraBackendConfig> factory =
          PersistLoader.findFactoryByName(CassandraBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(CassandraBackendFactory.class);

      try (CqlSession client = testFactory.buildNewClient()) {
        setupKeyspace(client);

        RepositoryDescription repoDesc;
        try (Backend backend = factory.buildBackend(buildConfig(client))) {
          soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(CassandraPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          repoDesc = repositoryLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }

        try (Backend backend = factory.buildBackend(buildConfig(client))) {
          soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(CassandraPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
        }
      }
    } finally {
      testFactory.stop();
    }
  }

  @Test
  public void backendTestFactory() throws Exception {
    AbstractCassandraBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try {
      BackendFactory<CassandraBackendConfig> factory =
          PersistLoader.findFactoryByName(CassandraBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(CassandraBackendFactory.class);

      RepositoryDescription repoDesc;
      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(CassandraPersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        repoDesc = repositoryLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(CassandraPersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }
  }

  static void executeDDL(CqlSession client, String cql) {
    client.execute(SimpleStatement.newInstance(cql).setTimeout(Duration.ofSeconds(30)));
  }

  @Test
  public void incompatibleTableSchema() throws Exception {
    AbstractCassandraBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try (CqlSession client = testFactory.buildNewClient()) {
      setupKeyspace(client);

      BackendFactory<CassandraBackendConfig> factory =
          PersistLoader.findFactoryByName(CassandraBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(CassandraBackendFactory.class);
      try (Backend backend = factory.buildBackend(buildConfig(client))) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        backend.setupSchema();
      }

      executeDDL(client, "DROP TABLE IF EXISTS nessie." + TABLE_REFS);
      executeDDL(client, "DROP TABLE IF EXISTS nessie." + TABLE_OBJS);

      executeDDL(client, "CREATE TABLE nessie." + TABLE_REFS + " (foobarbaz VARCHAR PRIMARY KEY)");

      try (Backend backend = factory.buildBackend(buildConfig(client))) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        soft.assertThatIllegalStateException()
            .isThrownBy(backend::setupSchema)
            .withMessageStartingWith(
                "Expected primary key columns {repo=TEXT, ref_name=TEXT} "
                    + "do not match existing primary key columns {foobarbaz=TEXT} for table '"
                    + TABLE_REFS
                    + "'. DDL template:\nCREATE TABLE nessie."
                    + TABLE_REFS);
      }

      executeDDL(client, "DROP TABLE IF EXISTS nessie." + TABLE_REFS);
      executeDDL(client, "DROP TABLE IF EXISTS nessie." + TABLE_OBJS);

      executeDDL(
          client,
          "CREATE TABLE nessie."
              + TABLE_REFS
              + " ("
              + COL_REPO_ID
              + " VARCHAR, "
              + COL_REFS_NAME
              + " VARCHAR, meep VARCHAR, boo BIGINT, PRIMARY KEY (("
              + COL_REPO_ID
              + ", "
              + COL_REFS_NAME
              + ")))");

      try (Backend backend = factory.buildBackend(buildConfig(client))) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        soft.assertThatIllegalStateException()
            .isThrownBy(backend::setupSchema)
            .withMessageStartingWith(
                "The database table "
                    + TABLE_REFS
                    + " is missing mandatory columns created_at,deleted,ext_info,pointer,prev_ptr.\n"
                    + "Found columns : boo,meep,ref_name,repo\n"
                    + "Expected columns : ")
            .withMessageContaining("DDL template:\nCREATE TABLE nessie." + TABLE_REFS);
      }

      executeDDL(client, "DROP TABLE IF EXISTS nessie." + TABLE_REFS);
      executeDDL(client, "DROP TABLE IF EXISTS nessie." + TABLE_OBJS);

      executeDDL(client, "CREATE TABLE nessie." + TABLE_OBJS + " (foobarbaz VARCHAR PRIMARY KEY)");

      try (Backend backend = factory.buildBackend(buildConfig(client))) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        soft.assertThatIllegalStateException()
            .isThrownBy(backend::setupSchema)
            .withMessageStartingWith(
                "Expected primary key columns {repo=TEXT, obj_id=ASCII} "
                    + "do not match existing primary key columns {foobarbaz=TEXT} for table '"
                    + TABLE_OBJS
                    + "'. DDL template:\nCREATE TABLE nessie."
                    + TABLE_OBJS);
      }
    } finally {
      testFactory.stop();
    }
  }

  private static ImmutableCassandraBackendConfig buildConfig(CqlSession client) {
    return CassandraBackendConfig.builder().client(client).build();
  }

  private void setupKeyspace(CqlSession client) {
    executeDDL(client, format("DROP KEYSPACE IF EXISTS %s", KEYSPACE_FOR_TEST));
    executeDDL(
        client,
        format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', %s}",
            KEYSPACE_FOR_TEST,
            client.getMetadata().getNodes().values().stream()
                .map(Node::getDatacenter)
                .distinct()
                .map(dc -> format("'%s': 1", dc))
                .collect(Collectors.joining(", "))));
  }

  protected abstract AbstractCassandraBackendTestFactory testFactory();
}
