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

import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.TABLE_REFS;
import static org.projectnessie.versioned.storage.common.logic.Logics.setupLogic;

import com.datastax.oss.driver.api.core.CqlSession;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.SetupLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractTestCassandraBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  static StoreConfig DEFAULT_CONFIG = new StoreConfig() {};

  @Test
  public void productionLike() throws Exception {
    AbstractCassandraBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try {
      BackendFactory<CassandraBackendConfig> factory =
          PersistLoader.findFactoryByName(CassandraBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(CassandraBackendFactory.class);

      try (CqlSession client = testFactory.buildNewClient()) {
        RepositoryDescription repoDesc;
        try (Backend backend = factory.buildBackend(buildConfig(client))) {
          soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(CassandraPersistFactory.class);
          Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
          soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

          SetupLogic setupLogic = setupLogic(persist);
          setupLogic.initialize("initializeAgain");
          repoDesc = setupLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }

        try (Backend backend = factory.buildBackend(buildConfig(client))) {
          soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(CassandraPersistFactory.class);
          Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
          soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

          SetupLogic setupLogic = setupLogic(persist);
          setupLogic.initialize("initializeAgain");
          soft.assertThat(setupLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
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
        Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
        soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

        SetupLogic setupLogic = setupLogic(persist);
        setupLogic.initialize("initializeAgain");
        repoDesc = setupLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(CassandraPersistFactory.class);
        Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
        soft.assertThat(persist).isNotNull().isInstanceOf(CassandraPersist.class);

        SetupLogic setupLogic = setupLogic(persist);
        setupLogic.initialize("initializeAgain");
        soft.assertThat(setupLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }
  }

  @Test
  public void incompatibleTableSchema() throws Exception {
    AbstractCassandraBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try (CqlSession client = testFactory.buildNewClient()) {
      BackendFactory<CassandraBackendConfig> factory =
          PersistLoader.findFactoryByName(CassandraBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(CassandraBackendFactory.class);
      try (Backend backend = factory.buildBackend(buildConfig(client))) {
        soft.assertThat(backend).isNotNull().isInstanceOf(CassandraBackend.class);
        backend.setupSchema();
      }

      client.execute("DROP TABLE IF EXISTS nessie." + TABLE_REFS);
      client.execute("DROP TABLE IF EXISTS nessie." + TABLE_OBJS);

      client.execute("CREATE TABLE nessie." + TABLE_REFS + " (foobarbaz VARCHAR PRIMARY KEY)");

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

      client.execute("DROP TABLE IF EXISTS nessie." + TABLE_REFS);
      client.execute("DROP TABLE IF EXISTS nessie." + TABLE_OBJS);

      client.execute(
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
            .withMessageStartingWith("Expected columns [")
            .withMessageContaining("] do not contain all columns [")
            .withMessageContaining(
                "] for table '"
                    + TABLE_REFS
                    + "'. DDL template:\nCREATE TABLE nessie."
                    + TABLE_REFS);
      }

      client.execute("DROP TABLE IF EXISTS nessie." + TABLE_REFS);
      client.execute("DROP TABLE IF EXISTS nessie." + TABLE_OBJS);

      client.execute("CREATE TABLE nessie." + TABLE_OBJS + " (foobarbaz VARCHAR PRIMARY KEY)");

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
    return CassandraBackendConfig.builder().client(client).replicationFactor(1).build();
  }

  protected abstract AbstractCassandraBackendTestFactory testFactory();
}
