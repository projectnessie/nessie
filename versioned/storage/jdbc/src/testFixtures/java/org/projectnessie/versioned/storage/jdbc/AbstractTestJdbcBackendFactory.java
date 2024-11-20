/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.jdbc;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.TABLE_REFS;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;
import org.projectnessie.versioned.storage.jdbctests.AbstractJdbcBackendTestFactory;
import org.projectnessie.versioned.storage.jdbctests.DataSourceProducer;

@SuppressWarnings("SqlDialectInspection")
@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractTestJdbcBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected abstract AbstractJdbcBackendTestFactory testFactory();

  @Test
  public void productionLike() throws Exception {
    AbstractJdbcBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try {

      DataSource dataSource =
          DataSourceProducer.builder()
              .jdbcUrl(testFactory.jdbcUrl())
              .jdbcUser(testFactory.jdbcUser())
              .jdbcPass(testFactory.jdbcPass())
              .transactionIsolation(testFactory.transactionIsolation())
              .build()
              .createNewDataSource();
      try {
        BackendFactory<JdbcBackendConfig> factory =
            PersistLoader.findFactoryByName(JdbcBackendFactory.NAME);
        soft.assertThat(factory).isNotNull().isInstanceOf(JdbcBackendFactory.class);
        RepositoryDescription repoDesc;

        try (Backend backend =
            factory.buildBackend(JdbcBackendConfig.builder().dataSource(dataSource).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(JdbcBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(JdbcPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(JdbcPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          repoDesc = repositoryLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }

        try (Backend backend =
            factory.buildBackend(JdbcBackendConfig.builder().dataSource(dataSource).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(JdbcBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(JdbcPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(JdbcPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
        }
      } finally {
        ((AutoCloseable) dataSource).close();
      }
    } finally {
      testFactory.stop();
    }
  }

  @Test
  public void backendTestFactory() throws Exception {
    AbstractJdbcBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try {

      // Need to keep one connection alive for H2-in-memory, so H2 does _not_ drop out data during
      // the test execution.
      DataSource dataSource =
          DataSourceProducer.builder()
              .jdbcUrl(testFactory.jdbcUrl())
              .jdbcUser(testFactory.jdbcUser())
              .jdbcPass(testFactory.jdbcPass())
              .transactionIsolation(testFactory.transactionIsolation())
              .build()
              .createNewDataSource();
      try (@SuppressWarnings("unused")
          Connection keepAliveForH2 = dataSource.getConnection()) {
        RepositoryDescription repoDesc;

        try (Backend backend = testFactory.createNewBackend()) {
          soft.assertThat(backend).isNotNull().isInstanceOf(JdbcBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(JdbcPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(JdbcPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          repoDesc = repositoryLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }

        try (Backend backend = testFactory.createNewBackend()) {
          soft.assertThat(backend).isNotNull().isInstanceOf(JdbcBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(JdbcPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(JdbcPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
        }
      } finally {
        ((AutoCloseable) dataSource).close();
      }
    } finally {
      testFactory.stop();
    }
  }

  @Test
  public void incompatibleTableSchema() throws Exception {
    AbstractJdbcBackendTestFactory testFactory = testFactory();
    testFactory.start();
    try {
      DataSource dataSource =
          DataSourceProducer.builder()
              .jdbcUrl(testFactory.jdbcUrl())
              .jdbcUrl(testFactory.jdbcUrl())
              .jdbcUser(testFactory.jdbcUser())
              .jdbcPass(testFactory.jdbcPass())
              .transactionIsolation(testFactory.transactionIsolation())
              .build()
              .createNewDataSource();
      try {
        BackendFactory<JdbcBackendConfig> factory =
            PersistLoader.findFactoryByName(JdbcBackendFactory.NAME);
        soft.assertThat(factory).isNotNull().isInstanceOf(JdbcBackendFactory.class);
        try (Backend backend =
            factory.buildBackend(JdbcBackendConfig.builder().dataSource(dataSource).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(JdbcBackend.class);
          backend.setupSchema();
        }

        try (Connection conn = dataSource.getConnection();
            Statement st = conn.createStatement()) {
          dropTables(conn, st);

          st.executeUpdate("CREATE TABLE " + TABLE_REFS + " (foobarbaz VARCHAR(255) PRIMARY KEY)");
        }

        try (Backend backend =
            factory.buildBackend(JdbcBackendConfig.builder().dataSource(dataSource).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(JdbcBackend.class);
          soft.assertThatIllegalStateException()
              .isThrownBy(backend::setupSchema)
              .withMessageStartingWith(
                  "Expected primary key columns ["
                      + COL_REPO_ID
                      + ", "
                      + COL_REFS_NAME
                      + "] do not match existing primary key columns [foobarbaz] for table '"
                      + TABLE_REFS
                      + "'. DDL template:\nCREATE TABLE "
                      + TABLE_REFS);
        }

        try (Connection conn = dataSource.getConnection();
            Statement st = conn.createStatement()) {
          dropTables(conn, st);

          st.executeUpdate(
              "CREATE TABLE "
                  + TABLE_REFS
                  + " ("
                  + COL_REPO_ID
                  + " VARCHAR(255), "
                  + COL_REFS_NAME
                  + " VARCHAR(255), meep VARCHAR(255), boo VARCHAR(255), PRIMARY KEY ("
                  + COL_REPO_ID
                  + ", "
                  + COL_REFS_NAME
                  + "))");
        }

        try (Backend backend =
            factory.buildBackend(JdbcBackendConfig.builder().dataSource(dataSource).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(JdbcBackend.class);
          soft.assertThatIllegalStateException()
              .isThrownBy(backend::setupSchema)
              .withMessageStartingWith(
                  "The database table "
                      + TABLE_REFS
                      + " is missing mandatory columns created_at,deleted,ext_info,pointer,prev_ptr.\n"
                      + "Found columns : boo,meep,ref_name,repo\n"
                      + "Expected columns : ")
              .withMessageContaining("DDL template:\nCREATE TABLE " + TABLE_REFS);
        }
      } finally {
        ((AutoCloseable) dataSource).close();
      }
    } finally {
      testFactory.stop();
    }
  }

  private static void dropTables(Connection conn, Statement st) throws SQLException {
    try {
      st.executeUpdate("DROP TABLE " + TABLE_REFS);
      conn.commit();
    } catch (SQLException ignore) {
      conn.rollback();
    }
    try {
      st.executeUpdate("DROP TABLE " + TABLE_OBJS);
      conn.commit();
    } catch (SQLException ignore) {
      conn.rollback();
    }
  }
}
