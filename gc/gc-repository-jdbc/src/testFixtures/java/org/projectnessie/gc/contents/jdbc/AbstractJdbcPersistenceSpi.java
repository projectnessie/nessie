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
package org.projectnessie.gc.contents.jdbc;

import static java.lang.String.format;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.contents.tests.AbstractPersistenceSpi;

public abstract class AbstractJdbcPersistenceSpi extends AbstractPersistenceSpi {

  static DataSource dataSource;

  static void initDataSource(String jdbcUrl) throws Exception {
    AgroalJdbcDataSourceProvider dsProvider =
        AgroalJdbcDataSourceProvider.builder()
            .jdbcUrl(jdbcUrl)
            .usernamePasswordCredentials("test", "test")
            .poolMinSize(1)
            .poolMaxSize(1)
            .poolInitialSize(1)
            .build();
    dataSource = dsProvider.dataSource();
  }

  @AfterAll
  static void closeDataSource() throws Exception {
    if (dataSource instanceof AutoCloseable) {
      ((AutoCloseable) dataSource).close();
    }
  }

  @Override
  protected PersistenceSpi createPersistenceSpi() {
    return JdbcPersistenceSpi.builder().dataSource(dataSource).fetchSize(10).build();
  }

  @BeforeEach
  void setup() throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      JdbcHelper.createTables(conn, false);
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      JdbcHelper.dropTables(conn);
    }
  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  @Override
  protected void assertDeleted(UUID id) throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      for (String tableName : SqlDmlDdl.ALL_CREATES.keySet()) {
        try (PreparedStatement st =
            conn.prepareStatement(format("SELECT * FROM %s WHERE live_set_id = ?", tableName))) {
          st.setString(1, id.toString());
          try (ResultSet rs = st.executeQuery()) {
            soft.assertThat(rs.next()).as(tableName).isFalse();
          }
        }
      }
    }
  }
}
