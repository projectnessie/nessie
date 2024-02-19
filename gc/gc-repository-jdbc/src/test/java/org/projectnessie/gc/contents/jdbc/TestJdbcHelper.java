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
package org.projectnessie.gc.contents.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.sql.Connection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

public class TestJdbcHelper extends AbstractJdbcHelper {

  @BeforeAll
  public static void createDataSource() throws Exception {
    initDataSource("jdbc:h2:mem:nessie;MODE=PostgreSQL");
  }

  @Test
  @Order(0)
  void createTables() {
    assertThatCode(
            () -> {
              try (Connection conn = dataSource.getConnection()) {
                JdbcHelper.createTables(conn, false);
              }
            })
        .doesNotThrowAnyException();
    assertThatCode(
            () -> {
              try (Connection conn = dataSource.getConnection()) {
                JdbcHelper.createTables(conn, true);
              }
            })
        .as("creating tables again should not throw when ifNotExists is true")
        .doesNotThrowAnyException();
  }

  @Test
  @Order(1)
  void dropTables() {
    assertThatCode(
            () -> {
              try (Connection conn = dataSource.getConnection()) {
                JdbcHelper.dropTables(conn);
              }
            })
        .doesNotThrowAnyException();
    assertThatCode(
            () -> {
              try (Connection conn = dataSource.getConnection()) {
                JdbcHelper.dropTables(conn);
              }
            })
        .as("dropping tables again should not throw")
        .doesNotThrowAnyException();
  }
}
