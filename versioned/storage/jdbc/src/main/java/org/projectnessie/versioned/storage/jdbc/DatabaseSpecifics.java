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
package org.projectnessie.versioned.storage.jdbc;

import jakarta.annotation.Nonnull;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;

public final class DatabaseSpecifics {
  private DatabaseSpecifics() {}

  // Use 'ucs_basic' collation for PostgreSQL, otherwise multiple spaces would be collapses and
  // result in wrong reference listings. Assume the following reference names
  // 'ref-    1'
  // 'ref-    2'
  // 'ref-    3'
  // 'ref-    8'
  // 'ref-    9'
  // 'ref-   10'
  // 'ref-   11'
  // 'ref-   19'
  // 'ref-   20'
  // 'ref-   21'
  // With ucs_basic, the above (expected) order is maintained, but the default behavior could
  // choose a collation in which 'ref-    2' is sorted _after_ 'ref-   19', which is unexpected
  // and wrong for Nessie.
  public static final DatabaseSpecific POSTGRESQL_DATABASE_SPECIFIC =
      new BasePostgresDatabaseSpecific("VARCHAR COLLATE ucs_basic");

  public static final DatabaseSpecific COCKROACH_DATABASE_SPECIFIC =
      new BasePostgresDatabaseSpecific("VARCHAR");

  public static final DatabaseSpecific H2_DATABASE_SPECIFIC =
      new BasePostgresDatabaseSpecific("VARCHAR");

  public static final DatabaseSpecific MARIADB_DATABASE_SPECIFIC = new MariaDBDatabaseSpecific();

  public static DatabaseSpecific detect(DataSource dataSource) {
    try (Connection conn = dataSource.getConnection()) {
      return detect(conn);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private static DatabaseSpecific detect(Connection conn) {
    try {
      String productName = conn.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
      switch (productName) {
        case "h2":
          return H2_DATABASE_SPECIFIC;
        case "postgresql":
          try (ResultSet rs = conn.getMetaData().getSchemas(conn.getCatalog(), "crdb_internal")) {
            if (rs.next()) {
              return COCKROACH_DATABASE_SPECIFIC;
            } else {
              return POSTGRESQL_DATABASE_SPECIFIC;
            }
          }
        case "mysql":
        case "mariadb":
          return MARIADB_DATABASE_SPECIFIC;
        default:
          throw new IllegalStateException(
              "Could not select specifics to use for database product '" + productName + "'");
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  static class BasePostgresDatabaseSpecific implements DatabaseSpecific {

    /** Integrity constraint violation error code, as returned by H2, Postgres &amp; Cockroach. */
    private static final String CONSTRAINT_VIOLATION_SQL_CODE = "23505";

    /** Deadlock error, returned by Postgres. */
    private static final String DEADLOCK_SQL_STATE_POSTGRES = "40P01";

    /** Already exists error, returned by Postgres, H2 and Cockroach. */
    private static final String ALREADY_EXISTS_STATE_POSTGRES = "42P07";

    /**
     * Cockroach "retry, write too old" error, see <a
     * href="https://www.cockroachlabs.com/docs/v21.1/transaction-retry-error-reference.html#retry_write_too_old">Cockroach's
     * Transaction Retry Error Reference</a>, and Postgres may return a "deadlock" error.
     */
    private static final String RETRY_SQL_STATE_COCKROACH = "40001";

    private final Map<JdbcColumnType, String> typeMap;
    private final Map<JdbcColumnType, Integer> typeIdMap;

    BasePostgresDatabaseSpecific(String varcharType) {
      typeMap = new EnumMap<>(JdbcColumnType.class);
      typeIdMap = new EnumMap<>(JdbcColumnType.class);
      typeMap.put(JdbcColumnType.NAME, varcharType);
      typeIdMap.put(JdbcColumnType.NAME, Types.VARCHAR);
      typeMap.put(JdbcColumnType.OBJ_ID, varcharType);
      typeIdMap.put(JdbcColumnType.OBJ_ID, Types.VARCHAR);
      typeMap.put(JdbcColumnType.OBJ_ID_LIST, varcharType);
      typeIdMap.put(JdbcColumnType.OBJ_ID_LIST, Types.VARCHAR);
      typeMap.put(JdbcColumnType.BOOL, "BOOLEAN");
      typeIdMap.put(JdbcColumnType.BOOL, Types.BOOLEAN);
      typeMap.put(JdbcColumnType.VARBINARY, "BYTEA");
      typeIdMap.put(JdbcColumnType.VARBINARY, Types.VARBINARY);
      typeMap.put(JdbcColumnType.BIGINT, "BIGINT");
      typeIdMap.put(JdbcColumnType.BIGINT, Types.BIGINT);
      typeMap.put(JdbcColumnType.VARCHAR, varcharType);
      typeIdMap.put(JdbcColumnType.VARCHAR, Types.VARCHAR);
    }

    @Override
    public Map<JdbcColumnType, String> columnTypes() {
      return typeMap;
    }

    @Override
    public Map<JdbcColumnType, Integer> columnTypeIds() {
      return typeIdMap;
    }

    @Override
    public boolean isConstraintViolation(SQLException e) {
      return CONSTRAINT_VIOLATION_SQL_CODE.equals(e.getSQLState());
    }

    @Override
    public boolean isRetryTransaction(SQLException e) {
      if (e.getSQLState() == null) {
        return false;
      }
      switch (e.getSQLState()) {
        case DEADLOCK_SQL_STATE_POSTGRES:
        case RETRY_SQL_STATE_COCKROACH:
          return true;
        default:
          return false;
      }
    }

    @Override
    public boolean isAlreadyExists(SQLException e) {
      return ALREADY_EXISTS_STATE_POSTGRES.equals(e.getSQLState());
    }

    @Override
    public String wrapInsert(String sql) {
      return sql + " ON CONFLICT DO NOTHING";
    }
  }

  static class MariaDBDatabaseSpecific implements DatabaseSpecific {

    private static final String VARCHAR = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
    private static final String TEXT = "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";

    private static final String MYSQL_CONSTRAINT_VIOLATION_SQL_STATE = "23000";
    private static final String MYSQL_LOCK_DEADLOCK_SQL_STATE = "40001";
    private static final String MYSQL_ALREADY_EXISTS_SQL_STATE = "42S01";

    private final Map<JdbcColumnType, String> typeMap;
    private final Map<JdbcColumnType, Integer> typeIdMap;

    MariaDBDatabaseSpecific() {
      typeMap = new EnumMap<>(JdbcColumnType.class);
      typeIdMap = new EnumMap<>(JdbcColumnType.class);
      typeMap.put(JdbcColumnType.NAME, VARCHAR);
      typeIdMap.put(JdbcColumnType.NAME, Types.VARCHAR);
      typeMap.put(JdbcColumnType.OBJ_ID, VARCHAR);
      typeIdMap.put(JdbcColumnType.OBJ_ID, Types.VARCHAR);
      typeMap.put(JdbcColumnType.OBJ_ID_LIST, TEXT);
      typeIdMap.put(JdbcColumnType.OBJ_ID_LIST, Types.VARCHAR);
      typeMap.put(JdbcColumnType.BOOL, "BIT(1)");
      typeIdMap.put(JdbcColumnType.BOOL, Types.BIT);
      typeMap.put(JdbcColumnType.VARBINARY, "LONGBLOB");
      typeIdMap.put(JdbcColumnType.VARBINARY, Types.BLOB);
      typeMap.put(JdbcColumnType.BIGINT, "BIGINT");
      typeIdMap.put(JdbcColumnType.BIGINT, Types.BIGINT);
      typeMap.put(JdbcColumnType.VARCHAR, VARCHAR);
      typeIdMap.put(JdbcColumnType.VARCHAR, Types.VARCHAR);
    }

    @Override
    public Map<JdbcColumnType, String> columnTypes() {
      return typeMap;
    }

    @Override
    public Map<JdbcColumnType, Integer> columnTypeIds() {
      return typeIdMap;
    }

    @Override
    public boolean isConstraintViolation(SQLException e) {
      return MYSQL_CONSTRAINT_VIOLATION_SQL_STATE.equals(e.getSQLState());
    }

    @Override
    public boolean isRetryTransaction(SQLException e) {
      return MYSQL_LOCK_DEADLOCK_SQL_STATE.equals(e.getSQLState());
    }

    @Override
    public boolean isAlreadyExists(SQLException e) {
      return MYSQL_ALREADY_EXISTS_SQL_STATE.equals(e.getSQLState());
    }

    @Override
    public String wrapInsert(String sql) {
      return sql.replace("INSERT INTO", "INSERT IGNORE INTO");
    }
  }
}
