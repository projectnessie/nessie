/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.versioned.store.jdbc;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.enterprise.inject.Alternative;

import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;

import io.agroal.pool.wrapper.ConnectionWrapper;
import oracle.jdbc.OracleConnection;

@Alternative
public class OracleDatabaseAdapter extends DatabaseAdapter {

  @Override
  String name() {
    return "Oracle";
  }

  @Override
  String columnType(ColumnType type) {
    switch (type) {
      case ID:
        return "VARCHAR2(80)";
      case DT:
      case SEQ:
      case LONG:
        return "NUMBER(20,0)";
      case BINARY:
        return "BLOB";
      case BOOL:
      case REF_TYPE:
        return "VARCHAR2(1)";
      case REF_NAME:
        return "VARCHAR2(4000)";
      case KEY_MUTATION_LIST:
        return "key_mutation_list_type";
      case KEY_DELTA_LIST:
        return "key_delta_list_type";
      case KEY_LIST:
        return "key_list_type";
      case ID_LIST:
        return "id_list_type";
      default:
        throw new IllegalArgumentException("Unknown column-type " + type);
    }
  }

  @Override
  boolean metadataUpperCase() {
    return true;
  }

  @Override
  String primaryKey(String rawTableName, String... primaryKeyColumns) {
    return super.primaryKey(rawTableName, primaryKeyColumns) + " USING INDEX";
  }

  @Override
  List<String> additionalDDL() {
    return Arrays.asList(
        "CREATE OR REPLACE TYPE key_mutation_list_type IS VARRAY(1000) of VARCHAR2(4102)",
        "CREATE OR REPLACE TYPE key_delta_list_type IS VARRAY(1000) of VARCHAR2(8300)",
        "CREATE OR REPLACE TYPE key_list_type IS VARRAY(1000) of VARCHAR2(4100)",
        "CREATE OR REPLACE TYPE id_list_type IS VARRAY(1000) of VARCHAR2(80)"
    );
  }

  private String typeForColumnType(ColumnType columnType) {
    switch (columnType) {
      case KEY_MUTATION_LIST:
        return "KEY_MUTATION_LIST_TYPE";
      case KEY_DELTA_LIST:
        return "KEY_DELTA_LIST_TYPE";
      case KEY_LIST:
        return "KEY_LIST_TYPE";
      case ID_LIST:
        return "ID_LIST_TYPE";
      default:
        throw new IllegalArgumentException("ColumnType " + columnType + " is not an array type");
    }
  }

  @Override
  boolean getBoolean(ResultSet resultSet, String column) {
    return "T".equals(getString(resultSet, column));
  }

  @Override
  void setBoolean(PreparedStatement pstmt, int i, boolean bool) {
    setString(pstmt, i, bool ? "T" : "F");
  }

  <E, S> void setArray(SQLChange change, String column, Stream<E> contents, Function<E, S> converter, ColumnType columnType) {
    if (contents == null) {
      change.setColumn(column, (stmt, index) -> {
        SQLError.run(() -> stmt.setNull(index, Types.ARRAY, typeForColumnType(columnType)));
        return 1;
      });
    } else {
      String[] arr = contents.map(converter).map(x -> x != null ? x.toString() : null).toArray(String[]::new);
      change.setColumn(column, (stmt, index) -> {
        SQLError.run(() -> {
          Connection conn = stmt.getConnection();

          // This is just for Oracle...
          // We need the wrapped connection to cast it to an OracleConnection to set an array.
          // So we use the Agroal connection-pool here, because that's the one used by Quarkus,
          // and it's therefore used in the JdbcFixture.
          if (conn instanceof ConnectionWrapper) {
            ConnectionWrapper cw = (ConnectionWrapper) conn;
            conn = cw.getHandler().getConnection();
          }

          // Using ARRAYs in Oracle is kinda "interesting"
          OracleConnection oracleConnection = (OracleConnection) conn;
          Array array = oracleConnection.createOracleArray(typeForColumnType(columnType), arr);
          stmt.setArray(index, array);
        });
        return 1;
      });
    }
  }
}
