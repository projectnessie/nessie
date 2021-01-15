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
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref.UnsavedCommitDelta;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.ImmutableKey.Builder;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.Key.Mutation;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.KeyDelta;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;
import com.google.protobuf.ByteString;

import io.agroal.pool.wrapper.ConnectionWrapper;
import oracle.jdbc.OracleConnection;

/**
 * Abstraction of database products like H2, CockroachDB and PostgresQL.
 * <p>Provides the actual {@link ColumnType} to database specific data type mapping and
 * handling of getting/setting the different data types plus a view optimizations like
 * optional batch-DDL operations or upper/lower-case database object checks.</p>
 */
@SuppressWarnings("SameParameterValue")
public enum Dialect {
  HSQL() {
    @Override
    String columnType(ColumnType type) {
      switch (type) {
        case ID:
          return "VARCHAR(80)";
        case DT:
        case SEQ:
        case LONG:
          return "BIGINT";
        case BINARY:
          return "VARBINARY(250000)";
        case BOOL:
          return "BOOLEAN";
        case REF_TYPE:
          return "VARCHAR(1)";
        case REF_NAME:
          return "VARCHAR(4100)";
        case KEY_MUTATION_LIST:
          return "VARCHAR(4102) ARRAY";
        case KEY_DELTA_LIST:
          return "VARCHAR(8300) ARRAY";
        case KEY_LIST:
          return "VARCHAR(4100) ARRAY";
        case ID_LIST:
          return "VARCHAR(80) ARRAY";
        default:
          throw new IllegalArgumentException("Unknown column-type " + type);
      }
    }

    @Override
    boolean metadataUpperCase() {
      return true;
    }
  },

  H2() {
    @Override
    String columnType(ColumnType type) {
      switch (type) {
        case ID:
          return "VARCHAR(80)";
        case DT:
        case SEQ:
        case LONG:
          return "BIGINT";
        case BINARY:
          return "VARBINARY(250000)";
        case BOOL:
          return "BOOLEAN";
        case REF_TYPE:
          return "VARCHAR(1)";
        case REF_NAME:
          return "VARCHAR(4100)";
        case KEY_MUTATION_LIST:
        case KEY_DELTA_LIST:
        case KEY_LIST:
        case ID_LIST:
          return "ARRAY";
        default:
          throw new IllegalArgumentException("Unknown column-type " + type);
      }
    }

    @Override
    boolean metadataUpperCase() {
      return true;
    }

    @Override
    boolean isIntegrityConstraintViolation(Exception e) {
      if (e instanceof SQLException) {
        SQLException sqlException = (SQLException) e;
        return sqlException.getErrorCode() == 23505;
      }
      return super.isIntegrityConstraintViolation(e);
    }
  },

  COCKROACH() {
    @Override
    String columnType(ColumnType type) {
      switch (type) {
        case ID:
          return "VARCHAR(80)";
        case DT:
        case SEQ:
        case LONG:
          return "BIGINT";
        case BINARY:
          return "BYTES";
        case BOOL:
          return "BOOL";
        case REF_TYPE:
          return "VARCHAR(1)";
        case REF_NAME:
          return "TEXT";
        case KEY_MUTATION_LIST:
        case KEY_DELTA_LIST:
        case KEY_LIST:
        case ID_LIST:
          return "TEXT ARRAY";
        default:
          throw new IllegalArgumentException("Unknown column-type " + type);
      }
    }

    @Override
    boolean isIntegrityConstraintViolation(Exception e) {
      if (super.isIntegrityConstraintViolation(e)) {
        return true;
      }
      if (e instanceof SQLException) {
        SQLException sql = (SQLException) e;
        return "23505".equals(sql.getSQLState());
      }
      return false;
    }

    @Override
    boolean batchDDL() {
      return true;
    }
  },

  POSTGRESQL() {
    @Override
    String columnType(ColumnType type) {
      switch (type) {
        case ID:
          return "VARCHAR(80)";
        case DT:
        case SEQ:
        case LONG:
          return "BIGINT";
        case BINARY:
          return "BYTEA";
        case BOOL:
          return "BOOLEAN";
        case REF_TYPE:
          return "VARCHAR(1)";
        case REF_NAME:
          return "TEXT";
        case KEY_MUTATION_LIST:
        case KEY_DELTA_LIST:
        case KEY_LIST:
        case ID_LIST:
          return "TEXT ARRAY";
        default:
          throw new IllegalArgumentException("Unknown column-type " + type);
      }
    }

    @Override
    boolean isIntegrityConstraintViolation(Exception e) {
      if (super.isIntegrityConstraintViolation(e)) {
        return true;
      }
      if (e instanceof SQLException) {
        SQLException sql = (SQLException) e;
        return "23505".equals(sql.getSQLState());
      }
      return false;
    }

    @Override
    boolean batchDDL() {
      return true;
    }
  },

  ORACLE {
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
    boolean getBool(ResultSet resultSet, String column) throws SQLException {
      return "T".equals(getString(resultSet, column));
    }

    @Override
    void setBool(PreparedStatement pstmt, int i, boolean bool) {
      setString(pstmt, i, bool ? "T" : "F");
    }

    <T> Stream<T> getArray(ResultSet resultSet, String column,
        Function<String, T> elementConverter) throws SQLException {
      Array array = resultSet.getArray(column);
      if (array != null) {
        Object[] arr = (Object[]) array.getArray();
        return Arrays.stream(arr).map(String.class::cast).map(elementConverter);
      }
      return Stream.empty();
    }

    <E, S> void setArray(SQLChange change, String column, Stream<E> contents, Function<E, S> converter, ColumnType columnType) {
      if (contents == null) {
        change.setColumn(column, (stmt, index) -> {
          stmt.setNull(index, Types.ARRAY, typeForColumnType(columnType));
          return 1;
        });
      } else {
        String[] arr = contents.map(converter).map(x -> x != null ? x.toString() : null).toArray(String[]::new);
        change.setColumn(column, (stmt, index) -> {
          Connection conn = stmt.getConnection();

          // This is just for Oracle...
          // We need the wrapped connection to cast it to an OracleConnection to set an array. WTF!
          // So we use the Agroal connection-pool here, because that's the one used by Quarkus,
          // and it's therefore used in the JdbcFixture.
          if (conn instanceof ConnectionWrapper) {
            ConnectionWrapper cw = (ConnectionWrapper) conn;
            conn = cw.getHandler().getConnection();
          }

          // Using ARRAYs in Oracle suc^Wis kinda interesting
          OracleConnection oracleConnection = (OracleConnection) conn;
          Array array = oracleConnection.createOracleArray(typeForColumnType(columnType), arr);
          stmt.setArray(index, array);
          return 1;
        });
      }
    }
  };

  abstract String columnType(ColumnType value);

  /**
   * Whether the database stores schema/catalog/table names in upper case.
   * @return {@code true} to use upper-case when inspecting the database
   */
  boolean metadataUpperCase() {
    return false;
  }

  /**
   * Whether the database supports DDL-statement batches.
   * @return {@code true}, if DDL-statement batches should be used
   */
  boolean batchDDL() {
    return false;
  }

  /**
   * Additional DDL statements that need to be executed before a table is created.
   * @return list of DDL statements
   */
  List<String> additionalDDL() {
    return Collections.emptyList();
  }

  String primaryKey(String rawTableName, String... primaryKeyColumns) {
    return "PRIMARY KEY (" + String.join(", ", primaryKeyColumns) + ")";
  }

  boolean isIntegrityConstraintViolation(Exception e) {
    return e instanceof SQLIntegrityConstraintViolationException;
  }

  long getDt(ResultSet resultSet, String column) throws SQLException {
    return getLong(resultSet, column);
  }

  void setDt(SQLChange change, String column, long dt) {
    setLong(change, column, dt);
  }

  ValueApplicator idApplicator(Id id) {
    return (stmt, index) -> {
      setId(stmt, index, id);
      return 1;
    };
  }

  Id getId(ResultSet resultSet, String column) throws SQLException {
    return stringToId(getString(resultSet, column));
  }

  void setId(PreparedStatement pstmt, int index, Id id) {
    try {
      pstmt.setString(index, id != null ? id.toString() : null);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  void setId(SQLChange change, String column, Id id) {
    change.setColumn(column, idApplicator(id));
  }

  ByteString getBinary(ResultSet resultSet, String column) throws SQLException {
    return ByteString.copyFrom(resultSet.getBytes(column));
  }

  void setBinary(PreparedStatement pstmt, int i, ByteString binary) {
    try {
      pstmt.setBytes(i, binary != null ? binary.toByteArray() : null);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  void setBinary(SQLChange change, String column, ByteString binary) {
    change.setColumn(column, (stmt, index) -> {
      setBinary(stmt, index, binary);
      return 1;
    });
  }

  String getString(ResultSet resultSet, String column) throws SQLException {
    return resultSet.getString(column);
  }

  void setString(PreparedStatement pstmt, int index, String string) {
    try {
      pstmt.setString(index, string);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  void setString(SQLChange change, String column, String string) {
    change.setColumn(column, (stmt, index) -> {
      setString(stmt, index, string);
      return 1;
    });
  }

  long getLong(ResultSet resultSet, String column) throws SQLException {
    return resultSet.getLong(column);
  }

  void setLong(PreparedStatement pstmt, int i, Long value) {
    try {
      if (value == null) {
        pstmt.setNull(i, Types.BIGINT);
      } else {
        pstmt.setLong(i, value);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  void setLong(SQLChange change, String column, Long value) {
    change.setColumn(column, (stmt, index) -> {
      setLong(stmt, index, value);
      return 1;
    });
  }

  boolean getBool(ResultSet resultSet, String column) throws SQLException {
    return resultSet.getBoolean(column);
  }

  void setBool(PreparedStatement pstmt, int i, boolean bool) {
    try {
      pstmt.setBoolean(i, bool);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  void setBool(SQLChange change, String column, boolean bool) {
    change.setColumn(column, (stmt, index) -> {
      setBool(stmt, index, bool);
      return 1;
    });
  }

  Stream<Key> getKeys(ResultSet resultSet, String column) throws SQLException {
    return getArray(resultSet, column, Dialect::fromFlattened);
  }

  void setKeys(SQLChange change, String column, Stream<Key> keys) {
    setArray(change, column, keys, Dialect::flattenKey, ColumnType.KEY_LIST);
  }

  Stream<Id> getIds(ResultSet resultSet, String column) throws SQLException {
    return getArray(resultSet, column, Dialect::stringToId);
  }

  void setIds(SQLChange change, String column, Stream<Id> ids) {
    setArray(change, column, ids, id -> id != null ? id.toString() : "", ColumnType.ID_LIST);
  }

  void setStrings(SQLChange change, String column, List<String> array, ColumnType columnType) {
    setArray(change, column, array != null ? array.stream() : null, Function.identity(), columnType);
  }

  Stream<KeyDelta> getKeyDeltas(ResultSet resultSet, String column) throws SQLException {
    return getArray(resultSet, column, Dialect::deserializeKeyDelta);
  }

  void setKeyDeltas(SQLChange change, String column, Stream<KeyDelta> keyDeltas) {
    setArray(change, column, keyDeltas, Dialect::serializeKeyDelta, ColumnType.KEY_DELTA_LIST);
  }

  void getUnsavedDeltas(ResultSet resultSet, String column, UnsavedCommitDelta delta)
      throws SQLException {
    getArray(resultSet, column, Function.identity()).forEach(s -> unsavedDelta(s, delta));
  }

  void getMutations(ResultSet resultSet, String column, Consumer<Mutation> keyMutation)
      throws SQLException {
    getArray(resultSet, column, Dialect::mutationFromString).forEach(keyMutation);
  }

  void setMutations(SQLChange change, String column, Stream<Mutation> mutations) {
    setArray(change, column, mutations, Dialect::mutationAsString, ColumnType.KEY_MUTATION_LIST);
  }

  /**
   * Get a string-ified version of this key-mutation in the form
   * {@code type-short-name ":" flattened-key}, which can be parsed with
   * {@link #mutationFromString(String)}.
   *
   * @return string-ified version
   * @see #flattenKey(Key)
   */
  static String mutationAsString(Key.Mutation mutation) {
    switch (mutation.getType()) {
      case ADDITION:
        return "a:" + flattenKey(mutation.getKey());
      case REMOVAL:
        return "d:" + flattenKey(mutation.getKey());
      default:
        throw new IllegalArgumentException("Invalid mutation-type " + mutation.getType());
    }
  }

  /**
   * Parsed a string-ified version of a key-mutation in the form
   * {@code type-short-name ":" flattened-key}, which can be generated with
   * {@link #mutationAsString(Key.Mutation)}.
   *
   * @param s string-ified version
   * @return parsed {@link Key.Mutation}
   * @see #fromFlattened(String)
   */
  static Key.Mutation mutationFromString(String s) {
    int i = s.indexOf(':');
    String addRemove = s.substring(0, i);
    Key key = fromFlattened(s.substring(i + 1));
    switch (addRemove) {
      case "a":
        return key.asAddition();
      case "d":
        return key.asRemoval();
      default:
        throw new IllegalArgumentException("Illegal mutation type " + addRemove);
    }
  }

  /**
   * Convert a {@link #flattenKey(Key)} representation back into a {@link Key} object.
   *
   * @param flattened the {@link #flattenKey(Key)} representation
   * @return parsed {@link Key}
   */
  static Key fromFlattened(String flattened) {
    Builder b = ImmutableKey.builder();
    StringBuilder sb = new StringBuilder();
    int l = flattened.length();
    for (int p = 0; p < l; p++) {
      char c = flattened.charAt(p);
      if (c == '\\') {
        c = flattened.charAt(++p);
        sb.append(c);
      } else if (c == '.') {
        if (sb.length() > 0) {
          b.addElements(sb.toString());
          sb.setLength(0);
        }
      } else {
        sb.append(c);
      }
    }
    if (sb.length() > 0) {
      b.addElements(sb.toString());
      sb.setLength(0);
    }
    return b.build();
  }

  /**
   * Flatten this {@link Key} into a single string.
   * <p>
   * Key elements are separated with a dot ({@code .}).
   * Dots are escaped using a leading backslash.
   * Backslashes are escaped using two backslashes.
   * </p>
   *
   * @return the flattened representation
   * @see #fromFlattened(String)
   */
  static String flattenKey(Key key) {
    StringBuilder sb = new StringBuilder();
    for (String element : key.getElements()) {
      if (sb.length() > 0) {
        sb.append('.');
      }

      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        switch (c) {
          case '.':
            sb.append("\\.");
            break;
          case '\\':
            sb.append("\\\\");
            break;
          default:
            sb.append(c);
            break;
        }
      }
    }
    return sb.toString();
  }

  static String serializeKeyDelta(KeyDelta keyDelta) {
    return keyDelta.getId().toString() + ',' + flattenKey(keyDelta.getKey());
  }

  static KeyDelta deserializeKeyDelta(String str) {
    int i = str.indexOf(',');
    Id id = stringToId(str.substring(0, i));
    Key key = fromFlattened(str.substring(i + 1));
    return KeyDelta.of(key, id);
  }

  static Id stringToId(String str) {
    return str != null && !str.isEmpty() ? Id.of(Hash.of(str).asBytes()) : null;
  }

  static void unsavedDelta(String str, UnsavedCommitDelta deltas) {
    int i1 = str.indexOf(',');
    int i2 = str.indexOf(',', i1 + 1);
    int position = Integer.parseInt(str.substring(0, i1));
    Id oldId = stringToId(str.substring(i1 + 1, i2));
    Id newId = stringToId(str.substring(i2 + 1));
    deltas.delta(position, oldId, newId);
  }

  int setNull(PreparedStatement stmt, int index, ColumnType type) {
    switch (type) {
      case ID:
        setId(stmt, index, null);
        return 1;
      case BINARY:
        setBinary(stmt, index, null);
        return 1;
      case LONG:
        setLong(stmt, index, null);
        return 1;
      case BOOL:
        throw new UnsupportedOperationException();
      case REF_TYPE:
        setString(stmt, index, null);
        return 1;
      case ID_LIST:
      case KEY_LIST:
      case KEY_DELTA_LIST:
      case KEY_MUTATION_LIST:
      default:
        throw new IllegalArgumentException("Cannot UPDATE column-type " + type);
    }
  }

  int setEntity(PreparedStatement stmt, int index, ColumnType type, Entity value) {
    switch (type) {
      case ID:
        setId(stmt, index, Id.of(value.getBinary()));
        return 1;
      case BINARY:
        setBinary(stmt, index, value.getBinary());
        return 1;
      case LONG:
        setLong(stmt, index, value.getNumber());
        return 1;
      case BOOL:
        setBool(stmt, index, value.getBoolean());
        return 1;
      case REF_TYPE:
        setString(stmt, index, value.getString());
        return 1;
      case ID_LIST:
      case KEY_LIST:
      case KEY_DELTA_LIST:
      case KEY_MUTATION_LIST:
      default:
        throw new IllegalArgumentException("Cannot UPDATE column-type " + type);
    }
  }

  <T> Stream<T> getArray(ResultSet resultSet, String column,
      Function<String, T> elementConverter) throws SQLException {
    Array array = resultSet.getArray(column);
    if (array != null) {
      Object[] arr = (Object[]) array.getArray();
      return Arrays.stream(arr).map(String.class::cast).map(elementConverter);
    }
    return Stream.empty();
  }

  <E, S> void setArray(SQLChange change, String column, Stream<E> contents, Function<E, S> converter, ColumnType columnType) {
    if (contents == null) {
      change.setColumn(column, (stmt, index) -> {
        stmt.setObject(index, null);
        return 1;
      });
    } else {
      @SuppressWarnings("unchecked") S[] arr = contents.map(converter).toArray(l -> (S[]) new String[l]);
      change.setColumn(column, (stmt, index) -> {
        stmt.setObject(index, arr);
        return 1;
      });
    }
  }
}
