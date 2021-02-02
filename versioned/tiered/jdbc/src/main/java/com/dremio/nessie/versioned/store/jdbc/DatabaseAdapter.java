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

/**
 * Abstraction of database products like H2, CockroachDB and PostgresQL.
 * <p>Provides the actual {@link ColumnType} to database specific data type mapping and
 * handling of getting/setting the different data types plus a view optimizations like
 * optional batch-DDL operations or upper/lower-case database object checks.</p>
 */
public abstract class DatabaseAdapter {
  public static class HSQL extends DatabaseAdapter {

    @Override
    String name() {
      return "HSQL";
    }

    @Override
    String columnType(ColumnType type) {
      switch (type) {
        case ID:
          // The commit-hashes can be up to 40 bytes -> 80 hex chars
          return "VARCHAR(80)";
        case DT:
        case SEQ:
        case LONG:
          return "BIGINT";
        case BINARY:
          // 250000 is actually an arbitrarily chosen number - like: "this is hopefully enough"
          return "VARBINARY(250000)";
        case BOOL:
          return "BOOLEAN";
        case REF_TYPE:
          return "VARCHAR(1)";
        case REF_NAME:
          // We use reference names up to this size in our tests
          return "VARCHAR(4100)";
        case KEY_MUTATION_LIST:
          // We use reference names up to this size in our tests (+ 2 chars for 'a:' or 'd:')
          return "VARCHAR(4102) ARRAY";
        case KEY_DELTA_LIST:
          // We require this size for our tests
          return "VARCHAR(8300) ARRAY";
        case KEY_LIST:
          // We use reference names up to this size in our tests
          return "VARCHAR(4100) ARRAY";
        case ID_LIST:
          // The commit-hashes can be up to 40 bytes -> 80 hex chars
          return "VARCHAR(80) ARRAY";
        default:
          throw new IllegalArgumentException("Unknown column-type " + type);
      }
    }

    @Override
    boolean metadataUpperCase() {
      return true;
    }
  }

  public static class H2 extends DatabaseAdapter {

    @Override
    String name() {
      return "H2";
    }

    @Override
    String columnType(ColumnType type) {
      switch (type) {
        case ID:
          // The commit-hashes can be up to 40 bytes -> 80 hex chars
          return "VARCHAR(80)";
        case DT:
        case SEQ:
        case LONG:
          return "BIGINT";
        case BINARY:
          // 250000 is actually an arbitrarily chosen number - like: "this is hopefully enough"
          return "VARBINARY(250000)";
        case BOOL:
          return "BOOLEAN";
        case REF_TYPE:
          return "VARCHAR(1)";
        case REF_NAME:
          // We use reference names up to this size in our tests
          return "VARCHAR(4100)";
        case KEY_MUTATION_LIST:
        case KEY_DELTA_LIST:
        case KEY_LIST:
        case ID_LIST:
          // H2 doesn't have typed arrays
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
    boolean isIntegrityConstraintViolation(Throwable e) {
      if (e instanceof SQLError) {
        e = e.getCause();
      }
      if (e instanceof SQLException) {
        SQLException sqlException = (SQLException) e;
        return sqlException.getErrorCode() == 23505;
      }
      return super.isIntegrityConstraintViolation(e);
    }
  }

  public static class Cockroach extends PostgresQL {

    @Override
    String name() {
      return "Cockroach";
    }

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
  }

  public static class PostgresQL extends DatabaseAdapter {

    @Override
    String name() {
      return "PostgresQL";
    }

    @Override
    String columnType(ColumnType type) {
      switch (type) {
        case ID:
          // The commit-hashes can be up to 40 bytes -> 80 hex chars
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
    boolean isIntegrityConstraintViolation(Throwable e) {
      if (e instanceof SQLError) {
        e = e.getCause();
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
  }

  abstract String name();

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

  boolean isIntegrityConstraintViolation(Throwable e) {
    if (e instanceof SQLError) {
      e = e.getCause();
    }
    return e instanceof SQLIntegrityConstraintViolationException;
  }

  @SuppressWarnings("SameParameterValue")
  long getDt(ResultSet resultSet, String column) {
    return getLong(resultSet, column);
  }

  @SuppressWarnings("SameParameterValue")
  void setDt(SQLChange change, String column, long dt) {
    setLong(change, column, dt);
  }

  ValueApplicator idApplicator(Id id) {
    return (stmt, index) -> {
      setId(stmt, index, id);
      return 1;
    };
  }

  Id getId(ResultSet resultSet, String column) {
    return stringToId(getString(resultSet, column));
  }

  void setId(PreparedStatement pstmt, int index, Id id) {
    SQLError.run(() -> pstmt.setString(index, id != null ? id.toString() : null));
  }

  void setId(SQLChange change, String column, Id id) {
    change.setColumn(column, idApplicator(id));
  }

  @SuppressWarnings("SameParameterValue")
  ByteString getBinary(ResultSet resultSet, String column) {
    return SQLError.call(() -> ByteString.copyFrom(resultSet.getBytes(column)));
  }

  void setBinary(PreparedStatement pstmt, int i, ByteString binary) {
    SQLError.run(() -> pstmt.setBytes(i, binary != null ? binary.toByteArray() : null));
  }

  @SuppressWarnings("SameParameterValue")
  void setBinary(SQLChange change, String column, ByteString binary) {
    change.setColumn(column, (stmt, index) -> {
      setBinary(stmt, index, binary);
      return 1;
    });
  }

  String getString(ResultSet resultSet, String column) {
    return SQLError.call(() -> resultSet.getString(column));
  }

  void setString(PreparedStatement pstmt, int index, String string) {
    SQLError.run(() -> pstmt.setString(index, string));
  }

  void setString(SQLChange change, String column, String string) {
    change.setColumn(column, (stmt, index) -> {
      setString(stmt, index, string);
      return 1;
    });
  }

  long getLong(ResultSet resultSet, String column) {
    return SQLError.call(() -> resultSet.getLong(column));
  }

  void setLong(PreparedStatement pstmt, int i, Long value) {
    SQLError.run(() -> {
      if (value == null) {
        pstmt.setNull(i, Types.BIGINT);
      } else {
        pstmt.setLong(i, value);
      }
    });
  }

  void setLong(SQLChange change, String column, Long value) {
    change.setColumn(column, (stmt, index) -> {
      setLong(stmt, index, value);
      return 1;
    });
  }

  @SuppressWarnings("SameParameterValue")
  boolean getBoolean(ResultSet resultSet, String column) {
    return SQLError.call(() -> resultSet.getBoolean(column));
  }

  void setBoolean(PreparedStatement pstmt, int i, boolean bool) {
    SQLError.run(() -> pstmt.setBoolean(i, bool));
  }

  @SuppressWarnings("SameParameterValue")
  void setBoolean(SQLChange change, String column, boolean bool) {
    change.setColumn(column, (stmt, index) -> {
      setBoolean(stmt, index, bool);
      return 1;
    });
  }

  @SuppressWarnings("SameParameterValue")
  Stream<Key> getKeys(ResultSet resultSet, String column) {
    return getArray(resultSet, column, DatabaseAdapter::fromFlattened);
  }

  @SuppressWarnings("SameParameterValue")
  void setKeys(SQLChange change, String column, Stream<Key> keys) {
    setArray(change, column, keys, DatabaseAdapter::flattenKey, ColumnType.KEY_LIST);
  }

  Stream<Id> getIds(ResultSet resultSet, String column) {
    return getArray(resultSet, column, DatabaseAdapter::stringToId);
  }

  void setIds(SQLChange change, String column, Stream<Id> ids) {
    setArray(change, column, ids, id -> id != null ? id.toString() : "", ColumnType.ID_LIST);
  }

  void setStrings(SQLChange change, String column, List<String> array, ColumnType columnType) {
    setArray(change, column, array != null ? array.stream() : null, Function.identity(), columnType);
  }

  @SuppressWarnings("SameParameterValue")
  Stream<KeyDelta> getKeyDeltas(ResultSet resultSet, String column) {
    return getArray(resultSet, column, DatabaseAdapter::deserializeKeyDelta);
  }

  @SuppressWarnings("SameParameterValue")
  void setKeyDeltas(SQLChange change, String column, Stream<KeyDelta> keyDeltas) {
    setArray(change, column, keyDeltas, DatabaseAdapter::serializeKeyDelta, ColumnType.KEY_DELTA_LIST);
  }

  void getUnsavedDeltas(ResultSet resultSet, String column, UnsavedCommitDelta delta) {
    getArray(resultSet, column, Function.identity()).forEach(s -> unsavedDelta(s, delta));
  }

  void getMutations(ResultSet resultSet, String column, Consumer<Mutation> keyMutation) {
    getArray(resultSet, column, DatabaseAdapter::mutationFromString).forEach(keyMutation);
  }

  @SuppressWarnings("SameParameterValue")
  void setMutations(SQLChange change, String column, Stream<Mutation> mutations) {
    setArray(change, column, mutations, DatabaseAdapter::mutationAsString, ColumnType.KEY_MUTATION_LIST);
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
        setBoolean(stmt, index, value.getBoolean());
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

  <T> Stream<T> getArray(ResultSet resultSet, String column, Function<String, T> elementConverter) {
    return SQLError.call(() -> {
      Array array = resultSet.getArray(column);
      if (array != null) {
        Object[] arr = (Object[]) array.getArray();
        return Arrays.stream(arr).map(String.class::cast).map(elementConverter);
      }
      return Stream.empty();
    });
  }

  <E, S> void setArray(SQLChange change, String column, Stream<E> contents, Function<E, S> converter, ColumnType columnType) {
    if (contents == null) {
      change.setColumn(column, (stmt, index) -> {
        SQLError.run(() -> stmt.setObject(index, null));
        return 1;
      });
    } else {
      @SuppressWarnings("unchecked") S[] arr = contents.map(converter).toArray(l -> (S[]) new String[l]);
      change.setColumn(column, (stmt, index) -> {
        SQLError.run(() -> stmt.setObject(index, arr));
        return 1;
      });
    }
  }

  /**
   * Create an instance of a {@link DatabaseAdapter} defined as an inner class:
   * {@link H2}, {@link HSQL}, {@link Cockroach}, {@link PostgresQL}.
   * @param name Name of the database-adapter, see above.
   * @return created database-adapter
   */
  public static DatabaseAdapter create(String name) {
    switch (name) {
      case "H2":
        return new H2();
      case "HSQL":
        return new HSQL();
      case "Cockroach":
        return new Cockroach();
      case "PostgresQL":
        return new PostgresQL();
      default:
        throw new IllegalArgumentException("Unknown DatabaseAdapter " + name);
    }
  }
}
