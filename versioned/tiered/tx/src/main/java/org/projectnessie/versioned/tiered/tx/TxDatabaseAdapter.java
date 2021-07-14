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
package org.projectnessie.versioned.tiered.tx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.tiered.adapter.AbstractDatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.CommitLogEntry;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfiguration;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfiguration.ConfigItem;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfiguration.IntegerConfigItem;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfiguration.StringConfigItem;
import org.projectnessie.versioned.tiered.adapter.DbObjectsSerializers;
import org.projectnessie.versioned.tiered.adapter.GlobalStateLogEntry;
import org.projectnessie.versioned.tiered.adapter.GlobalStatePointer;
import org.projectnessie.versioned.tiered.tx.TxConnectionProvider.LocalConnectionProvider;

public abstract class TxDatabaseAdapter extends AbstractDatabaseAdapter<Connection> {

  protected static final String TABLE_COMMIT_LOG = "commit_log";
  protected static final String TABLE_GLOBAL_LOG = "global_log";
  protected static final String TABLE_GLOBAL_POINTER = "global_pointer";

  protected static final String CREATE_TABLE_GLOBAL_POINTER =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  global_id {1},\n"
              + "  value {0},\n"
              + "  PRIMARY KEY (key_prefix)\n"
              + ")",
          TABLE_GLOBAL_POINTER);
  protected static final String CREATE_TABLE_GLOBAL_LOG =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  id {1},\n"
              + "  value {0},\n"
              + "  PRIMARY KEY (key_prefix, id)\n"
              + ")",
          TABLE_GLOBAL_LOG);
  protected static final String CREATE_TABLE_COMMIT_LOG =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  hash {1},\n"
              + "  value {0},\n"
              + "  PRIMARY KEY (key_prefix, hash)\n"
              + ")",
          TABLE_COMMIT_LOG);

  protected static final String SELECT_GLOBAL_POINTER =
      String.format("SELECT value FROM %s WHERE key_prefix = ?", TABLE_GLOBAL_POINTER);
  protected static final String SELECT_GLOBAL_LOG =
      String.format("SELECT value FROM %s WHERE key_prefix = ? AND id = ?", TABLE_GLOBAL_LOG);
  protected static final String SELECT_COMMIT_LOG =
      String.format("SELECT value FROM %s WHERE key_prefix = ? AND hash = ?", TABLE_COMMIT_LOG);
  protected static final String SELECT_GLOBAL_LOG_MANY =
      String.format("SELECT value FROM %s WHERE key_prefix = ? AND id IN (%%s)", TABLE_GLOBAL_LOG);
  protected static final String SELECT_COMMIT_LOG_MANY =
      String.format(
          "SELECT value FROM %s WHERE key_prefix = ? AND hash IN (%%s)", TABLE_COMMIT_LOG);
  protected static final String DELETE_GLOBAL_POINTER =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_GLOBAL_POINTER);
  protected static final String INSERT_GLOBAL_POINTER =
      String.format(
          "INSERT INTO %s (key_prefix, global_id, value) VALUES (?, ?, ?)", TABLE_GLOBAL_POINTER);
  protected static final String UPDATE_GLOBAL_POINTER =
      String.format(
          "UPDATE %s SET value = ?, global_id = ? WHERE key_prefix = ? AND global_id = ?",
          TABLE_GLOBAL_POINTER);
  protected static final String INSERT_COMMIT_LOG =
      String.format("INSERT INTO %s (key_prefix, hash, value) VALUES (?, ?, ?)", TABLE_COMMIT_LOG);
  protected static final String INSERT_GLOBAL_LOG =
      String.format("INSERT INTO %s (key_prefix, id, value) VALUES (?, ?, ?)", TABLE_GLOBAL_LOG);

  public static final ConfigItem<TxConnectionProvider> CONNECTION_PROVIDER =
      new ConfigItem<>("tx.connection-provider", null, false);
  public static final ConfigItem<Integer> BATCH_SIZE =
      new IntegerConfigItem("tx.db.batch-size", 20, false);
  public static final ConfigItem<String> CATALOG =
      new StringConfigItem("tx.db.catalog", null, false);
  public static final ConfigItem<String> SCHEMA = new StringConfigItem("tx.db.schema", null, false);

  private static final ObjectMapper MAPPER = DbObjectsSerializers.register(new IonObjectMapper());

  private final String keyPrefix;
  private final TxConnectionProvider db;

  public TxDatabaseAdapter(DatabaseAdapterConfiguration config) {
    super(config);

    this.keyPrefix = KEY_PREFIX.get(config);

    // get the externally configured RocksDbInstance
    TxConnectionProvider db = CONNECTION_PROVIDER.get(config);

    if (db == null) {
      // Create a ConnectionProvider, if none has been configured externally. This is mostly used
      // for tests and benchmarks.
      db = new LocalConnectionProvider();
      db.configure(config);
      db.setupDatabase(
          allCreateTableDDL(),
          databaseSqlFormatParameters(),
          metadataUpperCase(),
          batchDDL(),
          CATALOG.get(config),
          SCHEMA.get(config));
    }

    this.db = db;
  }

  private byte[] serialize(Object entry) {
    try {
      return MAPPER.writeValueAsBytes(entry);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T deserialize(byte[] bytes, Class<T> type) {
    try {
      return MAPPER.readValue(bytes, type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    db.close();
  }

  @Override
  protected Connection operationContext() {
    try {
      return db.borrowConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void ctxCommit(Connection ctx) {
    try {
      ctx.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(Connection c) {
    try (PreparedStatement ps = c.prepareStatement(SELECT_GLOBAL_POINTER)) {
      ps.setString(1, keyPrefix);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return deserialize(rs.getBytes(1), GlobalStatePointer.class);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(Connection c, Hash id) {
    return fetchSingle(c, SELECT_GLOBAL_LOG, id, GlobalStateLogEntry.class);
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(Connection c, Hash hash) {
    return fetchSingle(c, SELECT_COMMIT_LOG, hash, CommitLogEntry.class);
  }

  private <T> T fetchSingle(Connection c, String sql, Hash pk, Class<T> type) {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, keyPrefix);
      ps.setString(2, pk.asString());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? deserialize(rs.getBytes(1), type) : null;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(Connection c, List<Hash> hashes) {
    return fetchPage(
        c, SELECT_GLOBAL_LOG_MANY, hashes, GlobalStateLogEntry.class, GlobalStateLogEntry::getId);
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(Connection c, List<Hash> hashes) {
    return fetchPage(
        c, SELECT_COMMIT_LOG_MANY, hashes, CommitLogEntry.class, CommitLogEntry::getHash);
  }

  private <T> List<T> fetchPage(
      Connection c, String sql, List<Hash> hashes, Class<T> type, Function<T, Hash> keyExtracter) {
    String placeholders =
        IntStream.range(0, hashes.size()).mapToObj(x -> "?").collect(Collectors.joining(", "));
    sql = String.format(sql, placeholders);

    try (PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, keyPrefix);
      for (int i = 0; i < hashes.size(); i++) {
        ps.setString(2 + i, hashes.get(i).asString());
      }

      Map<Hash, T> result = new HashMap<>(hashes.size() * 2);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          T entry = deserialize(rs.getBytes(1), type);
          Hash key = keyExtracter.apply(entry);
          result.put(key, entry);
        }
      }
      return hashes.stream().map(result::get).collect(Collectors.toList());
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected void unsafeWriteGlobalPointer(Connection c, GlobalStatePointer pointer) {
    try (PreparedStatement ps = c.prepareStatement(DELETE_GLOBAL_POINTER)) {
      ps.setString(1, keyPrefix);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
    try (PreparedStatement ps = c.prepareStatement(INSERT_GLOBAL_POINTER)) {
      ps.setString(1, keyPrefix);
      ps.setString(2, pointer.getGlobalId().asString());
      ps.setBytes(3, serialize(pointer));
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected void writeIndividualCommit(Connection c, CommitLogEntry entry)
      throws ReferenceConflictException {
    try (PreparedStatement ps = c.prepareStatement(INSERT_COMMIT_LOG)) {
      ps.setString(1, keyPrefix);
      ps.setString(2, entry.getHash().asString());
      ps.setBytes(3, serialize(entry));
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected void writeIndividualCommits(Connection c, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    int batchSize = BATCH_SIZE.get(config);
    int cnt = 0;
    try (PreparedStatement ps = c.prepareStatement(INSERT_COMMIT_LOG)) {
      for (CommitLogEntry e : entries) {
        ps.setString(1, keyPrefix);
        ps.setString(2, e.getHash().asString());
        ps.setBytes(3, serialize(e));
        ps.addBatch();
        cnt++;
        if (cnt == batchSize) {
          ps.executeBatch();
          cnt = 0;
        }
      }
      if (cnt > 0) {
        ps.executeBatch();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected void writeGlobalCommit(Connection c, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    try (PreparedStatement ps = c.prepareStatement(INSERT_GLOBAL_LOG)) {
      ps.setString(1, keyPrefix);
      ps.setString(2, entry.getId().asString());
      ps.setBytes(3, serialize(entry));
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected boolean globalPointerCas(
      Connection c, GlobalStatePointer expected, GlobalStatePointer newPointer) {
    try (PreparedStatement ps = c.prepareStatement(UPDATE_GLOBAL_POINTER)) {
      // "UPDATE %s SET value = ? WHERE key_prefix = ? AND global_id = ?", TABLE_GLOBAL_POINTER);
      ps.setBytes(1, serialize(newPointer));
      ps.setString(2, newPointer.getGlobalId().asString());
      ps.setString(3, keyPrefix);
      ps.setString(4, expected.getGlobalId().asString());
      return ps.executeUpdate() == 1;
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO VersionStoreException
    }
  }

  @Override
  protected void cleanUpCommitCas(Connection c, Hash globalId, Set<Hash> branchCommits) {}

  @VisibleForTesting
  public void dropTables() {
    db.dropTables(allCreateTableDDL().keySet(), batchDDL());
  }

  protected Map<String, List<String>> allCreateTableDDL() {
    return ImmutableMap.<String, List<String>>builder()
        .put(TABLE_GLOBAL_POINTER, Collections.singletonList(CREATE_TABLE_GLOBAL_POINTER))
        .put(TABLE_GLOBAL_LOG, Collections.singletonList(CREATE_TABLE_GLOBAL_LOG))
        .put(TABLE_COMMIT_LOG, Collections.singletonList(CREATE_TABLE_COMMIT_LOG))
        .build();
  }

  /**
   * Get database-specific 'strings' like column definitions for 'BLOB' column types.
   *
   * <ul>
   *   <li>Index #0: column-type string for a 'BLOB' column.
   *   <li>Index #1: column-type string for the string representation of a {@link Hash}
   *   <li>Index #2: column-type string for key-prefix
   * </ul>
   */
  protected abstract List<String> databaseSqlFormatParameters();

  protected boolean metadataUpperCase() {
    return true;
  }

  protected boolean batchDDL() {
    return false;
  }
}
