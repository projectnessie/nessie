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
package com.dremio.nessie.hms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import com.dremio.nessie.hms.HMSProto.CommitMetadata;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.VersionStore;
import com.google.common.base.Preconditions;


/**
 * Class for maintaining metastore transactionality.
 */
class NessieTransaction {

  private final Hash hash;
  private final boolean writable;
  private final BranchName branch;

  private final VersionStore<Item, CommitMetadata> versionStore;
  private final Map<Key, Operation<Item>> operations = new HashMap<>();

  private int transactionCount;
  private boolean rolledback;
  private final Runnable closeListener;
  private final Handle handle;

  public NessieTransaction(Configuration conf, VersionStore<Item, CommitMetadata> store, Runnable closeListener) {
    super();
    final String name = conf.get(MetastoreConf.ConfVars.NESSIE_REF.getVarname(), "main");
    this.closeListener = closeListener;
    this.versionStore = store;
    this.handle = new Handle();
    Hash hash = null;
    BranchName branch = null;

    try {
      BranchName tempBranch = BranchName.of(name);
      hash = store.toHash(tempBranch);
      branch = tempBranch;
    } catch (ReferenceNotFoundException ex) {
      // not a branch.
    }

    if (hash == null) {
      try {
        hash = store.toHash(TagName.of(name));
      } catch (ReferenceNotFoundException ex) {
        // not a tag.
      }
    }

    if (hash == null ) {
      Hash trialHash ;

      try {
        trialHash = Hash.of(name);
        store.getValue(trialHash, Key.of("it","doesn't","matter"));
        hash = trialHash;
      } catch (IllegalArgumentException | ReferenceNotFoundException ex) {
         // not a hash.
      }

    }

    if(hash == null) {
      throw new IllegalArgumentException(String.format("Unknown reference: %s", name));
    }

    this.writable = branch != null;
    this.branch = branch;
    this.hash = hash;
    transactionCount++;
  }

  public void nestedOpen() {
    Preconditions.checkArgument(transactionCount >= 1);
    transactionCount++;
  }

  public void rollback() {
    Preconditions.checkArgument(transactionCount >= 1);
    transactionCount--;
    this.rolledback = true;

    if (transactionCount == 0) {
      closeListener.run();
    }
  }

  public boolean commit() {
    Preconditions.checkArgument(transactionCount >= 1);

    if (rolledback) {
      return false;
    }

    if (transactionCount > 1) {
      transactionCount--;
      return true;
    }

    if (operations.isEmpty()) {
      transactionCount--;
      closeListener.run();
      return true;
    }

    // we have operations to commit.
    checkWritable();
    try {
      versionStore.commit(branch, Optional.of(hash), CommitMetadata.getDefaultInstance(), operations.values().stream().collect(Collectors.toList()));
      transactionCount--;
      closeListener.run();
      return true;
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      return false;
    }

  }

  public List<Table> getTables(String dbName, List<String> tableNames) {
    List<Key> keys = tableNames.stream().map(t -> Key.of(dbName, t)).collect(Collectors.toList());
    try {
    return versionStore.getValue(hash, keys).stream().map(v -> v.map(i -> i.getTable()).orElse(null)).collect(Collectors.toList());
    } catch (ReferenceNotFoundException ex) {
      // shouldn't happen as we just grabbed the hash a moment ago...
      throw new RuntimeException(ex);
    }
  }

  public void createDatabase(Database db) throws MetaException {
    setItem(Item.wrap(db), db.getName());
  }

  public void alterDatabase(Database db) throws MetaException {
    setItem(Item.wrap(db), db.getName());
  }

  public Stream<String> getDatabases() {
    try {
      return versionStore.getKeys(hash)
        .filter(k -> k.getElements().size() == 1)
        .map(k -> k.getElements().get(0));
    } catch (ReferenceNotFoundException e) {
      // shouldn't happen as we just grabbed the hash a moment ago...
      throw new RuntimeException(e);
    }
  }

  public Stream<Key> getTables(String database) {
    try {
      return versionStore.getKeys(hash)
        .filter(k -> k.getElements().size() != 1)
        .filter(k -> k.getElements().get(0).equalsIgnoreCase(database));
    } catch (ReferenceNotFoundException e) {
      // shouldn't happen as we just grabbed the hash a moment ago...
      throw new RuntimeException(e);
    }
  }

  public void addPartitions(List<Partition> p) throws MetaException, InvalidObjectException {
    if(p.isEmpty()) {
      return;
    }
    Optional<Item> tAndP = getItem(p.get(0).getDbName(), p.get(0).getTableName());
    if (!tAndP.isPresent()) {
      throw new InvalidObjectException();
    }
    List<Partition> partitions = new ArrayList<>();
    partitions.addAll(tAndP.get().getPartitions());
    partitions.addAll(p);
    setItem(Item.wrap(tAndP.get().getTable(), partitions));
  }

  public void createTable(Table table) throws MetaException {
    setItem(Item.wrap(table, Collections.emptyList()), table.getDbName(), table.getTableName());
  }

  public void alterTable(Table table) throws MetaException {
    Optional<Item> oldItem = getItem(table.getDbName(), table.getTableName());
    if(!oldItem.isPresent()) {
      throw new MetaException("Table doesn't exist.");
    }

    setItem(Item.wrap(table, oldItem.get().getPartitions()), table.getDbName(), table.getTableName());
  }

  public Handle handle() {
    return handle;
  }

  public Optional<TableAndPartition> getTableAndPartitions(String dbName, String tableName) {
    return getItem(dbName, tableName).map(i -> new TableAndPartition(i.getTable(), i.getPartitions()));
  }

  public void save(TableAndPartition tandp) throws MetaException {
    setItem(Item.wrap(tandp.table, tandp.partitions), tandp.table.getDbName(), tandp.table.getTableName());
  }

  private void setItem(Item item, String...keyElements) throws MetaException {
    checkWritable();
    Key key = Key.of(keyElements);
    operations.put(key, Put.of(key, item));
  }

  public void deleteTable(String dbName, String tableName) {
    deleteTable(dbName, tableName);
  }

  public void deleteDatabase(String db) throws MetaException {
    deleteItem(db);
  }

  private void deleteItem(String... keyElements) throws MetaException {
    checkWritable();
    Key key = Key.of(keyElements);
    operations.put(key, Delete.of(key));
  }

  private void checkWritable() {
    if (!writable) {
      throw new IllegalArgumentException("Changes can only be applied to branches. The provided ref is not a branch.");
    }
  }

  public Database getDatabase(String dbName) {
    return getItem(dbName).map(Item::getDatabase).orElse(null);
  }

  public Table getTable(String dbName, String tableName) {
    return getItem(dbName, tableName).map(Item::getTable).orElse(null);
  }

  public List<Partition> getPartitions(String dbName, String tableName) {
    return getItem(dbName, tableName).map(Item::getPartitions).orElse(null);
  }

  public void removePartition(String dbName, String tableName, List<String> partitionValues) throws MetaException, NoSuchObjectException {
    List<Partition> newPartitions = new ArrayList<>();
    Optional<Item> opt = getItem(dbName, tableName);
    if (!opt.isPresent()) {
      throw new NoSuchObjectException();
    }

    for(Partition p : opt.get().getPartitions()) {
      if(p.getValues().equals(partitionValues)) {
        continue;
      }
      newPartitions.add(p);
    }

    setItem(Item.wrap(opt.get().getTable(), newPartitions));

  }

  private Optional<Item> getItem(String... elements) {
    Key key = Key.of(elements);
    try {
      return Optional.ofNullable(versionStore.getValue(hash, key));
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Handle start() {
    nestedOpen();
    return new Handle();
  }

  public boolean isActive() {
    return transactionCount > 0;
  }


  public void execute(Consumer<NessieTransaction> t) {

  }
  public class Handle implements AutoCloseable {

    @Override
    public void close() {
      boolean success = false;
      try {
        success = commit();
      } finally {
        if(!success) {
          rollback();
        }
      }
    }

  }

  public static class TableAndPartition {
    private final Table table;
    private final List<Partition> partitions;

    public TableAndPartition(Table table, List<Partition> partitions) {
      super();
      this.table = table;
      this.partitions = partitions;
    }

    public Table getTable() {
      return table;
    }

    public List<Partition> getPartitions() {
      return partitions;
    }
  }
}
