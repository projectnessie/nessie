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
package org.projectnessie.hms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.hms.TransactionStore.RefKey;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Reference;

import com.google.common.base.Preconditions;


/**
 * Class for maintaining metastore transactionality.
 *
 * <p>When created, grabs the current hash of the provided reference. All non-qualified reads are done against this hash.
 *
 * <p>Leverages TransactionStore to make sure that all reads and writes are maintained a consistent view.
 */
class NessieTransaction {

  private final String defaultName;
  private final String defaultHash;
  private final TransactionStore store;
  private int transactionCount;
  private boolean rolledback;
  private final Runnable closeListener;
  private final Handle handle;

  public NessieTransaction(String ref, NessieClient client, Runnable closeListener) {
    super();
    this.closeListener = closeListener;
    this.handle = new Handle();

    final Reference reference;
    try {
      if (ref == null) {
        reference = client.getTreeApi().getDefaultBranch();
      } else {
        reference = client.getTreeApi().getReferenceByName(ref);
      }
    } catch (NessieNotFoundException e) {
      if (ref == null) {
        throw new RuntimeException("Cannot start transaction, unable to retrieve default branch from server.", e);
      }
      throw new RuntimeException(String.format("Cannot start transaction, Provided reference [%s] does not exist.", ref), e);
    }

    this.store = new TransactionStore(reference, client.getContentsApi(), client.getTreeApi());
    this.defaultHash = reference.getHash();
    this.defaultName = reference.getName();
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

    try {
      store.commit();
      transactionCount--;
      closeListener.run();
      return true;
    } catch (NessieNotFoundException | NessieConflictException e) {
      return false;
    }

  }

  public List<Table> getTables(String dbName, List<String> tableNames) {
    List<RefKey> keys = tableNames.stream().map(t -> new RefKey(defaultHash, ContentsKey.of(dbName, t))).collect(Collectors.toList());
    try {
      return store.getItemsForRef(keys).stream()
          .filter(Optional::isPresent)
          .map(Optional::get)
          .map(Item::getTable)
          .collect(Collectors.toList());
    } catch (NessieNotFoundException ex) {
      // shouldn't happen as we just grabbed the hash a moment ago...
      throw new RuntimeException("Selected reference disappeared during operation.", ex);
    }
  }

  Stream<ContentsKey> getTables(String database) {
    try {
      return store.getEntriesForDefaultRef()
          .map(EntriesResponse.Entry::getName)
          .filter(k -> k.getElements().size() != 1)
          .filter(k -> k.getElements().get(0).equalsIgnoreCase(database));
    } catch (NessieNotFoundException e) {
      // shouldn't happen as we just grabbed the hash a moment ago...
      throw new RuntimeException(e);
    }
  }

  public void createDatabase(Database db) throws MetaException {
    setItem(Item.wrap(db, UUID.randomUUID().toString()), db.getName());
  }

  public void alterDatabase(Database db) throws MetaException {
    try {
      Optional<Item> oldDb = getItemForRef(defaultHash, db.getName());
      setItem(Item.wrap(db, oldDb.orElseThrow(() -> new MetaException("Db not found")).getUuid()), db.getName());
    } catch (NoSuchObjectException e) {
      //can't happen
    }

  }

  public Stream<String> getDatabases() {
    try {
      return store.getEntriesForDefaultRef()
          .map(EntriesResponse.Entry::getName)
          .filter(k -> k.getElements().size() == 1)
          .map(k -> k.getElements().get(0));
    } catch (NessieNotFoundException e) {
      // shouldn't happen as we just grabbed the hash a moment ago...
      throw new RuntimeException(e);
    }
  }

  public void addPartitions(List<Partition> p) throws MetaException, InvalidObjectException, NoSuchObjectException {
    if (p.isEmpty()) {
      return;
    }
    Optional<TableAndPartition> table = getTable(p.get(0).getDbName(), p.get(0).getTableName());
    if (!table.isPresent()) {
      throw new InvalidObjectException();
    }
    List<Partition> partitions = new ArrayList<>();
    partitions.addAll(table.get().getPartitions());
    partitions.addAll(p);
    setItem(Item.wrap(table.get().getTable(), partitions, table.get().getUuid()));
  }

  public void createTable(Table table) throws MetaException {
    setItem(Item.wrap(table, Collections.emptyList(), UUID.randomUUID().toString()), table.getDbName(), table.getTableName());
  }

  public void alterTable(Table table) throws MetaException, NoSuchObjectException {
    Optional<TableAndPartition> oldItem = getTable(table.getDbName(), table.getTableName());
    if (!oldItem.isPresent()) {
      throw new MetaException("Table doesn't exist.");
    }

    setItem(Item.wrap(table, oldItem.get().getPartitions(), oldItem.get().getUuid()), table.getDbName(), table.getTableName());
  }

  public Handle handle() {
    return handle;
  }

  public Table getTableOnly(String dbName, String tableName) throws NoSuchObjectException {
    return getTable(dbName, tableName).map(TableAndPartition::getTable).orElseThrow(() -> new NoSuchObjectException());
  }

  public Optional<TableAndPartition> getTable(String dbName, String tableName) throws NoSuchObjectException {

    if (!tableName.contains("@")) {
      return getItemForRef(defaultHash, dbName, tableName)
          .map(i -> new TableAndPartition(i.getTable(), i.getPartitions(), i.getUuid()));
    }

    final String ref;
    final String tName;
    String[] split = tableName.split("@");
    if (split.length != 2) {
      throw new NoSuchObjectException("Invalid reference.");
    }
    tName = split[0];
    ref = split[1];

    if (ref.equalsIgnoreCase(defaultName) || ref.equalsIgnoreCase(defaultHash)) {
      // stay in transaction rather than possibly doing a newer read.
      return getItemForRef(defaultHash, dbName, tableName)
          .map(i -> new TableAndPartition(i.getTable(), i.getPartitions(), i.getUuid()));
    }

    return getItemForRef(ref, dbName, tName).map(i -> {
      Table t = i.getTable();
      t.setTableName(t.getTableName() + "@" + ref);
      List<Partition> parts = i.getPartitions();
      parts.forEach(p -> p.setTableName(p.getTableName() + "@" + ref));
      return new TableAndPartition(t, parts, i.getUuid());
    });
  }

  public void save(TableAndPartition tandp) throws MetaException {
    setItem(Item.wrap(tandp.table, tandp.partitions, tandp.getUuid()), tandp.table.getDbName(), tandp.table.getTableName());
  }

  private void setItem(Item item, String...keyElements) throws MetaException {
    ContentsKey key = ContentsKey.of(keyElements);
    store.setItem(key, item);
  }

  public void deleteTable(String dbName, String tableName) throws MetaException {
    deleteItem(dbName, tableName);
  }

  public void deleteDatabase(String db) throws MetaException {
    deleteItem(db);
  }

  private void deleteItem(String... keyElements) throws MetaException {
    ContentsKey key = ContentsKey.of(keyElements);
    store.deleteItem(key);
  }

  public List<Partition> getPartitions(String dbName, String tableName) throws NoSuchObjectException {
    Optional<TableAndPartition> item = getTable(dbName, tableName);
    return item.map(TableAndPartition::getPartitions).orElse(null);
  }

  public void removePartition(String dbName, String tableName, List<String> partitionValues) throws MetaException, NoSuchObjectException {
    List<Partition> newPartitions = new ArrayList<>();
    Optional<TableAndPartition> opt = getTable(dbName, tableName);
    if (!opt.isPresent()) {
      throw new NoSuchObjectException();
    }

    for (Partition p : opt.get().getPartitions()) {
      if (p.getValues().equals(partitionValues)) {
        continue;
      }
      newPartitions.add(p);
    }

    setItem(Item.wrap(opt.get().getTable(), newPartitions, opt.get().getUuid()));

  }

  Optional<Database> getDatabase(String database) throws NoSuchObjectException {
    return getItemForRef(defaultHash, database).map(Item::getDatabase);
  }

  private Optional<Item> getItemForRef(String ref, String... elements) throws NoSuchObjectException {
    return store.getItemForRef(ref, ContentsKey.of(elements));
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
        if (!success) {
          rollback();
        }
      }
    }

  }

  public static class TableAndPartition {
    private final Table table;
    private final List<Partition> partitions;
    private final String uuid;

    public TableAndPartition(Item i) {
      this(i.getTable(), i.getPartitions(), i.getUuid());
    }

    public TableAndPartition(Table table, List<Partition> partitions, String uuid) {
      super();
      this.table = table;
      this.partitions = partitions;
      this.uuid = uuid;
    }

    public Table getTable() {
      return table;
    }

    public List<Partition> getPartitions() {
      return partitions;
    }

    public String getUuid() {
      return uuid;
    }
  }
}
