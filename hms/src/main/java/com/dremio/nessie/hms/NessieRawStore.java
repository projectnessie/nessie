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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.thrift.TException;

import com.dremio.nessie.hms.HMSProto.CommitMetadata;
import com.dremio.nessie.hms.NessieTransaction.Handle;
import com.dremio.nessie.hms.NessieTransaction.TableAndPartition;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import com.dremio.nessie.versioned.impl.DynamoStore;
import com.dremio.nessie.versioned.impl.DynamoStoreConfig;
import com.dremio.nessie.versioned.impl.DynamoVersionStore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.regions.Region;

public class NessieRawStore extends NotSupportedRawStore {

  private static final String NESSIE_DB = "$nessie";

  private Configuration conf;
  private VersionStore<Item, CommitMetadata> store;

  private BranchName branch;
  private Hash transactionHash;
  private String ref = "main";


  private List<Operation> operations = new ArrayList<>();

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    StoreWorker<Item, CommitMetadata> worker = new StoreWorker<Item, CommitMetadata>(){

      @Override
      public Serializer<Item> getValueSerializer() {
        return new ItemSerializer();
      }

      @Override
      public Serializer<CommitMetadata> getMetadataSerializer() {
        return new CommitSerializer();
      }

      @Override
      public Stream<AssetKey> getAssetKeys(Item value) {
        return Stream.of();
      }

      @Override
      public CompletableFuture<Void> deleteAsset(AssetKey key) {
        return CompletableFuture.completedFuture(null);
      }};

    DynamoStore store;
    try {
      store = new DynamoStore(DynamoStoreConfig.builder()
          .endpoint(new URI("http://localhost:8000"))
          .region(Region.US_WEST_2)
          .build());
      this.store = new DynamoVersionStore<>(worker, store, true);
      store.start();

    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void shutdown() {
  }

  private NessieTransaction transaction;

  @Override
  public boolean openTransaction() {
    if(transaction == null) {
      transaction = new NessieTransaction(ref, store, this::clearTransaction);
      return true;
    }

    tx().nestedOpen();
    return false;
  }

  @Override
  public boolean commitTransaction() {
    return tx().commit();
  }

  private NessieTransaction tx() {
    Preconditions.checkArgument(transaction != null, "Transaction not currently active.");
    return transaction;
  }

  private Handle txo() {
    if(transaction == null) {
      transaction = new NessieTransaction(ref, store, this::clearTransaction);
      return transaction.handle();
    }

    return transaction.start();
  }

  private void clearTransaction() {
    this.transaction = null;
  }

  @Override
  public boolean isActiveTransaction() {
    return transaction != null;
  }

  @Override
  public void rollbackTransaction() {
    tx().rollback();
  }

  @Override
  public void verifySchema() throws MetaException {
  }


  @Override
  public List<String> getCatalogs() throws MetaException {
    return ImmutableList.of("hive");
  }

  @Override
  public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    if(!catalogName.equals("hive")) {
      throw new IllegalArgumentException();
    }

    Catalog c = new Catalog();
    c.setName(catalogName);
    c.setLocationUri("file:///tmp/hms");
    return c;
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    try (Handle h = txo()) {
      tx().createDatabase(db);
    }
  }

  private Database getNessieDb() {
    Database db = new Database();
    db.setCatalogName("hive");
    db.setName(NESSIE_DB);
    db.setDescription(ref);
    return db;
  }

  @Override
  public Database getDatabase(String catalogName, String name) throws NoSuchObjectException {
    if (isNessie(name)) {
      return getNessieDb();
    }
    try (Handle h = txo()) {
      return orThrow(tx().getDatabase(name));
    }
  }

  /**
   * Return the value if it exists, otherwise throw NoSuchObjectException.
   * @param <V> The type of the value.
   * @param value The value to Evaluate;
   * @return The value if exists.
   * @throws NoSuchObjectException Thrown if the value is null.
   */
  private <V> V orThrow(V value) throws NoSuchObjectException {
    if (value == null) {
      throw new NoSuchObjectException();
    }
    return value;
  }

  @Override
  public boolean dropDatabase(String catalogName, String dbname) throws NoSuchObjectException, MetaException {
    return false;
  }

  private boolean handleNessieDb(Database db) throws MetaException {
    if(!db.isSetLocationUri()) {
      throw new MetaException("You must provide a reference value for the Nessie reference database.");
    }

    // this ensures the ref is valid.
    new NessieTransaction(db.getLocationUri(), store, this::clearTransaction);
    this.ref = db.getLocationUri();
    return true;
  }

  @Override
  public boolean alterDatabase(String catalogName, String dbname, Database db)
      throws NoSuchObjectException, MetaException {
    if(dbname.equalsIgnoreCase(NESSIE_DB)) {
      return handleNessieDb(db);
    }

    try (Handle h = txo()) {
      tx().alterDatabase(db);
    }
    return true;
  }

  @Override
  public List<String> getDatabases(String catalogName, String pattern) throws MetaException {
    try (Handle h = txo()) {
      return tx().getDatabases().collect(Collectors.toList());
    }
  }

  @Override
  public List<String> getAllDatabases(String catalogName) throws MetaException {
    try (Handle h = txo()) {
      return tx().getDatabases().collect(Collectors.toList());
    }
  }

  private void createBranchOrTag(Table tbl) throws MetaException {
    String tblName = tbl.getTableName();
    String ref = tbl.getParameters().get("ref");
    if(ref == null) {
      throw new MetaException("Can't create branch with null ref.");
    }
    try {
    WithHash<Ref> target = store.toRef(ref);
    final NamedRef newRef;
    if (tblName.startsWith("tag:")) {
      newRef = TagName.of(tblName.substring(4));
    } else if (ref.startsWith("branch:")) {
      newRef = BranchName.of(tblName.substring(7));
    } else {
      // default is a branch.
      newRef = BranchName.of(tblName);
    }
      store.create(newRef, Optional.of(target.getHash()));
    } catch (ReferenceNotFoundException e) {
      throw new MetaException("Cannot find the defined reference.");
    } catch (ReferenceAlreadyExistsException e) {
      throw new MetaException("Cannot create provided branch or tag, one with that name already exists.");
    }
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    if(isNessie(tbl.getDbName())) {
      createBranchOrTag(tbl);
      return;
    }

    checkTableProperties(tbl);
    try (Handle h = txo()) {
      tx().createTable(tbl);
    }
  }

  private void checkTableProperties(Table tbl) throws MetaException {
    if (
        TableType.EXTERNAL_TABLE.name().equals(tbl.getTableType()) ||
        TableType.VIRTUAL_VIEW.name().equals(tbl.getTableType())
        ) {

      if("true".equals(tbl.getParameters().getOrDefault("immutable", "false"))) {
        return;
      }

      throw new MetaException(String.format("Nessie only supports tables that carry the 'immutable=true' property. This allows partition add/removal but disallows INSERTS that skip the metastore.", tbl.getTableType().toString()));
    }
    throw new MetaException(String.format("Nessie only supports storing External Tables and Virtual Views. This ensures Hive doesn't delete historical data from valid branches and/or tags.", tbl.getTableType().toString()));
  }

  @Override
  public boolean dropTable(String catalogName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try (Handle h = txo()) {
      tx().deleteTable(dbName, tableName);
      return true;
    }
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName) throws MetaException {
    if (isNessie(dbName)) {
      Hash hash;
      try {
        hash = store.toHash(BranchName.of(tableName));
      } catch (ReferenceNotFoundException e) {
        try {
          hash = store.toHash(TagName.of(tableName));
        } catch (ReferenceNotFoundException e1) {
          throw new MetaException(String.format("Unknown hash or tag name [%s].", tableName));
        }
      }
      Table t = new Table();
      t.setCatName(catalogName);
      t.setDbName(NESSIE_DB);
      t.setTableName(tableName);
      t.setOwner("$nessie");
      t.setPartitionKeys(ImmutableList.of());
      t.setSd(new StorageDescriptor());
      t.getSd().setInputFormat("Nessie input format.");
      t.setParameters(ImmutableMap.of("hash", hash.asString()));
      t.setPrivileges(new PrincipalPrivilegeSet());
      t.setRewriteEnabled(false);
      t.setTableType(TableType.EXTERNAL_TABLE.name());
      t.setTemporary(false);
      return t;
    }
    try (Handle h = txo()) {
      return tx().getTable(dbName, tableName);
    }
  }

  private boolean isNessie(String dbName) {
    return dbName.equalsIgnoreCase(NESSIE_DB);
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    return addPartitions(part.getCatName(), part.getDbName(), part.getTableName(), ImmutableList.of(part));
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    try (Handle h = txo()) {
      Optional<TableAndPartition> tAndP = tx().getTableAndPartitions(parts.get(0).getDbName(), parts.get(0).getTableName());
      if (!tAndP.isPresent()) {
        throw new InvalidObjectException();
      }
      List<Partition> partitions = new ArrayList<>();
      partitions.addAll(tAndP.get().getPartitions());
      partitions.addAll(parts);
      tx().save(new TableAndPartition(tAndP.get().getTable(), partitions));
      return true;
    }
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, PartitionSpecProxy partitionSpec,
      boolean ifNotExists) throws InvalidObjectException, MetaException {

    // TODO: handle ifNotExists.
    return addPartitions(
        catName,
        dbName,
        tblName,
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(partitionSpec.getPartitionIterator(), 0), false).collect(Collectors.toList())
        );
  }


  @Override
  public Partition getPartition(String catName, String dbName, String tableName, List<String> partitionValues)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      return tx().getPartitions(dbName, tableName)
          .stream()
          .filter(p -> p.getValues().equals(partitionValues))
          .findFirst()
          .orElseThrow(() -> new NoSuchObjectException());
    }
  }

  @Override
  public Partition getPartitionWithAuth(String catName, String dbName, String tblName, List<String> partVals,
      String user_name, List<String> group_names) throws MetaException, NoSuchObjectException, InvalidObjectException {
    return getPartition(catName, dbName, tblName, partVals);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, NoSuchObjectException, InvalidObjectException {
    return getPartitions(catName, dbName, tblName, maxParts);
  }

  @Override
  public boolean doesPartitionExist(String catName, String dbName, String tableName, List<String> partitionValues)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      return tx().getPartitions(dbName, tableName)
          .stream()
          .filter(p -> p.getValues().equals(partitionValues))
          .findFirst()
          .isPresent();
    }
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try (Handle h = txo()) {
      Optional<TableAndPartition> tandp = tx().getTableAndPartitions(dbName, tableName);
      if (!tandp.isPresent()) {
        throw new NoSuchObjectException();
      }
      List<Partition> newPartitions = new ArrayList<>();
      boolean found = false;
      for (Partition p : tandp.get().getPartitions()) {
        if(p.getValues().equals(part_vals)) {
          found = true;
          continue;
        }
        newPartitions.add(p);
      }

      if(!found) {
        throw new InvalidObjectException();
      }

      tx().save(new TableAndPartition(tandp.get().getTable(), newPartitions));
    }

    return true;
  }

  @Override
  public void dropPartitions(String catName, String dbName, String tableName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      TableAndPartition tandp = tx().getTableAndPartitions(dbName, tableName).orElseThrow(() -> new NoSuchObjectException());
      List<Partition> newPartitions = new ArrayList<>();
      Set<String> dropNames = new HashSet<>(partNames);
      for (Partition p : tandp.getPartitions()) {
        String partName = Warehouse.makePartName(tandp.getTable().getPartitionKeys(), p.getValues());
        if(!dropNames.contains(partName)) {
          newPartitions.add(p);
        }
      }

      tx().save(new TableAndPartition(tandp.getTable(), newPartitions));
    }
  }


  @Override
  public List<Partition> getPartitions(String catName, String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      return tx().getPartitions(dbName, tableName).stream().limit(max).collect(Collectors.toList());
    }
  }

  @Override
  public void alterTable(String catName, String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {

    try (Handle h = txo()) {
      Table table = tx().getTable(dbname, name);
      if (table == null) {
        throw new InvalidObjectException();
      }

      checkTableProperties(newTable);
      tx().alterTable(newTable);
    }

  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern) throws MetaException {
    if (isNessie(dbName)) {
      return store.getNamedRefs().map(nr -> nr.getValue().getName()).collect(Collectors.toList());
    }

    try (Handle h = txo()) {
      // TODO: support pattern.
      return tx().getTables(dbName).map(k -> k.getElements().get(1)).collect(Collectors.toList());
    }
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern, TableType tableType)
      throws MetaException {
    try (Handle h = txo()) {
      // TODO: support tabletype and pattern.
      return tx().getTables(dbName).map(k -> k.getElements().get(1)).collect(Collectors.toList());
    }
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbName, String tableNames, List<String> tableTypes)
      throws MetaException {
    try (Handle h = txo()) {
      // TODO: support tabletype and pattern.
      return tx().getTables(dbName).map(k -> {
        TableMeta m = new TableMeta();
        m.setCatName("hive");
        m.setDbName(k.getElements().get(0));
        m.setTableName(k.getElements().get(1));

        // TODO:
        //m.setTableType(..);
        //m.setComments(..);

        return m;
      }).collect(Collectors.toList());
    }
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
      throws MetaException, UnknownDBException {
    try (Handle h = txo()) {
      return tx().getTables(dbName, tableNames);
    }
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws MetaException {
    try (Handle h = txo()) {
      return tx().getTables(dbName).map(k -> k.getElements().get(1)).collect(Collectors.toList());
    }
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter, short max_tables)
      throws MetaException, UnknownDBException {

    // TODO: filter
    try (Handle h = txo()) {
      // TODO: support tabletype and pattern.
      return tx().getTables(dbName).map(k -> k.getElements().get(1)).limit(max_tables).collect(Collectors.toList());
    }
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name, short max_parts)
      throws MetaException {
    try (Handle h = txo()) {
      Table tbl = tx().getTable(db_name, tbl_name);
      return tx().getPartitions(db_name, tbl_name).stream().map(p -> {
        try {
          return Warehouse.makePartName(tbl.getPartitionKeys(), p.getValues());
        } catch (MetaException e) {
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());
    }
  }

  @Override
  public PartitionValuesResponse listPartitionValues(String catName, String db_name, String tbl_name,
      List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending, List<FieldSchema> order,
      long maxParts) throws MetaException {
    throw new MetaException("Not yet supported.");
  }

  @Override
  public void alterPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      Partition new_part) throws InvalidObjectException, MetaException {
  }

  @Override
  public void alterPartitions(String catName, String db_name, String tbl_name, List<List<String>> part_vals_list,
      List<Partition> new_parts) throws InvalidObjectException, MetaException {
  }

  @Override
  public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName, String filter,
      short maxParts) throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public boolean getPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    String defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    PartitionExpressionForMetastore pems = new PartitionExpressionForMetastore();
    try (Handle h = txo()) {
      TableAndPartition tandp = tx().getTableAndPartitions(dbName, tblName).orElseThrow(() -> new NoSuchObjectException());
      List<FieldSchema> partitionKeys = tandp.getTable().getPartitionKeys();
      Map<String, Partition> partByName = tandp.getPartitions().stream().collect(Collectors.toMap(p -> {
        try {
          return Warehouse.makePartName(partitionKeys, p.getValues());
        } catch (MetaException e) {
          throw new RuntimeException(e);
        }
      }, Function.identity()));
      List<String> partitionNames = new ArrayList<>(partByName.keySet());
      boolean resultBool = pems.filterPartitionsByExpr(tandp.getTable().getPartitionKeys(), expr, defaultPartName, partitionNames);
      for (String name : partitionNames) {
        result.add(partByName.get(name));
      }
      return resultBool;
    }
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException {
    return 0;
  }

  @Override
  public int getNumPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    return 0;
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws MetaException {
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public int deleteRuntimeStats(int maxRetainSecs) throws MetaException {
    return 0;
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name, String tbl_name) throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name, String parent_tbl_name,
      String foreign_db_name, String foreign_tbl_name) throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return Collections.emptyList();
  }


}
