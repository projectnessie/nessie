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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.thrift.TException;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.hms.NessieTransaction.Handle;
import org.projectnessie.hms.NessieTransaction.TableAndPartition;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A rawstore implementation that is backed by the Nessie REST API. */
public class NessieStoreImpl implements NessieStore {

  private static Logger LOG = LoggerFactory.getLogger(NessieStoreImpl.class);

  private static final String NESSIE_DB = "$nessie";

  private Configuration conf;
  private NessieClient client;

  private String ref;
  private NessieTransaction transaction;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    client = NessieClient.builder().fromConfig(conf::get).build();
    try {
      ref = client.getTreeApi().getDefaultBranch().getName();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Unable to retrieve default branch.", e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  String getRef() {
    return ref;
  }

  @Override
  public void shutdown() {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public boolean openTransaction() {
    if (transaction == null) {
      transaction = new NessieTransaction(ref, client, this::clearTransaction);
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
    if (transaction == null) {
      transaction = new NessieTransaction(ref, client, this::clearTransaction);
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
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    try (Handle h = txo()) {
      tx().createDatabase(db);
    }
  }

  private Database getNessieDb() {
    Database db = new Database();
    try {
      db.setCatalogName("hive");
    } catch (NoSuchMethodError e) {
      // ignore since this only exists on hive3.
    }
    db.setName(NESSIE_DB);
    db.setDescription(ref);
    return db;
  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    if (isNessie(name)) {
      return getNessieDb();
    }
    try (Handle h = txo()) {
      return tx().getDatabase(name).orElseThrow(() -> new NoSuchObjectException());
    }
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    try (Handle h = txo()) {
      tx().deleteDatabase(dbname);
      return true;
    }
  }

  private boolean handleNessieDb(Database db) throws MetaException {
    final String refProvided = db.getParameters().get("ref");

    new NessieTransaction(db.getLocationUri(), client, this::clearTransaction);
    if (refProvided == null) {
      try {
        this.ref = client.getTreeApi().getDefaultBranch().getName();
        return true;
      } catch (NessieNotFoundException e) {
        throw new MetaException(
            "Failure while trying to reset to default branch. Default branch does not exist on Nessie.");
      }
    }

    // this ensures the ref is valid.
    new NessieTransaction(refProvided, client, this::clearTransaction);
    this.ref = refProvided;
    return true;
  }

  @Override
  public boolean alterDatabase(String dbname, Database db)
      throws NoSuchObjectException, MetaException {
    if (dbname.equalsIgnoreCase(NESSIE_DB)) {
      return handleNessieDb(db);
    }

    try (Handle h = txo()) {
      tx().alterDatabase(db);
    }
    return true;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    try (Handle h = txo()) {
      return tx().getDatabases().collect(Collectors.toList());
    }
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    try (Handle h = txo()) {
      return tx().getDatabases().collect(Collectors.toList());
    }
  }

  private void createBranchOrTag(Table tbl) throws MetaException {
    String tblName = tbl.getTableName();

    if (!tbl.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
      throw new MetaException(
          "Branches/tags can be made only by creating a pseudo-view when using HiveQL. "
              + "For example CREATE VIEW v1 DBPROPERTIES(\"type\"=\"branch\", \"ref\"=\"abcd...\") as SELECT 1");
    }

    Reference requestedReference = null;
    try {
      String ref = tbl.getParameters().get("ref");
      if (ref != null) {
        requestedReference = client.getTreeApi().getReferenceByName(ref);
      }

    } catch (NessieNotFoundException ex) {
      throw new MetaException(
          String.format("The requested Nessie reference [%s] does not exist.", ref));
    }

    String type = tbl.getParameters().get("type");
    boolean branch = true;
    if (type != null) {
      if (type.equalsIgnoreCase("branch")) {
        // noop
      } else if (type.equalsIgnoreCase("tag")) {
        branch = false;
      } else {
        throw new MetaException(
            "Invalid Nessie object type. Expected 'tag' or 'branch' or nothing.");
      }
    }

    try {
      Reference reference =
          branch
              ? Branch.of(tblName, requestedReference.getHash())
              : Tag.of(tblName, requestedReference.getHash());
      client.getTreeApi().createReference(reference);
    } catch (NessieNotFoundException e) {
      throw new MetaException("Cannot find the defined reference.");
    } catch (NessieConflictException e) {
      throw new MetaException(
          "Cannot create provided branch or tag, one with that name already exists.");
    }
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    if (isNessie(tbl.getDbName())) {
      createBranchOrTag(tbl);
      return;
    }

    checkTableProperties(tbl);
    // Nessie always appends a random uuid to a default table path to avoid future conflicts on
    // insertions. Since
    // partitions have independent paths, this only influences insertions on an empty external
    // table.
    tbl.getSd().setLocation(tbl.getSd().getLocation() + "/" + UUID.randomUUID().toString());
    try (Handle h = txo()) {
      tx().createTable(tbl);
    }
  }

  private void checkTableProperties(Table tbl) throws MetaException {
    final boolean isExternalTable = TableType.EXTERNAL_TABLE.name().equals(tbl.getTableType());
    if (!isExternalTable && !TableType.VIRTUAL_VIEW.name().equals(tbl.getTableType())) {
      throw new MetaException(
          "Nessie only supports storing External Tables and Virtual Views. "
              + "This ensures Hive doesn't delete historical data from valid branches and/or tags.");
    }

    if (isExternalTable && "true".equals(tbl.getParameters().getOrDefault("immutable", "false"))) {
      return;
    }

    throw new MetaException(
        String.format(
            "Nessie only supports tables that carry the 'immutable=true' property. "
                + "This allows partition add/removal but disallows operations that skip the metastore."));
  }

  @Override
  public boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try (Handle h = txo()) {
      tx().deleteTable(dbName, tableName);
      return true;
    }
  }

  private Table getNessieTable(String tableName) {
    Reference hash;
    try {
      hash = client.getTreeApi().getReferenceByName(tableName);
    } catch (NessieNotFoundException e) {
      return null;
    }
    Table t = new Table();
    t.setCatName("hive");
    t.setDbName(NESSIE_DB);
    t.setTableName(tableName);
    t.setOwner("$nessie");
    t.setPartitionKeys(ImmutableList.of());
    t.setSd(new StorageDescriptor());
    t.getSd().setInputFormat("Nessie input format.");
    t.setParameters(ImmutableMap.of("hash", hash.getHash()));
    t.setPrivileges(new PrincipalPrivilegeSet());
    t.setRewriteEnabled(false);
    t.setTableType(TableType.EXTERNAL_TABLE.name());
    t.setTemporary(false);
    return t;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    if (isNessie(dbName)) {
      return getNessieTable(tableName);
    }

    try (Handle h = txo()) {
      try {

        return tx().getTableOnly(dbName, tableName);
      } catch (NoSuchObjectException e) {
        return null;
      }
    }
  }

  private boolean isNessie(String dbName) {
    return dbName.equalsIgnoreCase(NESSIE_DB);
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    return addPartitions(part.getDbName(), part.getTableName(), ImmutableList.of(part));
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    try (Handle h = txo()) {
      Optional<TableAndPartition> table;
      try {
        table = tx().getTable(parts.get(0).getDbName(), parts.get(0).getTableName());
      } catch (NoSuchObjectException e) {
        throw new InvalidObjectException(
            String.format(
                "Unable to find table [%s.%s] for partitions.",
                parts.get(0).getDbName(), parts.get(0).getTableName()));
      }
      if (!table.isPresent()) {
        throw new InvalidObjectException();
      }
      List<Partition> partitions = new ArrayList<>();
      partitions.addAll(table.get().getPartitions());
      partitions.addAll(parts);
      tx().save(new TableAndPartition(table.get().getTable(), partitions, table.get().getId()));
      return true;
    }
  }

  @Override
  public boolean addPartitions(
      String dbName, String tblName, PartitionSpecProxy partitionSpec, boolean ifNotExists)
      throws InvalidObjectException, MetaException {

    // TODO: handle ifNotExists.
    return addPartitions(
        dbName,
        tblName,
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(partitionSpec.getPartitionIterator(), 0), false)
            .collect(Collectors.toList()));
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> partitionValues)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      return tx().getPartitions(dbName, tableName).stream()
          .filter(p -> p.getValues().equals(partitionValues))
          .findFirst()
          .orElseThrow(() -> new NoSuchObjectException());
    }
  }

  @Override
  public Partition getPartitionWithAuth(
      String dbName,
      String tblName,
      List<String> partVals,
      String userName,
      List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return getPartition(dbName, tblName, partVals);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(
      String dbName, String tblName, short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return getPartitions(dbName, tblName, maxParts);
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> partitionValues)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      return tx().getPartitions(dbName, tableName).stream()
          .filter(p -> p.getValues().equals(partitionValues))
          .findFirst()
          .isPresent();
    }
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try (Handle h = txo()) {
      Optional<TableAndPartition> tandp = tx().getTable(dbName, tableName);
      if (!tandp.isPresent()) {
        throw new NoSuchObjectException();
      }
      List<Partition> newPartitions = new ArrayList<>();
      boolean found = false;
      for (Partition p : tandp.get().getPartitions()) {
        if (p.getValues().equals(partVals)) {
          found = true;
          continue;
        }
        newPartitions.add(p);
      }

      if (!found) {
        throw new InvalidObjectException();
      }

      tx().save(new TableAndPartition(tandp.get().getTable(), newPartitions, tandp.get().getId()));
    }

    return true;
  }

  @Override
  public void dropPartitions(String dbName, String tableName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      TableAndPartition tandp =
          tx().getTable(dbName, tableName).orElseThrow(() -> new NoSuchObjectException());
      List<Partition> newPartitions = new ArrayList<>();
      Set<String> dropNames = new HashSet<>(partNames);
      for (Partition p : tandp.getPartitions()) {
        String partName =
            Warehouse.makePartName(tandp.getTable().getPartitionKeys(), p.getValues());
        if (!dropNames.contains(partName)) {
          newPartitions.add(p);
        }
      }

      tx().save(new TableAndPartition(tandp.getTable(), newPartitions, tandp.getId()));
    }
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      return tx().getPartitions(dbName, tableName).stream()
          .limit(max == -1 ? Integer.MAX_VALUE : max)
          .collect(Collectors.toList());
    }
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {

    try (Handle h = txo()) {
      Table table = tx().getTableOnly(dbname, name);
      if (table == null) {
        throw new InvalidObjectException();
      }

      checkTableProperties(newTable);
      tx().alterTable(newTable);
    } catch (NoSuchObjectException e) {
      throw new InvalidObjectException();
    }
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    if (isNessie(dbName)) {
      return client.getTreeApi().getAllReferences().stream()
          .map(nr -> nr.getName())
          .collect(Collectors.toList());
    }

    try (Handle h = txo()) {
      // TODO: support pattern.
      return tx().getTables(dbName).map(k -> k.getElements().get(1)).collect(Collectors.toList());
    }
  }

  @Override
  public List<String> getTables(String dbName, String pattern, TableType tableType)
      throws MetaException {
    try (Handle h = txo()) {
      // TODO: support tabletype and pattern.
      return tx().getTables(dbName).map(k -> k.getElements().get(1)).collect(Collectors.toList());
    }
  }

  @Override
  public List<TableMeta> getTableMeta(String dbName, String tableNames, List<String> tableTypes)
      throws MetaException {
    try (Handle h = txo()) {
      // TODO: support tabletype and pattern.
      return tx().getTables(dbName)
          .map(
              k -> {
                TableMeta m = new TableMeta();
                m.setCatName("hive");
                m.setDbName(k.getElements().get(0));
                m.setTableName(k.getElements().get(1));

                // TODO:
                // m.setTableType(..);
                // m.setComments(..);

                return m;
              })
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, UnknownDBException {
    try (Handle h = txo()) {
      return tx().getTables(dbName, tableNames);
    }
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    try (Handle h = txo()) {
      return tx().getTables(dbName).map(k -> k.getElements().get(1)).collect(Collectors.toList());
    }
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws MetaException, UnknownDBException {

    // TODO: filter
    try (Handle h = txo()) {
      // TODO: support tabletype and pattern.
      return tx().getTables(dbName)
          .map(k -> k.getElements().get(1))
          .limit(maxTables)
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short maxParts)
      throws MetaException {
    try (Handle h = txo()) {
      TableAndPartition tbl =
          tx().getTable(dbName, tblName).orElseThrow(() -> new NoSuchObjectException());
      return tbl.getPartitions().stream()
          .map(
              p -> {
                try {
                  return Warehouse.makePartName(tbl.getTable().getPartitionKeys(), p.getValues());
                } catch (MetaException e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(Collectors.toList());
    } catch (NoSuchObjectException e) {
      throw new MetaException(
          String.format("Requested table [%s.%s] does not exist.", dbName, tblName));
    }
  }

  @Override
  public PartitionValuesResponse listPartitionValues(
      String dbName,
      String tblName,
      List<FieldSchema> cols,
      boolean applyDistinct,
      String filter,
      boolean ascending,
      List<FieldSchema> order,
      long maxParts)
      throws MetaException {
    throw new MetaException("Not yet supported.");
  }

  @Override
  public void alterPartition(
      String dbName, String tblName, List<String> partVals, Partition newPart)
      throws InvalidObjectException, MetaException {}

  @Override
  public void alterPartitions(
      String dbName, String tblName, List<List<String>> partValsList, List<Partition> newParts)
      throws InvalidObjectException, MetaException {}

  @Override
  public List<Partition> getPartitionsByFilter(
      String dbName, String tblName, String filter, short maxParts)
      throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public boolean getPartitionsByExpr(
      String dbName,
      String tblName,
      byte[] expr,
      String defaultPartitionName,
      short maxParts,
      List<Partition> result)
      throws TException {

    try (Handle h = txo()) {
      TableAndPartition tandp =
          tx().getTable(dbName, tblName).orElseThrow(() -> new NoSuchObjectException());
      List<FieldSchema> partitionKeys = tandp.getTable().getPartitionKeys();
      Map<String, Partition> partByName =
          tandp.getPartitions().stream()
              .collect(
                  Collectors.toMap(
                      p -> {
                        try {
                          return Warehouse.makePartName(partitionKeys, p.getValues());
                        } catch (MetaException e) {
                          throw new RuntimeException(e);
                        }
                      },
                      Function.identity()));
      List<String> partitionNames = new ArrayList<>(partByName.keySet());
      boolean resultBool =
          PartitionFilterer.filterPartitionsByExpr(
              conf, tandp.getTable().getPartitionKeys(), expr, partitionNames);
      for (String name : partitionNames) {
        result.add(partByName.get(name));
      }
      return resultBool;
    }
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException {
    return 1;
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    return 1;
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    try (Handle h = txo()) {
      TableAndPartition tandp =
          tx().getTable(dbName, tblName).orElseThrow(() -> new NoSuchObjectException());
      List<FieldSchema> partitionKeys = tandp.getTable().getPartitionKeys();
      Set<String> parts = new HashSet<>(partNames);

      return tandp.getPartitions().stream()
          .filter(
              p -> {
                String name;
                try {
                  name = Warehouse.makePartName(partitionKeys, p.getValues());
                } catch (MetaException e) {
                  throw new RuntimeException(e);
                }
                return parts.contains(name);
              })
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(
      String dbName,
      String tblName,
      List<String> partVals,
      short maxParts,
      String userName,
      List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    return null;
  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    return null;
  }

  @Override
  public List<String> listPartitionNamesByFilter(
      String dbName, String tblName, String filter, short maxParts) throws MetaException {
    return null;
  }

  private static PartitionExpressionProxy createExpressionProxy(Configuration conf) {
    String className = conf.get("hive.metastore.expression.proxy");
    try {

      Class<? extends PartitionExpressionProxy> clazz =
          getClass(className, PartitionExpressionProxy.class);
      return newInstance(clazz, new Class<?>[0], new Object[0]);
    } catch (MetaException e) {
      LOG.error("Error loading PartitionExpressionProxy", e);
      throw new RuntimeException("Error loading PartitionExpressionProxy: " + e.getMessage());
    }
  }

  private static <T> T newInstance(
      Class<T> theClass, Class<?>[] parameterTypes, Object[] initargs) {
    if (parameterTypes.length != initargs.length) {
      throw new IllegalArgumentException(
          "Number of constructor parameter types doesn't match number of arguments");
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> clazz = parameterTypes[i];
      if (initargs[i] != null && !(clazz.isInstance(initargs[i]))) {
        throw new IllegalArgumentException(
            "Object : " + initargs[i] + " is not an instance of " + clazz);
      }
    }

    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(parameterTypes);
      meth.setAccessible(true);
      return meth.newInstance(initargs);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate " + theClass.getName(), e);
    }
  }

  @SuppressWarnings(value = "unchecked")
  private static <T> Class<? extends T> getClass(String className, Class<T> clazz)
      throws MetaException {
    try {
      return (Class<? extends T>) Class.forName(className, true, getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new MetaException(className + " class not found");
    }
  }

  private static ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = JavaUtils.class.getClassLoader();
    }
    return classLoader;
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    return 0;
  }

  @Override
  public int getTableCount() throws MetaException {
    return 0;
  }

  @Override
  public int getPartitionCount() throws MetaException {
    return 0;
  }

  @Override
  public Map<String, List<String>> getPartitionColsWithStats(String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    return null;
  }
}
