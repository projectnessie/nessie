package com.dremio.nessie.hms;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.FullTableName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceRawStore implements RawStore {
  // this is done as a physical class rather than solely a proxy because the class name must be provided to Hive via configuration.
  private static final Logger LOGGER = LoggerFactory.getLogger(TraceRawStore.class);

  private final RawStore delegate;
  private final RawStore inner = new NessieRawStore();
  private Configuration conf;

  public TraceRawStore() {

    delegate = (RawStore) Proxy.newProxyInstance(NessieRawStore.class.getClassLoader(), new Class[]{RawStore.class}, new InvocationHandler() {

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (conf != null) {
          System.out.println("Nessie Ref: " + conf.get("nessie.ref"));
        }
        if (args == null) {
          System.out.print(String.format("%s()", method.getName()));
        } else {
          System.out.print(String.format("%s(%s)", method.getName(), args.length == 0 ? "" : Stream.of(args).map(o -> o == null ? null : o.toString()).collect(Collectors.joining(", "))));
        }

        try {
          boolean isNoReturn = method.getReturnType().equals(void.class) || method.getReturnType().equals(Void.class);
          Object output = method.invoke(inner, args);
          if(isNoReturn) {
            System.out.println(" <no return>");
          } else {
            System.out.println(output == null ? " ==> null" : " ==> " + output.toString());
          }
          return output;
        } catch(InvocationTargetException ex) {
          System.out.println(String.format(" ==> %s: %s", ex.getCause().getClass().getSimpleName(), ex.getCause().getMessage()));
          throw ex.getCause();
        }
      }
    });
  }

  public void setConf(Configuration conf) {
    delegate.setConf(conf);
    this.conf = conf;
  }

  public Configuration getConf() {
    return delegate.getConf();
  }

  public void shutdown() {
    delegate.shutdown();
  }

  public boolean openTransaction() {
    return delegate.openTransaction();
  }

  public boolean commitTransaction() {
    return delegate.commitTransaction();
  }

  public boolean isActiveTransaction() {
    return delegate.isActiveTransaction();
  }

  public void rollbackTransaction() {
    delegate.rollbackTransaction();
  }

  public void createCatalog(Catalog cat) throws MetaException {
    delegate.createCatalog(cat);
  }

  public void alterCatalog(String catName, Catalog cat) throws MetaException, InvalidOperationException {
    delegate.alterCatalog(catName, cat);
  }

  public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    return delegate.getCatalog(catalogName);
  }

  public List<String> getCatalogs() throws MetaException {
    return delegate.getCatalogs();
  }

  public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    delegate.dropCatalog(catalogName);
  }

  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    delegate.createDatabase(db);
  }

  public Database getDatabase(String catalogName, String name) throws NoSuchObjectException {
    return delegate.getDatabase(catalogName, name);
  }

  public boolean dropDatabase(String catalogName, String dbname) throws NoSuchObjectException, MetaException {
    return delegate.dropDatabase(catalogName, dbname);
  }

  public boolean alterDatabase(String catalogName, String dbname, Database db)
      throws NoSuchObjectException, MetaException {
    return delegate.alterDatabase(catalogName, dbname, db);
  }

  public List<String> getDatabases(String catalogName, String pattern) throws MetaException {
    return delegate.getDatabases(catalogName, pattern);
  }

  public List<String> getAllDatabases(String catalogName) throws MetaException {
    return delegate.getAllDatabases(catalogName);
  }

  public boolean createType(Type type) {
    return delegate.createType(type);
  }

  public Type getType(String typeName) {
    return delegate.getType(typeName);
  }

  public boolean dropType(String typeName) {
    return delegate.dropType(typeName);
  }

  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    delegate.createTable(tbl);
  }

  public boolean dropTable(String catalogName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    return delegate.dropTable(catalogName, dbName, tableName);
  }

  public Table getTable(String catalogName, String dbName, String tableName) throws MetaException {
    return delegate.getTable(catalogName, dbName, tableName);
  }

  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    return delegate.addPartition(part);
  }

  public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    return delegate.addPartitions(catName, dbName, tblName, parts);
  }

  public boolean addPartitions(String catName, String dbName, String tblName, PartitionSpecProxy partitionSpec,
      boolean ifNotExists) throws InvalidObjectException, MetaException {
    return delegate.addPartitions(catName, dbName, tblName, partitionSpec, ifNotExists);
  }

  public Partition getPartition(String catName, String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException {
    return delegate.getPartition(catName, dbName, tableName, part_vals);
  }

  public boolean doesPartitionExist(String catName, String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException {
    return delegate.doesPartitionExist(catName, dbName, tableName, part_vals);
  }

  public boolean dropPartition(String catName, String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    return delegate.dropPartition(catName, dbName, tableName, part_vals);
  }

  public List<Partition> getPartitions(String catName, String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    return delegate.getPartitions(catName, dbName, tableName, max);
  }

  public void alterTable(String catName, String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {
    delegate.alterTable(catName, dbname, name, newTable);
  }

  public void updateCreationMetadata(String catName, String dbname, String tablename, CreationMetadata cm)
      throws MetaException {
    delegate.updateCreationMetadata(catName, dbname, tablename, cm);
  }

  public List<String> getTables(String catName, String dbName, String pattern) throws MetaException {
    return delegate.getTables(catName, dbName, pattern);
  }

  public List<String> getTables(String catName, String dbName, String pattern, TableType tableType)
      throws MetaException {
    return delegate.getTables(catName, dbName, pattern, tableType);
  }

  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    return delegate.getMaterializedViewsForRewriting(catName, dbName);
  }

  public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {
    return delegate.getTableMeta(catName, dbNames, tableNames, tableTypes);
  }

  public List<Table> getTableObjectsByName(String catName, String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException {
    return delegate.getTableObjectsByName(catName, dbname, tableNames);
  }

  public List<String> getAllTables(String catName, String dbName) throws MetaException {
    return delegate.getAllTables(catName, dbName);
  }

  public List<String> listTableNamesByFilter(String catName, String dbName, String filter, short max_tables)
      throws MetaException, UnknownDBException {
    return delegate.listTableNamesByFilter(catName, dbName, filter, max_tables);
  }

  public List<String> listPartitionNames(String catName, String db_name, String tbl_name, short max_parts)
      throws MetaException {
    return delegate.listPartitionNames(catName, db_name, tbl_name, max_parts);
  }

  public PartitionValuesResponse listPartitionValues(String catName, String db_name, String tbl_name,
      List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending, List<FieldSchema> order,
      long maxParts) throws MetaException {
    return delegate.listPartitionValues(catName, db_name, tbl_name, cols, applyDistinct, filter, ascending, order,
        maxParts);
  }

  public void alterPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      Partition new_part) throws InvalidObjectException, MetaException {
    delegate.alterPartition(catName, db_name, tbl_name, part_vals, new_part);
  }

  public void alterPartitions(String catName, String db_name, String tbl_name, List<List<String>> part_vals_list,
      List<Partition> new_parts) throws InvalidObjectException, MetaException {
    delegate.alterPartitions(catName, db_name, tbl_name, part_vals_list, new_parts);
  }

  public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName, String filter,
      short maxParts) throws MetaException, NoSuchObjectException {
    return delegate.getPartitionsByFilter(catName, dbName, tblName, filter, maxParts);
  }

  public boolean getPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    return delegate.getPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName, maxParts, result);
  }

  public int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException {
    return delegate.getNumPartitionsByFilter(catName, dbName, tblName, filter);
  }

  public int getNumPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    return delegate.getNumPartitionsByExpr(catName, dbName, tblName, expr);
  }

  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    return delegate.getPartitionsByNames(catName, dbName, tblName, partNames);
  }

  public Table markPartitionForEvent(String catName, String dbName, String tblName, Map<String, String> partVals,
      PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    return delegate.markPartitionForEvent(catName, dbName, tblName, partVals, evtType);
  }

  public boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName, Map<String, String> partName,
      PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    return delegate.isPartitionMarkedForEvent(catName, dbName, tblName, partName, evtType);
  }

  public boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return delegate.addRole(rowName, ownerName);
  }

  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    return delegate.removeRole(roleName);
  }

  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return delegate.grantRole(role, userName, principalType, grantor, grantorType, grantOption);
  }

  public boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException {
    return delegate.revokeRole(role, userName, principalType, grantOption);
  }

  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return delegate.getUserPrivilegeSet(userName, groupNames);
  }

  public PrincipalPrivilegeSet getDBPrivilegeSet(String catName, String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return delegate.getDBPrivilegeSet(catName, dbName, userName, groupNames);
  }

  public PrincipalPrivilegeSet getTablePrivilegeSet(String catName, String dbName, String tableName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return delegate.getTablePrivilegeSet(catName, dbName, tableName, userName, groupNames);
  }

  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String catName, String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
    return delegate.getPartitionPrivilegeSet(catName, dbName, tableName, partition, userName, groupNames);
  }

  public PrincipalPrivilegeSet getColumnPrivilegeSet(String catName, String dbName, String tableName,
      String partitionName, String columnName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return delegate.getColumnPrivilegeSet(catName, dbName, tableName, partitionName, columnName, userName, groupNames);
  }

  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName, PrincipalType principalType) {
    return delegate.listPrincipalGlobalGrants(principalName, principalType);
  }

  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName, PrincipalType principalType,
      String catName, String dbName) {
    return delegate.listPrincipalDBGrants(principalName, principalType, catName, dbName);
  }

  public List<HiveObjectPrivilege> listAllTableGrants(String principalName, PrincipalType principalType, String catName,
      String dbName, String tableName) {
    return delegate.listAllTableGrants(principalName, principalType, catName, dbName, tableName);
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName, PrincipalType principalType,
      String catName, String dbName, String tableName, List<String> partValues, String partName) {
    return delegate.listPrincipalPartitionGrants(principalName, principalType, catName, dbName, tableName, partValues,
        partName);
  }

  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName, PrincipalType principalType,
      String catName, String dbName, String tableName, String columnName) {
    return delegate.listPrincipalTableColumnGrants(principalName, principalType, catName, dbName, tableName,
        columnName);
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName, PrincipalType principalType,
      String catName, String dbName, String tableName, List<String> partValues, String partName, String columnName) {
    return delegate.listPrincipalPartitionColumnGrants(principalName, principalType, catName, dbName, tableName,
        partValues, partName, columnName);
  }

  public boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return delegate.grantPrivileges(privileges);
  }

  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return delegate.revokePrivileges(privileges, grantOption);
  }

  public boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return delegate.refreshPrivileges(objToRefresh, authorizer, grantPrivileges);
  }

  public Role getRole(String roleName) throws NoSuchObjectException {
    return delegate.getRole(roleName);
  }

  public List<String> listRoleNames() {
    return delegate.listRoleNames();
  }

  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    return delegate.listRoles(principalName, principalType);
  }

  public List<RolePrincipalGrant> listRolesWithGrants(String principalName, PrincipalType principalType) {
    return delegate.listRolesWithGrants(principalName, principalType);
  }

  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    return delegate.listRoleMembers(roleName);
  }

  public Partition getPartitionWithAuth(String catName, String dbName, String tblName, List<String> partVals,
      String user_name, List<String> group_names) throws MetaException, NoSuchObjectException, InvalidObjectException {
    return delegate.getPartitionWithAuth(catName, dbName, tblName, partVals, user_name, group_names);
  }

  public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, NoSuchObjectException, InvalidObjectException {
    return delegate.getPartitionsWithAuth(catName, dbName, tblName, maxParts, userName, groupNames);
  }

  public List<String> listPartitionNamesPs(String catName, String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws MetaException, NoSuchObjectException {
    return delegate.listPartitionNamesPs(catName, db_name, tbl_name, part_vals, max_parts);
  }

  public List<Partition> listPartitionsPsWithAuth(String catName, String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    return delegate.listPartitionsPsWithAuth(catName, db_name, tbl_name, part_vals, max_parts, userName, groupNames);
  }

  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return delegate.updateTableColumnStatistics(colStats);
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return delegate.updatePartitionColumnStatistics(statsObj, partVals);
  }

  public ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colName) throws MetaException, NoSuchObjectException {
    return delegate.getTableColumnStatistics(catName, dbName, tableName, colName);
  }

  public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    return delegate.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames);
  }

  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName, String partName,
      List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return delegate.deletePartitionColumnStatistics(catName, dbName, tableName, partName, partVals, colName);
  }

  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return delegate.deleteTableColumnStatistics(catName, dbName, tableName, colName);
  }

  public long cleanupEvents() {
    return delegate.cleanupEvents();
  }

  public boolean addToken(String tokenIdentifier, String delegationToken) {
    return delegate.addToken(tokenIdentifier, delegationToken);
  }

  public boolean removeToken(String tokenIdentifier) {
    return delegate.removeToken(tokenIdentifier);
  }

  public String getToken(String tokenIdentifier) {
    return delegate.getToken(tokenIdentifier);
  }

  public List<String> getAllTokenIdentifiers() {
    return delegate.getAllTokenIdentifiers();
  }

  public int addMasterKey(String key) throws MetaException {
    return delegate.addMasterKey(key);
  }

  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException {
    delegate.updateMasterKey(seqNo, key);
  }

  public boolean removeMasterKey(Integer keySeq) {
    return delegate.removeMasterKey(keySeq);
  }

  public String[] getMasterKeys() {
    return delegate.getMasterKeys();
  }

  public void verifySchema() throws MetaException {
    delegate.verifySchema();
  }

  public String getMetaStoreSchemaVersion() throws MetaException {
    return delegate.getMetaStoreSchemaVersion();
  }

  public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
    delegate.setMetaStoreSchemaVersion(version, comment);
  }

  public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    delegate.dropPartitions(catName, dbName, tblName, partNames);
  }

  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName, PrincipalType principalType) {
    return delegate.listPrincipalDBGrantsAll(principalName, principalType);
  }

  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName, PrincipalType principalType) {
    return delegate.listPrincipalTableGrantsAll(principalName, principalType);
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName, PrincipalType principalType) {
    return delegate.listPrincipalPartitionGrantsAll(principalName, principalType);
  }

  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    return delegate.listPrincipalTableColumnGrantsAll(principalName, principalType);
  }

  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    return delegate.listPrincipalPartitionColumnGrantsAll(principalName, principalType);
  }

  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    return delegate.listGlobalGrantsAll();
  }

  public List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName) {
    return delegate.listDBGrantsAll(catName, dbName);
  }

  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String catName, String dbName, String tableName,
      String partitionName, String columnName) {
    return delegate.listPartitionColumnGrantsAll(catName, dbName, tableName, partitionName, columnName);
  }

  public List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName, String tableName) {
    return delegate.listTableGrantsAll(catName, dbName, tableName);
  }

  public List<HiveObjectPrivilege> listPartitionGrantsAll(String catName, String dbName, String tableName,
      String partitionName) {
    return delegate.listPartitionGrantsAll(catName, dbName, tableName, partitionName);
  }

  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String catName, String dbName, String tableName,
      String columnName) {
    return delegate.listTableColumnGrantsAll(catName, dbName, tableName, columnName);
  }

  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    delegate.createFunction(func);
  }

  public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
    delegate.alterFunction(catName, dbName, funcName, newFunction);
  }

  public void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    delegate.dropFunction(catName, dbName, funcName);
  }

  public Function getFunction(String catName, String dbName, String funcName) throws MetaException {
    return delegate.getFunction(catName, dbName, funcName);
  }

  public List<Function> getAllFunctions(String catName) throws MetaException {
    return delegate.getAllFunctions(catName);
  }

  public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException {
    return delegate.getFunctions(catName, dbName, pattern);
  }

  public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName, List<String> partNames,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    return delegate.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames);
  }

  public List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    return delegate.getPartitionColStatsForDatabase(catName, dbName);
  }

  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    return delegate.getNextNotification(rqst);
  }

  public void addNotificationEvent(NotificationEvent event) {
    delegate.addNotificationEvent(event);
  }

  public void cleanNotificationEvents(int olderThan) {
    delegate.cleanNotificationEvents(olderThan);
  }

  public CurrentNotificationEventId getCurrentNotificationEventId() {
    return delegate.getCurrentNotificationEventId();
  }

  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    return delegate.getNotificationEventsCount(rqst);
  }

  public void flushCache() {
    delegate.flushCache();
  }

  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    return delegate.getFileMetadata(fileIds);
  }

  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata, FileMetadataExprType type)
      throws MetaException {
    delegate.putFileMetadata(fileIds, metadata, type);
  }

  public boolean isFileMetadataSupported() {
    return delegate.isFileMetadataSupported();
  }

  public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr, ByteBuffer[] metadatas,
      ByteBuffer[] exprResults, boolean[] eliminated) throws MetaException {
    delegate.getFileMetadataByExpr(fileIds, type, expr, metadatas, exprResults, eliminated);
  }

  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    return delegate.getFileMetadataHandler(type);
  }

  public int getTableCount() throws MetaException {
    return delegate.getTableCount();
  }

  public int getPartitionCount() throws MetaException {
    return delegate.getPartitionCount();
  }

  public int getDatabaseCount() throws MetaException {
    return delegate.getDatabaseCount();
  }

  public List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name, String tbl_name) throws MetaException {
    return delegate.getPrimaryKeys(catName, db_name, tbl_name);
  }

  public List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name, String parent_tbl_name,
      String foreign_db_name, String foreign_tbl_name) throws MetaException {
    return delegate.getForeignKeys(catName, parent_db_name, parent_tbl_name, foreign_db_name, foreign_tbl_name);
  }

  public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return delegate.getUniqueConstraints(catName, db_name, tbl_name);
  }

  public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return delegate.getNotNullConstraints(catName, db_name, tbl_name);
  }

  public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return delegate.getDefaultConstraints(catName, db_name, tbl_name);
  }

  public List<SQLCheckConstraint> getCheckConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    return delegate.getCheckConstraints(catName, db_name, tbl_name);
  }

  public List<String> createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints) throws InvalidObjectException, MetaException {
    return delegate.createTableWithConstraints(tbl, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints,
        defaultConstraints, checkConstraints);
  }

  public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
      throws NoSuchObjectException {
    delegate.dropConstraint(catName, dbName, tableName, constraintName);
  }

  public void dropConstraint(String catName, String dbName, String tableName, String constraintName, boolean missingOk)
      throws NoSuchObjectException {
    delegate.dropConstraint(catName, dbName, tableName, constraintName, missingOk);
  }

  public List<String> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException {
    return delegate.addPrimaryKeys(pks);
  }

  public List<String> addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
    return delegate.addForeignKeys(fks);
  }

  public List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks) throws InvalidObjectException, MetaException {
    return delegate.addUniqueConstraints(uks);
  }

  public List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns)
      throws InvalidObjectException, MetaException {
    return delegate.addNotNullConstraints(nns);
  }

  public List<String> addDefaultConstraints(List<SQLDefaultConstraint> dv)
      throws InvalidObjectException, MetaException {
    return delegate.addDefaultConstraints(dv);
  }

  public List<String> addCheckConstraints(List<SQLCheckConstraint> cc) throws InvalidObjectException, MetaException {
    return delegate.addCheckConstraints(cc);
  }

  public String getMetastoreDbUuid() throws MetaException {
    return delegate.getMetastoreDbUuid();
  }

  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException {
    delegate.createResourcePlan(resourcePlan, copyFrom, defaultPoolSize);
  }

  public WMFullResourcePlan getResourcePlan(String name) throws NoSuchObjectException, MetaException {
    return delegate.getResourcePlan(name);
  }

  public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
    return delegate.getAllResourcePlans();
  }

  public WMFullResourcePlan alterResourcePlan(String name, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    return delegate.alterResourcePlan(name, resourcePlan, canActivateDisabled, canDeactivate, isReplace);
  }

  public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
    return delegate.getActiveResourcePlan();
  }

  public WMValidateResourcePlanResponse validateResourcePlan(String name)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    return delegate.validateResourcePlan(name);
  }

  public void dropResourcePlan(String name) throws NoSuchObjectException, MetaException {
    delegate.dropResourcePlan(name);
  }

  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.createWMTrigger(trigger);
  }

  public void alterWMTrigger(WMTrigger trigger) throws NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.alterWMTrigger(trigger);
  }

  public void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.dropWMTrigger(resourcePlanName, triggerName);
  }

  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException {
    return delegate.getTriggersForResourcePlan(resourcePlanName);
  }

  public void createPool(WMPool pool)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.createPool(pool);
  }

  public void alterPool(WMNullablePool pool, String poolPath)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.alterPool(pool, poolPath);
  }

  public void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.dropWMPool(resourcePlanName, poolPath);
  }

  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.createOrUpdateWMMapping(mapping, update);
  }

  public void dropWMMapping(WMMapping mapping) throws NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.dropWMMapping(mapping);
  }

  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.createWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }

  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    delegate.dropWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }

  public void createISchema(ISchema schema) throws AlreadyExistsException, MetaException, NoSuchObjectException {
    delegate.createISchema(schema);
  }

  public void alterISchema(ISchemaName schemaName, ISchema newSchema) throws NoSuchObjectException, MetaException {
    delegate.alterISchema(schemaName, newSchema);
  }

  public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    return delegate.getISchema(schemaName);
  }

  public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {
    delegate.dropISchema(schemaName);
  }

  public void addSchemaVersion(SchemaVersion schemaVersion)
      throws AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {
    delegate.addSchemaVersion(schemaVersion);
  }

  public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion)
      throws NoSuchObjectException, MetaException {
    delegate.alterSchemaVersion(version, newVersion);
  }

  public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    return delegate.getSchemaVersion(version);
  }

  public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    return delegate.getLatestSchemaVersion(schemaName);
  }

  public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    return delegate.getAllSchemaVersion(schemaName);
  }

  public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace, String type)
      throws MetaException {
    return delegate.getSchemaVersionsByColumns(colName, colNamespace, type);
  }

  public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException, MetaException {
    delegate.dropSchemaVersion(version);
  }

  public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
    return delegate.getSerDeInfo(serDeName);
  }

  public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {
    delegate.addSerde(serde);
  }

  public void addRuntimeStat(RuntimeStat stat) throws MetaException {
    delegate.addRuntimeStat(stat);
  }

  public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
    return delegate.getRuntimeStats(maxEntries, maxCreateTime);
  }

  public int deleteRuntimeStats(int maxRetainSecs) throws MetaException {
    return delegate.deleteRuntimeStats(maxRetainSecs);
  }

  public List<FullTableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
    return delegate.getTableNamesWithStats();
  }

  public List<FullTableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
    return delegate.getAllTableNamesForStats();
  }

  public Map<String, List<String>> getPartitionColsWithStats(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    return delegate.getPartitionColsWithStats(catName, dbName, tableName);
  }

}
