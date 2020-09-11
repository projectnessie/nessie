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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
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
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
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
import org.apache.hadoop.hive.metastore.api.Type;
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
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.FullTableName;

public abstract class NotSupportedRawStore implements RawStore {


  @Override
  public Table markPartitionForEvent(String catName, String dbName, String tblName, Map<String, String> partVals,
      PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    throw t();
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName, Map<String, String> partName,
      PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    throw t();
  }

  @Override
  public boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    //throw t();
    return true;
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    throw t();
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String catName, String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String catName, String dbName, String tableName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String catName, String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String catName, String dbName, String tableName,
      String partitionName, String columnName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName, PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName, PrincipalType principalType,
      String catName, String dbName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName, PrincipalType principalType, String catName,
      String dbName, String tableName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName, PrincipalType principalType,
      String catName, String dbName, String tableName, List<String> partValues, String partName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName, PrincipalType principalType,
      String catName, String dbName, String tableName, String columnName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName, PrincipalType principalType,
      String catName, String dbName, String tableName, List<String> partValues, String partName, String columnName) {
    throw t();
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return true; //throw t();
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return false;
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    return null;
  }

  @Override
  public List<String> listRoleNames() {
    throw t();
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName, PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    throw t();
  }

  @Override
  public Partition getPartitionWithAuth(String catName, String dbName, String tblName, List<String> partVals,
      String user_name, List<String> group_names) throws MetaException, NoSuchObjectException, InvalidObjectException {
    throw t();
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, NoSuchObjectException, InvalidObjectException {
    throw t();
  }

  @Override
  public List<String> listPartitionNamesPs(String catName, String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String catName, String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    throw t();
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return false;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return false;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colName) throws MetaException, NoSuchObjectException {
    return new ColumnStatistics();
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName, String partName,
      List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return false;
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return false;
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException {
  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName) throws MetaException {
    return null;
  }

  @Override
  public List<Function> getAllFunctions(String catName) throws MetaException {
    return null;
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException {
    return null;
  }

  @Override
  public long cleanupEvents() {
    return 0;
  }


  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) {
    throw t();
  }

  @Override
  public boolean removeToken(String tokenIdentifier) {
    throw t();
  }

  @Override
  public String getToken(String tokenIdentifier) {
    throw t();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    throw t();
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    throw t();
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    throw t();
  }

  @Override
  public String[] getMasterKeys() {
    throw t();
  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    throw t();
  }

  @Override
  public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
    throw t();
  }

  @Override
  public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName, PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName, PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName, PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String catName, String dbName, String tableName,
      String partitionName, String columnName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName, String tableName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String catName, String dbName, String tableName,
      String partitionName) {
    throw t();
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String catName, String dbName, String tableName,
      String columnName) {
    throw t();
  }

  @Override
  public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName, List<String> partNames,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    NotificationEventResponse resp = new NotificationEventResponse();
    resp.setEvents(Collections.emptyList());
    return resp;
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) {
    throw t();
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    CurrentNotificationEventId id = new CurrentNotificationEventId();
    id.setEventId(0);
    return id;
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    throw t();
  }

  @Override
  public void flushCache() {
  }

  @Override
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    throw t();
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata, FileMetadataExprType type)
      throws MetaException {
    throw t();
  }

  @Override
  public boolean isFileMetadataSupported() {
    return false;
  }

  @Override
  public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr, ByteBuffer[] metadatas,
      ByteBuffer[] exprResults, boolean[] eliminated) throws MetaException {
    throw t();
  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    throw t();
  }

  @Override
  public int getTableCount() throws MetaException {
    return -1;
  }

  @Override
  public int getPartitionCount() throws MetaException {
    throw t();
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    throw t();
  }

  @Override
  public List<String> createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName, String constraintName, boolean missingOk)
      throws NoSuchObjectException {
    throw t();
  }

  @Override
  public List<String> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public List<String> addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns)
      throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public List<String> addDefaultConstraints(List<SQLDefaultConstraint> dv)
      throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public List<String> addCheckConstraints(List<SQLCheckConstraint> cc) throws InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException {
    return "123";
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException {
    throw t();
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
    throw t();
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String name, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
    throw t();
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String name)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    throw t();
  }

  @Override
  public void dropResourcePlan(String name) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger) throws NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public void createPool(WMPool pool)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void alterPool(WMNullablePool pool, String poolPath)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void dropWMMapping(WMMapping mapping) throws NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    throw t();
  }

  @Override
  public void createISchema(ISchema schema) throws AlreadyExistsException, MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public void alterISchema(ISchemaName schemaName, ISchema newSchema) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    throw t();
  }

  @Override
  public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion)
      throws AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion)
      throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    throw t();
  }

  @Override
  public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    throw t();
  }

  @Override
  public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    throw t();
  }

  @Override
  public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace, String type)
      throws MetaException {
    throw t();
  }

  @Override
  public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {
    throw t();
  }



  @Override
  public List<FullTableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public List<FullTableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public Map<String, List<String>> getPartitionColsWithStats(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public void alterCatalog(String catName, Catalog cat) throws MetaException, InvalidOperationException {
    throw t();
  }

  @Override
  public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    throw t();
  }

  @Override
  public void updateCreationMetadata(String catName, String dbname, String tablename, CreationMetadata cm)
      throws MetaException {
    throw t();
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    throw t();
  }

  @Override
  public void createCatalog(Catalog cat) throws MetaException {
    throw t();
  }

  @Override
  public boolean createType(Type type) {
    throw t();
  }

  @Override
  public Type getType(String typeName) {
    throw t();
  }

  @Override
  public boolean dropType(String typeName) {
    throw t();
  }


  private static final RuntimeException t() {
    StackTraceElement ele = new Throwable().getStackTrace()[1];
    IllegalArgumentException ex = new IllegalArgumentException(String.format("Unsupport operation %s. Located at line %d.", ele.getMethodName(), ele.getLineNumber()));
    return ex;
  }

}
