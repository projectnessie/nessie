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
package com.dremio.nessie.hms.apis;

import com.dremio.nessie.hms.NessieStore;
import com.dremio.nessie.hms.annotation.CatalogExtend;
import com.dremio.nessie.hms.annotation.NoopQuiet;
import com.dremio.nessie.hms.annotation.NoopQuiet.QuietMode;
import com.dremio.nessie.hms.annotation.NoopThrow;
import com.dremio.nessie.hms.annotation.Route;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;

/**
 * Defines the vast majority of Hive metastore apis that exist, prescribing the handling of each via
 * annotations.
 */
@SuppressWarnings({
  "checkstyle:*",
  "checkstyle:LineLength",
  "checkstyle:OverloadMethodsDeclarationOrder",
  "checkstyle:ParameterName"
})
public interface BaseRawStoreUnion extends NessieStore {

  @NoopThrow
  boolean dropType(String typeName);

  @NoopQuiet
  List<String> listRoleNames();

  @NoopQuiet
  boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  @NoopThrow
  Role getRole(String roleName) throws NoSuchObjectException;

  @NoopQuiet
  String[] getMasterKeys();

  @NoopThrow
  void getFileMetadataByExpr(
      List<Long> fileIds,
      FileMetadataExprType type,
      byte[] expr,
      java.nio.ByteBuffer[] metadatas,
      java.nio.ByteBuffer[] exprResults,
      boolean[] eliminated)
      throws MetaException;

  @NoopThrow
  List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType);

  @NoopThrow
  boolean grantRole(
      Role role,
      String userName,
      PrincipalType principalType,
      String grantor,
      PrincipalType grantorType,
      boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  @NoopThrow
  List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType);

  @NoopThrow
  List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType);

  @NoopThrow
  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType);

  @NoopQuiet
  void verifySchema() throws MetaException;

  @NoopThrow
  List<HiveObjectPrivilege> listPrincipalGlobalGrants(
      String principalName, PrincipalType principalType);

  @NoopThrow
  List<Role> listRoles(String principalName, PrincipalType principalType);

  @NoopThrow
  int addMasterKey(String key) throws MetaException;

  @NoopThrow
  boolean removeRole(String roleName) throws MetaException, NoSuchObjectException;

  @NoopQuiet
  void cleanNotificationEvents(int olderThan);

  @NoopQuiet
  void flushCache();

  @NoopQuiet
  boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  @NoopQuiet
  boolean isFileMetadataSupported();

  @NoopQuiet
  List<HiveObjectPrivilege> listGlobalGrantsAll();

  @NoopQuiet
  NotificationEventResponse getNextNotification(NotificationEventRequest rqst);

  @NoopQuiet
  PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException;

  @NoopThrow
  void createFunction(Function func) throws InvalidObjectException, MetaException;

  @NoopQuiet
  boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  @NoopThrow
  boolean addToken(String tokenIdentifier, String delegationToken);

  @NoopQuiet
  void setMetaStoreSchemaVersion(String version, String comment) throws MetaException;

  @NoopQuiet
  List<String> getAllTokenIdentifiers();

  @NoopQuiet
  List<RolePrincipalGrant> listRoleMembers(String roleName);

  @NoopQuiet
  long cleanupEvents();

  @NoopQuiet
  void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException;

  @NoopThrow
  boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException;

  @NoopThrow
  boolean createType(Type type);

  @NoopThrow
  Type getType(String typeName);

  @NoopThrow
  boolean removeMasterKey(Integer keySeq);

  @NoopQuiet
  void addNotificationEvent(NotificationEvent event);

  @NoopThrow
  java.nio.ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException;

  @NoopThrow
  boolean updatePartitionColumnStatistics(
      @Route(true) ColumnStatistics statsObj, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType);

  @NoopQuiet
  List<RolePrincipalGrant> listRolesWithGrants(String principalName, PrincipalType principalType);

  @NoopQuiet
  boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  @NoopThrow
  boolean removeToken(String tokenIdentifier);

  @NoopThrow
  String getToken(String tokenIdentifier);

  @NoopQuiet
  CurrentNotificationEventId getCurrentNotificationEventId();

  @CatalogExtend
  @NoopQuiet
  List<HiveObjectPrivilege> listTableGrantsAll(@Route String dbName, String tableName);

  @CatalogExtend
  @NoopQuiet
  ColumnStatistics getTableColumnStatistics(
      @Route String dbName, String tableName, List<String> colName)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  @NoopQuiet
  PrincipalPrivilegeSet getDBPrivilegeSet(
      @Route String dbName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  @NoopQuiet
  List<Function> getAllFunctions() throws MetaException;

  @CatalogExtend
  @NoopQuiet
  List<HiveObjectPrivilege> listDBGrantsAll(@Route String dbName);

  @CatalogExtend
  @NoopQuiet
  PrincipalPrivilegeSet getTablePrivilegeSet(
      @Route String dbName, String tableName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  @NoopQuiet
  List<String> listPartitionNamesPs(
      @Route String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  @NoopQuiet
  PrincipalPrivilegeSet getColumnPrivilegeSet(
      @Route String dbName,
      String tableName,
      String partitionName,
      String columnName,
      String userName,
      List<String> groupNames)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  @NoopQuiet(QuietMode.NULL)
  List<String> getFunctions(@Route String dbName, String pattern) throws MetaException;

  @CatalogExtend
  @NoopQuiet
  List<HiveObjectPrivilege> listTableColumnGrantsAll(
      @Route String dbName, String tableName, String columnName);

  @NoopQuiet
  @CatalogExtend
  List<ColumnStatistics> getPartitionColumnStatistics(
      @Route String dbName, String tblName, List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException;

  @NoopThrow
  @CatalogExtend
  void dropFunction(@Route String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  @NoopQuiet
  @CatalogExtend
  List<HiveObjectPrivilege> listPartitionColumnGrantsAll(
      @Route String dbName, String tableName, String partitionName, String columnName);

  @CatalogExtend
  @NoopQuiet(QuietMode.NULL)
  Function getFunction(@Route String dbName, String funcName) throws MetaException;

  @NoopThrow
  @CatalogExtend
  void alterFunction(@Route String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException;

  @NoopThrow
  @CatalogExtend
  AggrStats get_aggr_stats_for(
      @Route String dbName, String tblName, List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException;

  @NoopThrow
  org.apache.hadoop.hive.metastore.FileMetadataHandler getFileMetadataHandler(
      FileMetadataExprType type);

  @NoopThrow
  void putFileMetadata(
      List<Long> fileIds, List<java.nio.ByteBuffer> metadata, FileMetadataExprType type)
      throws MetaException;

  @CatalogExtend
  @NoopQuiet
  List<SQLForeignKey> getForeignKeys(
      @Route String parent_db_name,
      String parent_tbl_name,
      String foreign_db_name,
      String foreign_tbl_name)
      throws MetaException;

  @NoopQuiet
  @CatalogExtend
  List<SQLPrimaryKey> getPrimaryKeys(@Route String db_name, String tbl_name) throws MetaException;

  @NoopQuiet
  @CatalogExtend
  void dropConstraint(@Route String dbName, String tableName, String constraintName)
      throws NoSuchObjectException;

  @NoopThrow
  void alterIndex(@Route String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException;

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalDBGrants(
      String principalName, PrincipalType principalType, String dbName);

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalPartitionGrants(
      String principalName,
      PrincipalType principalType,
      String dbName,
      String tableName,
      List<String> partValues,
      String partName);

  @NoopQuiet
  List<HiveObjectPrivilege> listAllTableGrants(
      String principalName, PrincipalType principalType, String dbName, String tableName);

  @NoopThrow
  boolean dropIndex(@Route String dbName, String origTableName, String indexName)
      throws MetaException;

  @NoopQuiet
  List<Index> getIndexes(@Route String dbName, String origTableName, int max) throws MetaException;

  @NoopQuiet(QuietMode.NULL)
  Index getIndex(@Route String dbName, String origTableName, String indexName) throws MetaException;

  @NoopThrow
  boolean addIndex(@Route Index index) throws InvalidObjectException, MetaException;

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName,
      PrincipalType principalType,
      String dbName,
      String tableName,
      List<String> partValues,
      String partName,
      String columnName);

  @NoopQuiet
  List<String> listIndexNames(@Route String dbName, String origTableName, short max)
      throws MetaException;

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalTableColumnGrants(
      String principalName,
      PrincipalType principalType,
      String dbName,
      String tableName,
      String columnName);

  @CatalogExtend
  @NoopQuiet
  void dropConstraint(String dbName, String tableName, String constraintName, boolean missingOk)
      throws NoSuchObjectException;

  @NoopThrow
  void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException;

  @NoopThrow
  SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException;

  @NoopThrow
  void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  List<String> getCatalogs() throws MetaException;

  String getMetastoreDbUuid() throws MetaException;

  @NoopQuiet
  int deleteRuntimeStats(int maxRetainSecs) throws MetaException;

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalDBGrants(
      String principalName, PrincipalType principalType, String catName, String dbName);

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalTableColumnGrants(
      String principalName,
      PrincipalType principalType,
      String catName,
      String dbName,
      String tableName,
      String columnName);

  @NoopThrow
  void dropResourcePlan(String name) throws NoSuchObjectException, MetaException;

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName,
      PrincipalType principalType,
      String catName,
      String dbName,
      String tableName,
      List<String> partValues,
      String partName,
      String columnName);

  @NoopQuiet
  List<HiveObjectPrivilege> listAllTableGrants(
      String principalName,
      PrincipalType principalType,
      String catName,
      @Route String dbName,
      String tableName);

  @NoopThrow
  void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  @NoopQuiet
  List<HiveObjectPrivilege> listPrincipalPartitionGrants(
      String principalName,
      PrincipalType principalType,
      String catName,
      @Route String dbName,
      String tableName,
      List<String> partValues,
      String partName);

  @NoopThrow
  void putFileMetadata(List<Long> fileIds, List<java.nio.ByteBuffer> metadata) throws MetaException;

  @NoopThrow
  void createTableWithConstraints(
      @Route Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
      throws InvalidObjectException, MetaException;

  @NoopQuiet
  boolean refreshPrivileges(HiveObjectRef objToRefresh, PrivilegeBag grantPrivileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  @NoopQuiet
  boolean refreshPrivileges(
      HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  @NoopQuiet
  @CatalogExtend
  boolean deletePartitionColumnStatistics(
      @Route String dbName,
      String tableName,
      String partName,
      List<String> partVals,
      String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  @NoopQuiet
  @CatalogExtend
  boolean isPartitionMarkedForEvent(
      @Route String dbName,
      String tblName,
      Map<String, String> partName,
      PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
          UnknownPartitionException;

  @NoopQuiet
  @CatalogExtend
  boolean deleteTableColumnStatistics(@Route String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  @NoopQuiet
  @CatalogExtend
  List<HiveObjectPrivilege> listPartitionGrantsAll(
      @Route String dbName, String tableName, String partitionName);

  @NoopQuiet
  @CatalogExtend
  PrincipalPrivilegeSet getPartitionPrivilegeSet(
      @Route String dbName,
      String tableName,
      String partition,
      String userName,
      List<String> groupNames)
      throws InvalidObjectException, MetaException;

  @NoopThrow
  @CatalogExtend
  Table markPartitionForEvent(
      @Route String dbName,
      String tblName,
      Map<String, String> partVals,
      PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
          UnknownPartitionException;
}
