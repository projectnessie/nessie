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
package org.projectnessie.hms.apis;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.projectnessie.hms.annotation.CatalogExtend;
import org.projectnessie.hms.annotation.NoopQuiet;
import org.projectnessie.hms.annotation.NoopThrow;
import org.projectnessie.hms.annotation.Route;

@SuppressWarnings({"checkstyle:*", "checkstyle:LineLength", "checkstyle:OverloadMethodsDeclarationOrder", "checkstyle:ParameterName"})
public interface AnnotatedHive3RawStore extends BaseRawStoreUnion {

  @NoopThrow
  List<String> addForeignKeys(@Route List<SQLForeignKey> fks) throws InvalidObjectException, MetaException;

  @NoopThrow
  List<String> addPrimaryKeys(@Route List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException;

  @NoopThrow
  void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException;

  @NoopThrow
  void createWMTrigger(WMTrigger trigger) throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  WMFullResourcePlan getResourcePlan(String name) throws NoSuchObjectException, MetaException;

  @NoopThrow
  void addSchemaVersion(SchemaVersion schemaVersion) throws AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException;

  @NoopThrow
  void alterISchema(ISchemaName schemaName, ISchema newSchema) throws NoSuchObjectException, MetaException;

  @NoopThrow
  void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException, MetaException;

  @NoopQuiet
  Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException;

  @NoopThrow
  void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion) throws NoSuchObjectException, MetaException;

  @NoopThrow
  void createOrUpdateWMMapping(WMMapping mapping, boolean update) throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  void dropWMMapping(WMMapping mapping) throws NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize) throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException;

  @NoopThrow
  void createISchema(ISchema schema) throws AlreadyExistsException, MetaException, NoSuchObjectException;

  @NoopThrow
  List<String> addUniqueConstraints(@Route List<SQLUniqueConstraint> uks) throws InvalidObjectException, MetaException;

  @NoopThrow
  List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException;

  @CatalogExtend
  @NoopQuiet
  List<SQLUniqueConstraint> getUniqueConstraints(@Route String db_name, String tbl_name) throws MetaException;

  @NoopThrow
  WMValidateResourcePlanResponse validateResourcePlan(String name) throws NoSuchObjectException, InvalidObjectException, MetaException;

  @NoopThrow
  SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException;

  @CatalogExtend
  @NoopThrow
  void updateCreationMetadata(String dbname, String tablename, CreationMetadata cm) throws MetaException;

  @NoopThrow
  List<String> addCheckConstraints(@Route List<SQLCheckConstraint> cc) throws InvalidObjectException, MetaException;

  @NoopThrow
  List<String> addNotNullConstraints(@Route List<SQLNotNullConstraint> nns) throws InvalidObjectException, MetaException;

  @CatalogExtend
  @NoopQuiet
  List<MetaStoreUtils.ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(@Route String dbName) throws MetaException, NoSuchObjectException;

  @CatalogExtend
  @NoopQuiet
  List<SQLDefaultConstraint> getDefaultConstraints(@Route String db_name, String tbl_name) throws MetaException;

  @NoopThrow
  List<String> addDefaultConstraints(@Route List<SQLDefaultConstraint> dv) throws InvalidObjectException, MetaException;

  @CatalogExtend
  @NoopQuiet
  List<SQLCheckConstraint> getCheckConstraints(@Route String db_name, String tbl_name) throws MetaException;

  @NoopThrow
  void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  void alterCatalog(String catName, Catalog cat) throws MetaException, InvalidOperationException;

  @NoopThrow
  SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException;

  @NoopThrow
  List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace, String type) throws MetaException;

  @NoopQuiet
  List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException;

  @NoopThrow
  ISchema getISchema(ISchemaName schemaName) throws MetaException;

  @NoopQuiet
  @CatalogExtend
  List<String> getMaterializedViewsForRewriting(@Route String dbName) throws MetaException, NoSuchObjectException;

  @NoopQuiet
  List<WMResourcePlan> getAllResourcePlans() throws MetaException;

  @NoopThrow
  WMFullResourcePlan alterResourcePlan(String name, WMNullableResourcePlan resourcePlan, boolean canActivateDisabled, boolean canDeactivate, boolean isReplace) throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  WMFullResourcePlan getActiveResourcePlan() throws MetaException;

  @NoopThrow
  void addRuntimeStat(RuntimeStat stat) throws MetaException;

  @CatalogExtend
  @NoopQuiet
  List<SQLNotNullConstraint> getNotNullConstraints(@Route String db_name, String tbl_name) throws MetaException;

  @NoopThrow
  void alterWMTrigger(WMTrigger trigger) throws NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath) throws NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopQuiet
  void createCatalog(Catalog cat) throws MetaException;

  @NoopThrow
  void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException;

  @NoopThrow
  List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException;

  @NoopThrow
  void dropWMPool(String resourcePlanName, String poolPath) throws NoSuchObjectException, InvalidOperationException, MetaException;

  @NoopThrow
  List<MetaStoreUtils.FullTableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException;

  @NoopThrow
  List<MetaStoreUtils.FullTableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException;

  @NoopThrow
  List<String> createTableWithConstraints(@Route Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints) throws InvalidObjectException, MetaException;

  @NoopQuiet
  NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst);


}
