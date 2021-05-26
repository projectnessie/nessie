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

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.projectnessie.hms.annotation.CatalogExtend;
import org.projectnessie.hms.annotation.Route;
import org.projectnessie.hms.annotation.Union;

@SuppressWarnings({
  "checkstyle:*",
  "checkstyle:LineLength",
  "checkstyle:OverloadMethodsDeclarationOrder",
  "checkstyle:ParameterName"
})
public interface NessieStore extends Configurable, TransactionHandler {

  static final String NESSIE_WHITELIST_DBS_OPTION = "nessie.dbs";

  @CatalogExtend
  List<Partition> getPartitions(@Route String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  void alterPartitions(
      @Route String db_name,
      String tbl_name,
      List<List<String>> part_vals_list,
      List<Partition> new_parts)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  List<Partition> listPartitionsPsWithAuth(
      @Route String db_name,
      String tbl_name,
      List<String> part_vals,
      short max_parts,
      String userName,
      List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException;

  @CatalogExtend
  void alterPartition(
      @Route String db_name, String tbl_name, List<String> part_vals, Partition new_part)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  boolean getPartitionsByExpr(
      @Route String dbName,
      String tblName,
      byte[] expr,
      String defaultPartitionName,
      short maxParts,
      List<Partition> result)
      throws org.apache.thrift.TException;

  @CatalogExtend
  List<String> getAllTables(@Route String dbName) throws MetaException;

  @CatalogExtend
  boolean dropTable(@Route String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  @CatalogExtend
  boolean addPartitions(@Route String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  boolean dropDatabase(@Route String dbname) throws NoSuchObjectException, MetaException;

  @CatalogExtend
  boolean alterDatabase(@Route String dbname, Database db)
      throws NoSuchObjectException, MetaException;

  @CatalogExtend
  Table getTable(@Route String dbName, String tableName) throws MetaException;

  @CatalogExtend
  boolean addPartitions(
      @Route String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy partitionSpec,
      boolean ifNotExists)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  List<Partition> getPartitionsWithAuth(
      @Route String dbName,
      String tblName,
      short maxParts,
      String userName,
      List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  void createTable(@Route Table tbl) throws InvalidObjectException, MetaException;

  void createDatabase(@Route Database db) throws InvalidObjectException, MetaException;

  boolean addPartition(@Route Partition part) throws InvalidObjectException, MetaException;

  @Union
  int getDatabaseCount() throws MetaException;

  @Union
  int getTableCount() throws MetaException;

  @Union
  int getPartitionCount() throws MetaException;

  String getMetaStoreSchemaVersion() throws MetaException;

  @CatalogExtend
  boolean dropPartition(@Route String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  @CatalogExtend
  List<TableMeta> getTableMeta(@Route String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException;

  @CatalogExtend
  @Union
  List<String> getAllDatabases() throws MetaException;

  @CatalogExtend
  List<String> listTableNamesByFilter(@Route String dbName, String filter, short max_tables)
      throws MetaException, UnknownDBException;

  @CatalogExtend
  void alterTable(@Route String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  @CatalogExtend
  List<Table> getTableObjectsByName(@Route String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException;

  @CatalogExtend
  List<Partition> getPartitionsByNames(@Route String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  boolean doesPartitionExist(@Route String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  List<Partition> getPartitionsByFilter(
      @Route String dbName, String tblName, String filter, short maxParts)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  List<String> getTables(@Route String dbName, String pattern) throws MetaException;

  @CatalogExtend
  Partition getPartitionWithAuth(
      @Route String dbName,
      String tblName,
      List<String> partVals,
      String user_name,
      List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  @CatalogExtend
  Database getDatabase(@Route String name) throws NoSuchObjectException;

  @Union
  @CatalogExtend
  List<String> getDatabases(String pattern) throws MetaException;

  @CatalogExtend
  void dropPartitions(@Route String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  Partition getPartition(@Route String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  List<String> listPartitionNames(@Route String db_name, String tbl_name, short max_parts)
      throws MetaException;

  @CatalogExtend
  int getNumPartitionsByFilter(@Route String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  List<String> getTables(
      @Route String dbName, String pattern, org.apache.hadoop.hive.metastore.TableType tableType)
      throws MetaException;

  @CatalogExtend
  PartitionValuesResponse listPartitionValues(
      @Route String db_name,
      String tbl_name,
      List<FieldSchema> cols,
      boolean applyDistinct,
      String filter,
      boolean ascending,
      List<FieldSchema> order,
      long maxParts)
      throws MetaException;

  @CatalogExtend
  int getNumPartitionsByExpr(@Route String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException;

  @CatalogExtend
  List<String> listPartitionNamesByFilter(
      @Route String db_name, String tbl_name, String filter, short max_parts) throws MetaException;

  @CatalogExtend
  Map<String, List<String>> getPartitionColsWithStats(@Route String dbName, String tableName)
      throws MetaException, NoSuchObjectException;
}
