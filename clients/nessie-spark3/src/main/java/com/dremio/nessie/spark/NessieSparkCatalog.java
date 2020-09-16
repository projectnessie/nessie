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
package com.dremio.nessie.spark;

import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.SupportsPathIdentifier;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.dremio.nessie.iceberg.spark.NessieIcebergSparkCatalog;

public class NessieSparkCatalog implements StagingTableCatalog, SupportsNamespaces, SupportsPathIdentifier {

  enum TableType {
    HIVE,
    ICEBERG,
    DELTA
  }

  private final NessieIcebergSparkCatalog iceberg;
  private final DeltaCatalog delta;
  private final String name;

  public NessieSparkCatalog(String name) {
    this.name = name;
    iceberg = new NessieIcebergSparkCatalog();
    delta = new DeltaCatalog(SparkSession.builder().getOrCreate());
  }

  @Override
  public String[][] listNamespaces() {
    return iceberg.listNamespaces(); //todo do we want to delegate to iceberg here? Can we support this for Delta
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return iceberg.listNamespaces(namespace);//todo do we want to delegate to iceberg here? Can we support this for Delta
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return iceberg.namespaceExists(namespace);//todo do we want to delegate to iceberg here? Can we support this for Delta
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    return iceberg.loadNamespaceMetadata(namespace);//todo do we want to delegate to iceberg here? Can we support this for Delta
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    iceberg.createNamespace(namespace, metadata);//todo do we want to delegate to iceberg here? Can we support this for Delta
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    iceberg.alterNamespace(namespace, changes); //todo do we want to delegate to iceberg here? Can we support this for Delta
  }

  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    return iceberg.dropNamespace(namespace); //todo do we want to delegate to iceberg here? Can we support this for Delta
  }

  @Override
  public SessionCatalog catalog() {
    return delta.catalog(); //todo do we want to delegate to delta here? Can we support this for Iceberg
  }

  @Override
  public boolean isPathIdentifier(Identifier ident) {
    return delta.isPathIdentifier(ident); //todo do we want to delegate to delta here? Can we support this for Iceberg
  }

  @Override
  public boolean isPathIdentifier(CatalogTable table) {
    return delta.isPathIdentifier(table); //todo do we want to delegate to delta here? Can we support this for Iceberg
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return delta.tableExists(ident); //todo do we want to delegate to delta here? Can we support this for Iceberg
  }

  private TableType getTableType() {
    return TableType.ICEBERG;
  }

  @Override
  public StagedTable stageCreate(Identifier ident,
                                 StructType schema,
                                 Transform[] partitions,
                                 Map<String, String> properties) throws TableAlreadyExistsException {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.stageCreate(ident, schema, partitions, properties);
      case DELTA:
        return delta.stageCreate(ident, schema, partitions, properties);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] partitions,
                                  Map<String, String> properties) throws NoSuchTableException {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.stageReplace(ident, schema, partitions, properties);
      case DELTA:
        return delta.stageReplace(ident, schema, partitions, properties);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(Identifier ident, StructType schema,
                                          Transform[] partitions, Map<String, String> properties) {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.stageCreateOrReplace(ident, schema, partitions, properties);
      case DELTA:
        return delta.stageCreateOrReplace(ident, schema, partitions, properties);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.listTables(namespace);
      case DELTA:
        return delta.listTables(namespace);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.loadTable(ident);
      case DELTA:
        return delta.loadTable(ident);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                           Map<String, String> properties) throws TableAlreadyExistsException {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.createTable(ident, schema, partitions, properties);
      case DELTA:
        return delta.createTable(ident, schema, partitions, properties);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.alterTable(ident, changes);
      case DELTA:
        return delta.alterTable(ident, changes);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        return iceberg.dropTable(ident);
      case DELTA:
        return delta.dropTable(ident);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        iceberg.renameTable(oldIdent, newIdent);
        break;
      case DELTA:
        delta.renameTable(oldIdent, newIdent);
        break;
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    TableType tableType = getTableType();
    switch (tableType) {
      case ICEBERG:
        iceberg.initialize(name, options);
        break;
      case DELTA:
        delta.initialize(name, options);
        break;
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  @Override
  public String name() {
    return name;
  }
}
