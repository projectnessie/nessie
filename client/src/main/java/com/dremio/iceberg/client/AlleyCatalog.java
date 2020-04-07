/*
 * Copyright (C) 2020 Dremio
 *
 *             Licensed under the Apache License, Version 2.0 (the "License");
 *             you may not use this file except in compliance with the License.
 *             You may obtain a copy of the License at
 *
 *             http://www.apache.org/licenses/LICENSE-2.0
 *
 *             Unless required by applicable law or agreed to in writing, software
 *             distributed under the License is distributed on an "AS IS" BASIS,
 *             WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *             See the License for the specific language governing permissions and
 *             limitations under the License.
 */
package com.dremio.iceberg.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.iceberg.model.Configuration;
import com.dremio.iceberg.model.Table;

public class AlleyCatalog extends BaseMetastoreCatalog implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AlleyCatalog.class);
  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private static final String TABLE_METADATA_FILE_EXTENSION = ".metadata.json";
  private static final PathFilter TABLE_FILTER = path -> path.getName().endsWith(TABLE_METADATA_FILE_EXTENSION);
  private final AlleyClient client;
  private final String warehouseLocation;
  private Configuration config;

  public AlleyCatalog(Configuration config) {
    this.config = config;
    client = new AlleyClient(config.getConfiguration());
    warehouseLocation = config.getConfiguration().get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  protected String name() {
    return "alley";
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    Table table = tableFromTableIdentifier(tableIdentifier);
    table = client.getTable(table.getTableName());
    if (table == null) {
      table = tableFromTableIdentifier(tableIdentifier);
    }
    //todo checks for safety!
    return new AlleyTableOperations(config.getConfiguration(), table);
  }

  private Table tableFromTableIdentifier(TableIdentifier tableIdentifier) {
    return new Table(tableIdentifier.toString(), warehouseLocation);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    Table table = tableFromTableIdentifier(tableIdentifier);
    table = client.getTable(table.getTableName());
    if (table == null) {
      table = tableFromTableIdentifier(tableIdentifier);
    }
    // todo safety checks
    return table.getBaseLocation() + "/" + table.getTableName();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    List<Table> tables = client.getTables();
    // todo safety checks
    return tables.stream().map(t -> TableIdentifier.parse(t.getTableName())).collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    Table table = tableFromTableIdentifier(identifier);
    Table existingTable = client.getTable(table.getTableName());
    if (existingTable == null) {
      return false;
    }
    if (purge) {
      client.deleteTable(existingTable.getTableName());
    } else {
      existingTable.setDeleted(true);
      client.updateTable(existingTable);
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Table table = tableFromTableIdentifier(from);
    Table existingFromTable = client.getTable(table.getTableName());
    if (existingFromTable == null) {
      throw new NoSuchTableException("table {} doesn't exists", table.getTableName());
    }
    if (existingFromTable.isDeleted()) {
      throw new NoSuchTableException("table {} doesn't exists", table.getTableName());
    }
    table = tableFromTableIdentifier(to);
    Table existingToTable = client.getTable(table.getTableName());
    if (existingToTable != null && !existingToTable.isDeleted()) {
      throw new AlreadyExistsException("table {} already exists", table.getTableName());
    }
    client.deleteTable(existingFromTable.getTableName());
    client.createTable(existingFromTable.rename(table.getTableName()));
  }
}
