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

package com.dremio.iceberg.client;

import com.dremio.iceberg.client.tag.Tag;
import com.dremio.iceberg.client.tag.TagCatalog;
import com.dremio.iceberg.model.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg Alley implementation of Iceberg Catalog.
 */
public class AlleyCatalog extends BaseMetastoreCatalog implements TagCatalog, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AlleyCatalog.class);
  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final AlleyClient client;
  private final String warehouseLocation;
  private final Configuration config;
  private final String tag;

  public AlleyCatalog(Configuration config) {
    this.config = config;
    client = new AlleyClient(config);
    warehouseLocation = config.get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
    tag = config.get("iceberg.alley.view-tag", client.getConfig().getDefaultTag());
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  //@Override
  protected String name() {
    return "alley";
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    Table table = client.getTableClient()
                        .getObjectByName(tableIdentifier.name(),
                                         tableIdentifier.namespace().toString());
    if (table == null) {
      table = tableFromTableIdentifier(tableIdentifier);
    }
    return new AlleyTableOperations(config, table, loadTag(tag), client);
  }

  private Table tableFromTableIdentifier(TableIdentifier tableIdentifier) {
    Table.TableBuilder builder = Table.builder()
                                      .tableName(tableIdentifier.name())
                                      .baseLocation(warehouseLocation);
    if (tableIdentifier.hasNamespace()) {
      return builder.namespace(tableIdentifier.namespace().toString()).build();
    }
    return builder.build();
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    Table table = client.getTableClient()
                        .getObjectByName(tableIdentifier.name(),
                                         tableIdentifier.namespace().toString());
    if (table == null) {
      table = tableFromTableIdentifier(tableIdentifier);
    }
    String namespace = (table.getNamespace() == null || table.getNamespace().equals("")) ? ""
        : (Joiner.on('/').join(table.getNamespace().split("\\.")) + "/");
    return table.getBaseLocation() + "/" + namespace + table.getTableName();
  }

  //@Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Table[] tables =
      client.getTableClient().getAll(namespace == null ? null : namespace.toString());
    return Arrays.stream(tables).map(t -> {
      Namespace n = t.getNamespace() == null ? Namespace.empty() : Namespace.of(t.getNamespace());
      return TableIdentifier.of(n, t.getTableName());
    }).collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    Table existingTable = client.getTableClient()
                                .getObjectByName(identifier.name(),
                                                 identifier.namespace().toString());
    if (existingTable == null) {
      return false;
    }
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    client.getTableClient().deleteObject(existingTable.getId(), purge);
    if (purge && lastMetadata != null) {
      dropTableData(ops.io(), lastMetadata);
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Table existingFromTable = client.getTableClient()
                                    .getObjectByName(from.name(), from.namespace().toString());
    if (existingFromTable == null) {
      throw new NoSuchTableException("table {} doesn't exists", from.name());
    }
    if (existingFromTable.isDeleted()) {
      throw new NoSuchTableException("table {} doesn't exists", existingFromTable.getTableName());
    }
    Table existingToTable = client.getTableClient()
                                  .getObjectByName(to.name(), to.namespace().toString());
    if (existingToTable != null && !existingToTable.isDeleted()) {
      throw new AlreadyExistsException("table {} already exists", to.name());
    }
    Table updatedTable = Table.copyTable(existingFromTable)
                              .tableName(to.name())
                              .namespace(to.namespace().toString())
                              .build();
    try {
      client.getTableClient().updateObject(updatedTable);
    } catch (Throwable t) {
      throw new CommitFailedException(t, "failed");
    }
  }

  @Override
  public Tag createTag(String tagName, String parentName) {
    com.dremio.iceberg.model.Tag newTag = com.dremio.iceberg.model.Tag.builder()
                                                                      .name(tagName)
                                                                      .baseTag(parentName)
                                                                      .build();
    newTag = client.getTagClient().createObject(newTag);
    return new AlleyTag(newTag, client);
  }

  @Override
  public boolean dropTag(String tagName, boolean purge) {
    com.dremio.iceberg.model.Tag currentTag = client.getTagClient().getObjectByName(tagName, null);
    if (currentTag == null) {
      return false;
    }
    client.getTagClient().deleteObject(currentTag.getId(), purge);
    return true;
  }

  @Override
  public void renameTag(String from, String to) {
    com.dremio.iceberg.model.Tag existingFromTable = client.getTagClient()
                                                           .getObjectByName(from, null);
    if (existingFromTable == null) {
      throw new NoSuchTableException("tag {} doesn't exists", from);
    }
    if (existingFromTable.isDeleted()) {
      throw new NoSuchTableException("tag {} doesn't exists", existingFromTable.getName());
    }
    com.dremio.iceberg.model.Tag existingToTable = client.getTagClient().getObjectByName(to, null);
    if (existingToTable != null && !existingToTable.isDeleted()) {
      throw new AlreadyExistsException("tag {} already exists", to);
    }
    com.dremio.iceberg.model.Tag updatedTable = com.dremio.iceberg.model.Tag.copyOf(
        existingFromTable).name(to).build();
    try {
      client.getTagClient().updateObject(updatedTable);
    } catch (Throwable t) {
      throw new CommitFailedException(t, "failed");
    }
  }

  @Override
  public Tag loadTag(String tagName) {
    com.dremio.iceberg.model.Tag currentTag = client.getTagClient().getObjectByName(tagName, null);
    return new AlleyTag(currentTag, client);
  }

  @VisibleForTesting
  public AlleyClient getClient() {
    return client;
  }
}
