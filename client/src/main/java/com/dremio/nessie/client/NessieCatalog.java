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

package com.dremio.nessie.client;

import com.dremio.nessie.client.branch.Branch;
import com.dremio.nessie.client.branch.BranchCatalog;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableBranchTable;
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

/**
 * Nessie implementation of Iceberg Catalog.
 */
public class NessieCatalog extends BaseMetastoreCatalog implements BranchCatalog, Closeable {

  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final NessieClient client;
  private final String warehouseLocation;
  private final Configuration config;
  private final Branch branch;

  public NessieCatalog(Configuration config) {
    this.config = config;
    client = new NessieClient(config);
    warehouseLocation = config.get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
    branch = client.getBranch(config.get("nessie.view-branch",
                                         client.getConfig().getDefaultTag()));
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  //@Override
  protected String name() {
    return "alley";
  }

  private BranchTable table(TableIdentifier tableIdentifier, boolean defaultTable) {
    BranchTable table = client.getTable(branch.name(),
                                        tableIdentifier.name(),
                                        tableIdentifier.hasNamespace()
                                          ? tableIdentifier.namespace().toString() : null);
    if (table == null && defaultTable) {
      table = tableFromTableIdentifier(tableIdentifier);
    }
    return table;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {

    return new NessieTableOperations(config,
                                     table(tableIdentifier, true),
                                     branch,
                                     client);
  }

  private BranchTable tableFromTableIdentifier(TableIdentifier tableIdentifier) {
    ImmutableBranchTable.Builder builder =
        ImmutableBranchTable.builder()
                          .tableName(tableIdentifier.name())
                          .baseLocation(warehouseLocation)
                          .metadataLocation("");
    if (tableIdentifier.hasNamespace()) {
      return builder.namespace(tableIdentifier.namespace().toString())
                    .id(tableIdentifier.toString())
                    .build();
    }
    return builder.id(tableIdentifier.name()).build();
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    BranchTable table = table(tableIdentifier, true);
    String namespace = (table.getNamespace() == null || table.getNamespace().equals("")) ? ""
        : (Joiner.on('/').join(table.getNamespace().split("\\.")) + "/");
    return table.getBaseLocation() + "/" + namespace + table.getTableName();
  }

  //@Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    BranchTable[] tables = client.getAllTables(branch.name(),
                                               namespace == null ? null : namespace.toString());
    return Arrays.stream(tables).map(t -> {
      Namespace n = t.getNamespace() == null ? Namespace.empty() : Namespace.of(t.getNamespace());
      return TableIdentifier.of(n, t.getTableName());
    }).collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    BranchTable existingTable = table(identifier, false);
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

    client.deleteTable(branch, existingTable.getId(), purge);
    if (purge && lastMetadata != null) {
      dropTableData(ops.io(), lastMetadata);
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    BranchTable existingFromTable = table(from, false);
    if (existingFromTable == null) {
      throw new NoSuchTableException(String.format("table %s doesn't exists", from.name()));
    }
    if (existingFromTable.isDeleted()) {
      throw new NoSuchTableException(String.format("table %s doesn't exists",
                                                   existingFromTable.getTableName()));
    }
    BranchTable existingToTable = table(to, false);
    if (existingToTable != null && !existingToTable.isDeleted()) {
      throw new AlreadyExistsException("table {} already exists", to.name());
    }
    String name = to.name();
    String namespace = to.hasNamespace() ? to.namespace().toString() : null;
    String id = (namespace != null) ? namespace + "." + name : name;
    BranchTable updatedTable = ImmutableBranchTable.builder().from(existingFromTable)
                                                   .tableName(name)
                                                   .namespace(namespace)
                                                   .baseLocation(existingFromTable
                                                                   .getBaseLocation())
                                                   .id(id)
                                                   .build();
    try {
      client.commit(branch, updatedTable);
    } catch (Exception e) {
      throw new CommitFailedException(e, "failed");
    }
  }

  @Override
  public Branch createBranch(String branchName, String parentName) {
    com.dremio.nessie.model.Branch newBranch = ImmutableBranch.builder()
                                                              .name(branchName)
                                                              .id(parentName)
                                                              .build();
    return client.createBranch(newBranch);
  }

  @Override
  public boolean dropBranch(String branchName, boolean purge) {
    Branch currentBranch = client.getBranch(branchName);
    if (currentBranch == null) {
      return false;
    }
    client.deleteBranch(currentBranch, purge);
    return true;
  }

  @Override
  public void promoteBranch(String from, String to) {
    Branch existingFromTable = client.getBranch(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException("branch {} doesn't exists", from);
    }
    Branch existingToTable = client.getBranch(to);
    if (existingToTable != null) {
      throw new AlreadyExistsException("branch {} already exists", to);
    }
    com.dremio.nessie.model.Branch updatedTable =
        ImmutableBranch.builder()
                       .from(((NessieBranch) existingFromTable).branch()).name(to).build();
    try {
      client.updateBranch(updatedTable);
    } catch (Throwable t) {
      throw new CommitFailedException(t, "failed");
    }
  }

  @Override
  public Branch loadBranch(String branchName) {
    return client.getBranch(branchName);
  }

  @Override
  public void refreshBranch() {
    branch.refresh();
  }

  @VisibleForTesting
  public NessieClient getClient() {
    return client;
  }
}
