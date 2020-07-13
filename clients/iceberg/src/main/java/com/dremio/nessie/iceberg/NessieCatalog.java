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

package com.dremio.nessie.iceberg;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.iceberg.branch.BranchCatalog;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Table;
import com.google.common.base.Joiner;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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

  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on('.');
  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final NessieClient client;
  private final String warehouseLocation;
  private final Configuration config;
  private AtomicReference<Branch> branch;

  /**
   * create a catalog from a hadoop configuration.
   */
  public NessieCatalog(Configuration config) {
    this.config = config;
    String path = config.get("nessie.url");
    String username = config.get("nessie.username");
    String password = config.get("nessie.password");
    this.client = new NessieClient(path, username, password);
    warehouseLocation = config.get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
    branch = new AtomicReference<>(getOrCreate(config.get("nessie.view-branch",
                                                          client.getConfig().getDefaultBranch())));
  }

  private Branch getOrCreate(String branchName) {
    Branch branch = client.getBranch(branchName);
    if (branch == null) {
      branch = client.createBranch(ImmutableBranch.builder().name(branchName).id("master").build());
    }
    return branch;
  }

  @Override
  public void close() {
    client.close();
  }

  //@Override
  protected String name() {
    return "alley";
  }

  private Table table(TableIdentifier tableIdentifier) {
    Table table = client.getTable(branch.get().getName(),
                                  tableIdentifier.name(),
                                  tableIdentifier.hasNamespace()
                                    ? tableIdentifier.namespace().toString() : null);
    return table;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {

    return new NessieTableOperations(config,
                                     tableIdentifier,
                                     branch,
                                     client);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    if (table.hasNamespace()) {
      return SLASH.join(warehouseLocation, table.namespace().toString(), table.name());
    }
    return SLASH.join(warehouseLocation, table.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    String[] tables = client.getAllTables(branch.get().getName(),
                                         namespace == null ? null : namespace.toString());
    return Arrays.stream(tables).map(NessieCatalog::fromString).collect(Collectors.toList());
  }

  private static TableIdentifier fromString(String tableName) {
    String[] names = tableName.split("\\.");
    String namespace = null;
    if (names.length > 1) {
      namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
    }
    String name = names[names.length - 1];
    return TableIdentifier.of(namespace, name);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    Table existingTable = table(identifier);
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

    client.commit(branch.get(), ImmutableTable.copyOf(existingTable).withIsDeleted(true));
    if (purge && lastMetadata != null) {
      BaseMetastoreCatalog.dropTableData(ops.io(), lastMetadata);
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Table existingFromTable = table(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException(String.format("table %s doesn't exists", from.name()));
    }
    if (existingFromTable.isDeleted()) {
      throw new NoSuchTableException(String.format("table %s doesn't exists",
                                                   existingFromTable.getName()));
    }
    Table existingToTable = table(to);
    if (existingToTable != null && !existingToTable.isDeleted()) {
      throw new AlreadyExistsException("table {} already exists", to.name());
    }
    String name = to.name();
    String namespace = to.hasNamespace() ? to.namespace().toString() : null;
    String id = (namespace != null) ? namespace + "." + name : name;
    Table updatedTable = ImmutableTable.builder().from(existingFromTable)
                                       .name(name)
                                       .namespace(namespace)
                                       .id(id)
                                       .build();
    List<Table> tables = new ArrayList<>();
    if (existingToTable != null) {
      Table deletedTable = ImmutableTable.copyOf(existingToTable).withIsDeleted(true);
      tables.add(deletedTable);
    }
    Table deletedTable = ImmutableTable.copyOf(existingFromTable).withIsDeleted(true);
    tables.add(updatedTable);
    tables.add(deletedTable);
    try {
      client.commit(branch.get(), tables.toArray(new Table[0]));
    } catch (Exception e) {
      throw new CommitFailedException(e, "failed");
    }
  }

  @Override
  public void createBranch(String branchName, String parentName) {
    client.createBranch(ImmutableBranch.builder().name(branchName).id(parentName).build());
  }

  @Override
  public boolean dropBranch(String branchName) {
    Branch currentBranch = client.getBranch(branchName);
    if (currentBranch == null) {
      return false;
    }
    client.deleteBranch(currentBranch);
    return true;
  }

  @Override
  public void promoteBranch(String from, boolean force) {
    Branch existingFromTable = client.getBranch(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException("branch %s doesn't exists", from);
    }
    try {
      client.mergeBranch(existingFromTable, branch.get().getName(), force);
    } catch (Throwable t) {
      throw new CommitFailedException(t, "failed");
    }
    refreshBranch();
  }

  public void refreshBranch() {
    Branch newBranch = client.getBranch(branch.get().getName());
    branch.set(newBranch);
  }

}
