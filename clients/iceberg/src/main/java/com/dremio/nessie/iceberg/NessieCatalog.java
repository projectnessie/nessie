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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
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

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.iceberg.branch.GitCatalog;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableDelete;
import com.dremio.nessie.model.ImmutableMultiContents;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.ImmutableReferenceUpdate;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.NessieObjectKey;
import com.dremio.nessie.model.ObjectsResponse;
import com.dremio.nessie.model.Reference;
import com.google.common.base.Joiner;

/**
 * Nessie implementation of Iceberg Catalog.
 */
public class NessieCatalog extends BaseMetastoreCatalog implements GitCatalog, AutoCloseable {

  private static final Joiner SLASH = Joiner.on("/");
  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final NessieClient client;
  private final String warehouseLocation;
  private final Configuration config;
  private final UpdateableReference reference;

  /**
   * create a catalog from a hadoop configuration.
   */
  public NessieCatalog(Configuration config) {
    this.config = config;
    String path = config.get("nessie.url");
    String username = config.get("nessie.username");
    String password = config.get("nessie.password");
    String authTypeStr = config.get("nessie.auth.type", "BASIC");
    AuthType authType = AuthType.valueOf(authTypeStr);
    this.client = new NessieClient(authType, path, username, password);

    warehouseLocation = config.get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;

    final String requestedRef = config.get("nessie.ref");
    Reference r = requestedRef == null ? client.getTreeApi().getDefaultBranch() : client.getTreeApi().getReferenceByName(requestedRef);
    this.reference = new UpdateableReference(r, client.getTreeApi());
  }


  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  protected String name() {
    return "nessie";
  }

  private static NessieObjectKey toKey(TableIdentifier tableIdentifier) {
    List<String> identifiers = new ArrayList<>();
    if (tableIdentifier.hasNamespace()) {
      identifiers.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
    }
    identifiers.add(tableIdentifier.name());

    NessieObjectKey key = new NessieObjectKey(identifiers);
    return key;
  }

  private IcebergTable table(TableIdentifier tableIdentifier) {
    Contents table = client.getContentsApi().getObjectForReference(reference.getHash(), toKey(tableIdentifier));
    if (table instanceof IcebergTable) {
      return (IcebergTable) table;
    }

    return null;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new NessieTableOperations(config,
                                     toKey(tableIdentifier),
                                     reference,
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
    return client.getTreeApi()
        .getObjects(reference.getHash())
        .getEntries()
        .stream()
        .filter(namespacePredicate(namespace))
        .map(NessieCatalog::toIdentifier)
        .collect(Collectors.toList());
  }

  private static Predicate<ObjectsResponse.Entry> namespacePredicate(Namespace ns) {
    // TODO: filter to just iceberg tables.
    final List<String> namespace = Arrays.asList(ns.levels());
    Predicate<ObjectsResponse.Entry> predicate = e -> {
      List<String> names = e.getName().getElements();

      if (names.size() <= namespace.size()) {
        return false;
      }

      return namespace.equals(names.subList(0, namespace.size()));
    };
    return predicate;
  }

  private static TableIdentifier toIdentifier(ObjectsResponse.Entry entry) {
    List<String> elements = entry.getName().getElements();
    return TableIdentifier.of(elements.toArray(new String[elements.size()]));
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    reference.checkMutable();

    IcebergTable existingTable = table(identifier);
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



    client.getContentsApi().deleteObject(toKey(identifier), "no message", reference.getAsBranch());
    if (purge && lastMetadata != null) {
      BaseMetastoreCatalog.dropTableData(ops.io(), lastMetadata);
    }
    return true;
  }



  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    reference.checkMutable();

    IcebergTable existingFromTable = table(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException(String.format("table %s doesn't exists", from.name()));
    }
    IcebergTable existingToTable = table(to);
    if (existingToTable != null) {
      throw new AlreadyExistsException("table {} already exists", to.name());
    }

    MultiContents c = ImmutableMultiContents.builder()
        .branch(reference.getAsBranch())
        .addOperations(ImmutablePut.builder().key(toKey(to)).build())
        .addOperations(ImmutableDelete.builder().key(toKey(from)).build())
        .build();

    try {
      client.getContentsApi().commitMultipleOperations("iceberg rename table", c);
    } catch (Exception e) {
      throw new CommitFailedException(e, "failed");
    }
  }

  @Override
  public void createBranch(String branchName, Optional<String> hash) {
    client.getTreeApi().createNewReference(ImmutableBranch.builder().name(branchName).hash(hash.orElse(null)).build());
  }

  @Override
  public boolean deleteBranch(String branchName, String hash) {
    client.getTreeApi().deleteReference(ImmutableBranch.builder().name(branchName).hash(hash).build());
    return true;
  }

  @Override
  public void assignReference(String from, String currentHash, String targetHash) {
    client.getTreeApi().assignReference(from, ImmutableReferenceUpdate.builder().expectedId(currentHash).updateId(targetHash).build());
    refresh();
  }

  public void refresh() {
    reference.refresh();
  }

  @Override
  public void createTag(String tagName, String hash) {
    client.getTreeApi().createNewReference(ImmutableTag.builder().name(tagName).hash(hash).build());
  }

  @Override
  public void deleteTag(String tagName, String hash) {
    client.getTreeApi().deleteReference(ImmutableTag.builder().name(tagName).hash(hash).build());
  }


}
