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
import com.dremio.nessie.client.rest.NessieNotFoundClientException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableDelete;
import com.dremio.nessie.model.ImmutableMultiContents;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.ImmutableReferenceUpdate;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.Reference;
import com.google.common.base.Joiner;

/**
 * Nessie implementation of Iceberg Catalog.
 */
public class NessieCatalog extends BaseMetastoreCatalog implements AutoCloseable {

  public static final String CONF_NESSIE_URL = "nessie.url";
  public static final String CONF_NESSIE_USERNAME = "nessie.username";
  public static final String CONF_NESSIE_PASSWORD = "nessie.password";
  public static final String CONF_NESSIE_AUTH_TYPE = "nessie.auth_type";
  public static final String NESSIE_AUTH_TYPE_DEFAULT = "BASIC";
  public static final String CONF_NESSIE_REF = "nessie.ref";

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
    String path = config.get(CONF_NESSIE_URL);
    String username = config.get(CONF_NESSIE_USERNAME);
    String password = config.get(CONF_NESSIE_PASSWORD);
    String authTypeStr = config.get(CONF_NESSIE_AUTH_TYPE, NESSIE_AUTH_TYPE_DEFAULT);
    AuthType authType = AuthType.valueOf(authTypeStr);
    this.client = new NessieClient(authType, path, username, password);

    warehouseLocation = config.get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;

    final String requestedRef = config.get(CONF_NESSIE_REF);
    try {
      Reference r = requestedRef == null ? client.getTreeApi().getDefaultBranch() : client.getTreeApi().getReferenceByName(requestedRef);
      this.reference = new UpdateableReference(r, client.getTreeApi());
    } catch (NessieNotFoundClientException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(String.format("Nessie ref '%s' provided via %s does not exist. "
            + "This ref must exist before creating a NessieCatalog.", requestedRef, CONF_NESSIE_REF), ex);
      }

      throw new IllegalArgumentException(String.format("Nessie does not have an existing default branch."
          + "Either configure an alternative ref via %s or create the default branch on the server.", CONF_NESSIE_REF), ex);
    }

  }


  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  protected String name() {
    return "nessie";
  }

  private static ContentsKey toKey(TableIdentifier tableIdentifier) {
    List<String> identifiers = new ArrayList<>();
    if (tableIdentifier.hasNamespace()) {
      identifiers.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
    }
    identifiers.add(tableIdentifier.name());

    ContentsKey key = new ContentsKey(identifiers);
    return key;
  }

  private IcebergTable table(TableIdentifier tableIdentifier) {
    try {
      Contents table = client.getContentsApi().getContents(reference.getHash(), toKey(tableIdentifier));
      if (table instanceof IcebergTable) {
        return (IcebergTable) table;
      }
    } catch (NessieNotFoundClientException | NessieNotFoundException e) {
      // ignore
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
        .getEntries(reference.getHash())
        .getEntries()
        .stream()
        .filter(namespacePredicate(namespace))
        .map(NessieCatalog::toIdentifier)
        .collect(Collectors.toList());
  }

  private static Predicate<EntriesResponse.Entry> namespacePredicate(Namespace ns) {
    // TODO: filter to just iceberg tables.
    if (ns == null) {
      return e -> true;
    }

    final List<String> namespace = Arrays.asList(ns.levels());
    Predicate<EntriesResponse.Entry> predicate = e -> {
      List<String> names = e.getName().getElements();

      if (names.size() <= namespace.size()) {
        return false;
      }

      return namespace.equals(names.subList(0, namespace.size()));
    };
    return predicate;
  }

  private static TableIdentifier toIdentifier(EntriesResponse.Entry entry) {
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

    client.getContentsApi().deleteContents(toKey(identifier), "no message", reference.getAsBranch());

    // TODO: purge should be blocked since nessie will clean through other means.
    if (purge && lastMetadata != null) {
      BaseMetastoreCatalog.dropTableData(ops.io(), lastMetadata);
    }
    // TODO: fix this so we don't depend on it in tests.
    refresh();
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
        .addOperations(ImmutablePut.builder().key(toKey(to)).contents(existingFromTable).build())
        .addOperations(ImmutableDelete.builder().key(toKey(from)).build())
        .build();

    try {
      client.getContentsApi().commitMultipleOperations("iceberg rename table", c);
      // TODO: fix this so we don't depend on it in tests.
      refresh();
    } catch (Exception e) {
      throw new CommitFailedException(e, "failed");
    }
  }

  public void createBranch(String branchName, Optional<String> hash) {
    client.getTreeApi().createNewReference(ImmutableBranch.builder().name(branchName).hash(hash.orElse(null)).build());
  }

  public boolean deleteBranch(String branchName, String hash) {
    client.getTreeApi().deleteReference(ImmutableBranch.builder().name(branchName).hash(hash).build());
    return true;
  }

  public void assignReference(String from, String currentHash, String targetHash) {
    client.getTreeApi().assignReference(from, ImmutableReferenceUpdate.builder().expectedId(currentHash).updateId(targetHash).build());
    refresh();
  }

  public void refresh() {
    reference.refresh();
  }

  public String getHashForRef(String ref) {
    return client.getTreeApi().getReferenceByName(ref).getHash();
  }

  public void createTag(String tagName, String hash) {
    client.getTreeApi().createNewReference(ImmutableTag.builder().name(tagName).hash(hash).build());
  }

  public void deleteTag(String tagName, String hash) {
    client.getTreeApi().deleteReference(ImmutableTag.builder().name(tagName).hash(hash).build());
  }

  public String getHash() {
    return reference.getHash();
  }

}
