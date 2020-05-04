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
import com.dremio.nessie.client.branch.CopyBranch;
import com.dremio.nessie.client.branch.UpdateBranch;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.HeadVersionPair;
import com.google.common.base.Joiner;
import java.util.List;
import java.util.StringJoiner;
import javax.ws.rs.core.EntityTag;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Nessie representation of Iceberg Branch.
 */
class NessieBranch implements Branch {
  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on('.');
  private final NessieClient client;
  private HeadVersionPair headVersionPair;
  private com.dremio.nessie.model.Branch branch;

  public NessieBranch(com.dremio.nessie.model.Branch branch,
                      NessieClient client,
                      HeadVersionPair headVersionPair) {
    this.branch = branch;
    this.client = client;
    this.headVersionPair = headVersionPair;
  }

  @Override
  public String name() {
    return branch.getName();
  }

  @Override
  public void refresh() {
    Branch branch = client.getBranch(this.branch.getName());
    this.branch = ((NessieBranch) branch).branch;
    this.headVersionPair = ((NessieBranch) branch).headVersionPair;
  }

  //@Override
  public void commit(Branch old) {
    //Map<String, TableVersion> snapshots = new HashMap<>();
    //for (TableIdentifier t : old.tables()) {
    //  TableVersion tableVersion = setTableVersion(t, old.getMetadataLocation(t));
    //  if (snapshots.put(tableVersion.getUuid(), tableVersion) != null) {
    //    throw new IllegalStateException("Duplicate key");
    //  }
    //}
    //Map<String, TableVersion> newSnapshots = new HashMap<>(branch.getTableVersions());
    //newSnapshots.putAll(snapshots);
    //com.dremio.nessie.model.Branch newBranch = ImmutableBranch.builder().from(branch)
    //                                                  .tableVersions(newSnapshots)
    //                                                  .build();
    //client.getBranchClient().updateObject(newBranch);
    //refresh();
  }

  private BranchTable getTable(TableIdentifier tableIdentifier) {
    return client.getTable(branch.getName(),
                           tableIdentifier.name(),
                           tableIdentifier.hasNamespace()
                             ? tableIdentifier.namespace().toString() : null);
  }

  @Override
  public String getMetadataLocation(TableIdentifier tableIdentifier) {
    BranchTable table = getTable(tableIdentifier);
    if (table == null) {
      return null;
    }
    return table.getMetadataLocation();
  }

  //@Override
  public List<TableIdentifier> tables() {
    //return branch.getTableVersions()
    //          .keySet()
    //          .stream()
    //          .map(s -> client.getTableClient().getObject(s))
    //          .map(NessieBranch::tableIdentifier)
    //          .collect(Collectors.toList());
    return null;
  }

  //@Override
  public UpdateBranch updateTables() {
    refresh();
    return new UpdateBranch(this);
  }

  //@Override
  public CopyBranch copyTables() {
    refresh();
    return new CopyBranch(this);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", NessieBranch.class.getSimpleName() + "[", "]")
      .add("client=" + client)
      .add("branch=" + branch)
      .toString();
  }

  public EntityTag version() {
    return new EntityTag(
      SLASH.join(headVersionPair.getHead(), Long.toString(headVersionPair.getVersion()))
    );
  }

  com.dremio.nessie.model.Branch branch() {
    return branch;
  }
}
