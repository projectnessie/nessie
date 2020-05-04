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
import java.util.List;
import java.util.StringJoiner;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Nessie representation of Iceberg Branch.
 */
class NessieBranch implements Branch {
  private final NessieClient client;
  private com.dremio.nessie.model.Branch branch;

  public NessieBranch(com.dremio.nessie.model.Branch branch,
                      NessieClient client) {
    this.branch = branch;
    this.client = client;
  }

  @Override
  public String name() {
    return branch.getName();
  }

  @Override
  public void refresh() {
    this.branch = client.getBranch(this.branch.getName());
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

}
