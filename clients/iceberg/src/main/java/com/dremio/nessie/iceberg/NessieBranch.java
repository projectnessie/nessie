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
import com.dremio.nessie.iceberg.branch.Branch;
import java.util.StringJoiner;

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

  @Override
  public String toString() {
    return new StringJoiner(", ", NessieBranch.class.getSimpleName() + "[", "]")
      .add("client=" + client)
      .add("branch=" + branch)
      .toString();
  }

}
