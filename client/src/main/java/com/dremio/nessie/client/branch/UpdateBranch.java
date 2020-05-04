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

package com.dremio.nessie.client.branch;

import com.dremio.nessie.client.NessieTableOperations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * transaction object to update branchs.
 */
public class UpdateBranch implements PendingUpdate<Branch> {

  private final Branch branch;
  private final Map<TableIdentifier, TableOperations> updates = new HashMap<>();

  public UpdateBranch(Branch branch) {
    this.branch = branch;
  }

  public UpdateBranch updateTable(TableIdentifier tableIdentifier,
                                  TableOperations table) {
    updates.put(tableIdentifier, table);
    return this;
  }

  @Override
  public Branch apply() {
    return branch;
  }

  @Override
  public void commit() {
    try {
      //branch.commit(new UpdateBranchBranch(updates));
    } finally {
      branch.refresh(); // todo if it fails we need to ensure we get old data back
    }
  }

}
