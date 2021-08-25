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
package org.projectnessie.client.http;

import org.projectnessie.client.api.AssignBranchBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;

final class HttpAssignBranch extends BaseHttpRequest implements AssignBranchBuilder {

  private String branchName;
  private String oldHash;
  private Branch assignTo;

  HttpAssignBranch(NessieHttpClient client) {
    super(client);
  }

  @Override
  public AssignBranchBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public AssignBranchBuilder oldHash(String oldHash) {
    this.oldHash = oldHash;
    return this;
  }

  @Override
  public AssignBranchBuilder assignTo(Branch assignTo) {
    this.assignTo = assignTo;
    return this;
  }

  @Override
  public void submit() throws NessieNotFoundException, NessieConflictException {
    client.getTreeApi().assignBranch(branchName, oldHash, assignTo);
  }
}
