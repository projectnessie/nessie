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
package org.projectnessie.client.rest.v1;

import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.builder.BaseOnBranchBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

final class HttpDeleteBranch extends BaseOnBranchBuilder<DeleteBranchBuilder>
    implements DeleteBranchBuilder {
  private final NessieApiClient client;

  HttpDeleteBranch(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public void delete() throws NessieConflictException, NessieNotFoundException {
    client.getTreeApi().deleteReference(Reference.ReferenceType.BRANCH, branchName, hash);
  }

  @Override
  public Branch getAndDelete() {
    throw new UnsupportedOperationException(
        "The getAndDelete operation is not supported for branches in API v1");
  }
}
