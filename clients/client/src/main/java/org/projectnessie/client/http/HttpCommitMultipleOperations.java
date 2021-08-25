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

import java.util.List;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;

final class HttpCommitMultipleOperations extends BaseHttpRequest
    implements CommitMultipleOperationsBuilder {

  private ImmutableOperations.Builder operations = ImmutableOperations.builder();
  private String branchName;
  private String hash;

  HttpCommitMultipleOperations(NessieHttpClient client) {
    super(client);
  }

  @Override
  public CommitMultipleOperationsBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder commitMeta(CommitMeta commitMeta) {
    operations.commitMeta(commitMeta);
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder operations(List<Operation> operations) {
    this.operations.addAllOperations(operations);
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder operation(Operation operation) {
    operations.addOperations(operation);
    return this;
  }

  @Override
  public Branch submit() throws NessieNotFoundException, NessieConflictException {
    return client.getTreeApi().commitMultipleOperations(branchName, hash, operations.build());
  }
}
