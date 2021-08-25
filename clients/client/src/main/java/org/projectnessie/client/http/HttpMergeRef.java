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

import org.projectnessie.client.api.MergeRefBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ImmutableMerge;

final class HttpMergeRef extends BaseHttpRequest implements MergeRefBuilder {

  private final ImmutableMerge.Builder merge = ImmutableMerge.builder();
  private String branchName;
  private String hash;

  HttpMergeRef(NessieHttpClient client) {
    super(client);
  }

  @Override
  public MergeRefBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public MergeRefBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public MergeRefBuilder fromHash(String fromHash) {
    merge.fromHash(fromHash);
    return this;
  }

  @Override
  public void submit() throws NessieNotFoundException, NessieConflictException {
    client.getTreeApi().mergeRefIntoBranch(branchName, hash, merge.build());
  }
}
