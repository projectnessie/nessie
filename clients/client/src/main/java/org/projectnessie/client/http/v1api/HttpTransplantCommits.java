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
package org.projectnessie.client.http.v1api;

import java.util.List;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.client.http.NessieHttpClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ImmutableTransplant;

final class HttpTransplantCommits extends BaseHttpRequest implements TransplantCommitsBuilder {

  private final ImmutableTransplant.Builder transplant = ImmutableTransplant.builder();
  private String branchName;
  private String hash;
  private String message;

  HttpTransplantCommits(NessieHttpClient client) {
    super(client);
  }

  @Override
  public TransplantCommitsBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public TransplantCommitsBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public TransplantCommitsBuilder message(String message) {
    this.message = message;
    return this;
  }

  @Override
  public TransplantCommitsBuilder hashesToTransplant(List<String> hashesToTransplant) {
    transplant.addAllHashesToTransplant(hashesToTransplant);
    return this;
  }

  @Override
  public void submit() throws NessieNotFoundException, NessieConflictException {
    client.getTreeApi().transplantCommitsIntoBranch(branchName, hash, message, transplant.build());
  }
}
