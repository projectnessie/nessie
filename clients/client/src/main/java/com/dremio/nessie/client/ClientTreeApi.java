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

import java.util.List;

import javax.validation.constraints.NotNull;

import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.http.HttpClient;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.Merge;
import com.dremio.nessie.model.Operations;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.Tag;
import com.dremio.nessie.model.Transplant;
import com.fasterxml.jackson.core.type.TypeReference;

class ClientTreeApi implements TreeApi {

  private static final TypeReference<List<Reference>> REFERENCE_LIST = new TypeReference<List<Reference>>() {
  };

  private final HttpClient client;

  public ClientTreeApi(HttpClient client) {
    this.client = client;
  }

  @Override
  public List<Reference> getAllReferences() {
    return client.create().path("trees").get().readEntity(REFERENCE_LIST);
  }

  @Override
  public void createReference(@NotNull Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    client.create().path("trees/tree").post(reference);
  }

  @Override
  public Reference getReferenceByName(@NotNull String refName) throws NessieNotFoundException {
    return client.create().path("trees/tree/{ref}").resolveTemplate("ref", refName).get().readEntity(Reference.class);
  }

  @Override
  public void assignTag(@NotNull String tagName, @NotNull String expectedHash, @NotNull Tag tag)
      throws NessieNotFoundException, NessieConflictException {
    client.create().path("trees/tag/{tagName}")
          .resolveTemplate("tagName", tagName)
          .queryParam("expectedHash", expectedHash)
          .put(tag);
  }

  @Override
  public void deleteTag(@NotNull String tagName, @NotNull String expectedHash) throws NessieConflictException, NessieNotFoundException {
    client.create().path("trees/tag/{tagName}")
          .resolveTemplate("tagName", tagName)
          .queryParam("expectedHash", expectedHash)
          .delete();
  }

  @Override
  public void assignBranch(@NotNull String branchName, @NotNull String expectedHash,
                           @NotNull Branch branch) throws NessieNotFoundException, NessieConflictException {
    client.create().path("trees/branch/{branchName}")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .put(branch);
  }

  @Override
  public void deleteBranch(@NotNull String branchName, @NotNull String expectedHash)
      throws NessieConflictException, NessieNotFoundException {
    client.create().path("trees/branch/{branchName}")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .delete();
  }

  @Override
  public Branch getDefaultBranch() {
    return client.create().path("trees/tree").get().readEntity(Branch.class);
  }

  @Override
  public LogResponse getCommitLog(@NotNull String ref) throws NessieNotFoundException {
    return client.create().path("trees/tree/{ref}/log").resolveTemplate("ref", ref)
                 .get()
                 .readEntity(LogResponse.class);
  }

  @Override
  public void transplantCommitsIntoBranch(@NotNull String branchName, @NotNull String expectedHash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    client.create().path("trees/branch/{branchName}/transplant")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .queryParam("message", message)
          .put(transplant);
  }

  @Override
  public void mergeRefIntoBranch(@NotNull String branchName, @NotNull String expectedHash, @NotNull Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    client.create().path("trees/branch/{branchName}/merge")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .put(merge);
  }

  @Override
  public EntriesResponse getEntries(@NotNull String refName) throws NessieNotFoundException {
    return client.create().path("trees/tree/{ref}/entries")
                 .resolveTemplate("ref", refName)
                 .get()
                 .readEntity(EntriesResponse.class);
  }

  @Override
  public void commitMultipleOperations(String branch, @NotNull String expectedHash, String message,
                                       @NotNull Operations operations) throws NessieNotFoundException, NessieConflictException {
    client.create().path("trees/branch/{branchName}/commit")
          .resolveTemplate("branchName", branch)
          .queryParam("expectedHash", expectedHash)
          .queryParam("message", message)
          .post(operations);
  }
}
