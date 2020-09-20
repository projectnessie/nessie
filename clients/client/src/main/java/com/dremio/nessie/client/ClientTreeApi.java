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
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.Merge;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.Transplant;

public class ClientTreeApi implements TreeApi {

  private static final GenericType<List<Reference>> REFERENCE_LIST = new GenericType<List<Reference>>() {
  };
  private final WebTarget target;

  public ClientTreeApi(WebTarget target) {
    this.target = target;
  }

  @Override
  public List<Reference> getAllReferences() {
    return target.path("trees/")
                   .request(MediaType.APPLICATION_JSON_TYPE)
                   .accept(MediaType.APPLICATION_JSON_TYPE)
                   .get()
                   .readEntity(REFERENCE_LIST);
  }

  @Override
  public Reference getReferenceByName(@NotNull String refName) throws NessieNotFoundException {
    return target.path("trees/tree/" + refName)
                   .request()
                   .accept(MediaType.APPLICATION_JSON_TYPE)
                   .get()
                   .readEntity(Reference.class);
  }

  @Override
  public void createEmptyTag(String tagName) throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/tag/%s", tagName))
          .request()
          .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public void createNewTag(@NotNull String tagName, String hash) throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/tag/%s/%s", tagName, hash))
          .request()
          .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public void assignTag(@NotNull String tagName, @NotNull String oldHash, @NotNull String newHash)
      throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/tag/%s/%s/%s", tagName, oldHash, newHash))
          .request()
          .put(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public void deleteTag(@NotNull String tagName, @NotNull String hash) throws NessieConflictException, NessieNotFoundException {
    target.path(String.format("trees/tag/%s/%s", tagName, hash))
          .request()
          .delete();
  }

  @Override
  public void createEmptyBranch(@NotNull String branchName) throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/branch/%s", branchName))
          .request()
          .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public void createNewBranch(@NotNull String branchName, @NotNull String hash) throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/branch/%s/%s", branchName, hash))
          .request()
          .post(Entity.json(null));
  }

  @Override
  public void assignBranch(@NotNull String branchName, @NotNull String oldHash,
                           @NotNull String newHash) throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/branch/%s/%s/%s", branchName, oldHash, newHash))
          .request()
          .put(Entity.entity("", MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public void deleteBranch(@NotNull String tagName, @NotNull String hash) throws NessieConflictException, NessieNotFoundException {
    target.path(String.format("trees/branch/%s/%s", tagName, hash))
          .request()
          .delete();
  }

  @Override
  public Branch getDefaultBranch() {
    return target.path("trees/tree")
                   .request()
                   .accept(MediaType.APPLICATION_JSON_TYPE)
                   .get()
                   .readEntity(Branch.class);
  }

  @Override
  public LogResponse getCommitLog(@NotNull String ref) throws NessieNotFoundException {
    return target.path(String.format("trees/tree/%s/log", ref))
                   .request()
                   .accept(MediaType.APPLICATION_JSON_TYPE)
                   .get()
                   .readEntity(LogResponse.class);
  }

  @Override
  public void transplantCommitsIntoBranch(String message, Transplant transplant) throws NessieNotFoundException, NessieConflictException {
    target.path("trees/transplant")
            .queryParam("message", message)
            .request()
            .put(Entity.entity(transplant, MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public void mergeRefIntoBranch(@NotNull Merge merge) throws NessieNotFoundException, NessieConflictException {
    target.path("trees/merge")
            .request()
            .put(Entity.entity(merge, MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public EntriesResponse getEntries(@NotNull String refName) throws NessieNotFoundException {
    return target.path(String.format("trees/tree/%s/entries", refName))
                   .request()
                   .accept(MediaType.APPLICATION_JSON_TYPE)
                   .get()
                   .readEntity(EntriesResponse.class);
  }

  @Override
  public void commitMultipleOperations(@NotNull String hash, String message,
                                       @NotNull MultiContents operations) throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/multi/%s", hash))
          .queryParam("message", message)
          .request()
          .put(Entity.entity(operations, MediaType.APPLICATION_JSON_TYPE));
  }

  @Override
  public void commitMultipleOperations(String branch, @NotNull String hash, String message,
                                       @NotNull MultiContents operations) throws NessieNotFoundException, NessieConflictException {
    target.path(String.format("trees/multi/%s/%s", branch, hash))
          .queryParam("message", message)
          .request()
          .put(Entity.entity(operations, MediaType.APPLICATION_JSON_TYPE));
  }
}
