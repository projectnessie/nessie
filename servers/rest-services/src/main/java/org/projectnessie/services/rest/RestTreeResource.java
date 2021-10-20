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
package org.projectnessie.services.rest;

import java.util.List;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.http.HttpTreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Transplant;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.TreeApiImplWithAuthorization;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the tree-API. */
@RequestScoped
public class RestTreeResource implements HttpTreeApi {
  // Cannot extend the TreeApiImplWithAuthn class, because then CDI gets confused
  // about which interface to use - either HttpTreeApi or the plain TreeApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final ServerConfig config;
  private final VersionStore<Contents, CommitMeta, Type> store;
  private final AccessChecker accessChecker;

  @Context SecurityContext securityContext;

  // Mandated by CDI 2.0
  public RestTreeResource() {
    this(null, null, null);
  }

  @Inject
  public RestTreeResource(
      ServerConfig config,
      VersionStore<Contents, CommitMeta, Type> store,
      AccessChecker accessChecker) {
    this.config = config;
    this.store = store;
    this.accessChecker = accessChecker;
  }

  private TreeApi resource() {
    return new TreeApiImplWithAuthorization(
        config,
        store,
        accessChecker,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  @Override
  public List<Reference> getAllReferences() {
    return resource().getAllReferences();
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return resource().getDefaultBranch();
  }

  @Override
  public Reference createReference(String sourceRefName, Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    return resource().createReference(sourceRefName, reference);
  }

  @Override
  public Reference getReferenceByName(String refName) throws NessieNotFoundException {
    return resource().getReferenceByName(refName);
  }

  @Override
  public EntriesResponse getEntries(String refName, EntriesParams params)
      throws NessieNotFoundException {
    return resource().getEntries(refName, params);
  }

  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    return resource().getCommitLog(ref, params);
  }

  @Override
  public void assignTag(String tagName, String oldHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    resource().assignTag(tagName, oldHash, assignTo);
  }

  @Override
  public void deleteTag(String tagName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    resource().deleteTag(tagName, hash);
  }

  @Override
  public void assignBranch(String branchName, String oldHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    resource().assignBranch(branchName, oldHash, assignTo);
  }

  @Override
  public void deleteBranch(String branchName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    resource().deleteBranch(branchName, hash);
  }

  @Override
  public void transplantCommitsIntoBranch(
      String branchName, String hash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    resource().transplantCommitsIntoBranch(branchName, hash, message, transplant);
  }

  @Override
  public void mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    resource().mergeRefIntoBranch(branchName, hash, merge);
  }

  @Override
  public Branch commitMultipleOperations(String branchName, String hash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    return resource().commitMultipleOperations(branchName, hash, operations);
  }
}
