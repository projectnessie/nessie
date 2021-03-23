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

import java.security.Principal;
import java.util.List;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.projectnessie.api.TreeRestApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.versioned.VersionStore;

/**
 * REST endpoint for trees.
 */
@RequestScoped
public class TreeResource implements TreeRestApi {

  /**
   * Delegate to (do not extend) the implementation for better code-coverage.
   */
  private final TreeApiImpl delegate;

  @Inject
  public TreeResource(ServerConfig config, Principal principal,
      VersionStore<Contents, CommitMeta> store) {
    delegate = new TreeApiImpl(config, principal, store);
  }

  @Override
  public List<Reference> getAllReferences() {
    return delegate.getAllReferences();
  }

  @Override
  public Reference getReferenceByName(String refName) throws NessieNotFoundException {
    return delegate.getReferenceByName(refName);
  }

  @Override
  public void createReference(Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    delegate.createReference(reference);
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return delegate.getDefaultBranch();
  }

  @Override
  public void assignTag(String tagName, String expectedHash, Tag tag)
      throws NessieNotFoundException, NessieConflictException {
    delegate.assignTag(tagName, expectedHash, tag);
  }

  @Override
  public void deleteTag(String tagName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    delegate.deleteTag(tagName, hash);
  }

  @Override
  public void assignBranch(String branchName, String expectedHash, Branch branch)
      throws NessieNotFoundException, NessieConflictException {
    delegate.assignBranch(branchName, expectedHash, branch);
  }

  @Override
  public void deleteBranch(String branchName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    delegate.deleteBranch(branchName, hash);
  }

  @Override
  public LogResponse getCommitLog(String ref, Integer maxEntriesHint, String pageToken)
      throws NessieNotFoundException {
    return delegate.getCommitLog(ref, maxEntriesHint, pageToken);
  }

  @Override
  public void transplantCommitsIntoBranch(String branchName, String hash, String message,
      Transplant transplant) throws NessieNotFoundException, NessieConflictException {
    delegate.transplantCommitsIntoBranch(branchName, hash, message, transplant);
  }

  @Override
  public void mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    delegate.mergeRefIntoBranch(branchName, hash, merge);
  }

  @Override
  public EntriesResponse getEntries(String refName, Integer maxEntriesHint, String pageToken)
      throws NessieNotFoundException {
    return delegate.getEntries(refName, maxEntriesHint, pageToken);
  }

  @Override
  public void commitMultipleOperations(String branch, String hash, String message,
      Operations operations) throws NessieNotFoundException, NessieConflictException {
    delegate.commitMultipleOperations(branch, hash, message, operations);
  }
}
