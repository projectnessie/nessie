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
package org.projectnessie.services.impl;

import static org.projectnessie.model.Operation.Delete;
import static org.projectnessie.model.Operation.Put;

import java.security.AccessControlException;
import java.security.Principal;
import javax.annotation.Nullable;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.api.params.GetReferenceParams;
import org.projectnessie.api.params.ReferencesParams;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.Transplant;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.authz.ServerAccessContext;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.VersionStore;

/** Does authorization checks (if enabled) on the {@link TreeApiImpl}. */
public class TreeApiImplWithAuthorization extends TreeApiImpl {

  public TreeApiImplWithAuthorization(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Type> store,
      AccessChecker accessChecker,
      Principal principal) {
    super(config, store, accessChecker, principal);
  }

  @Override
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    ServerAccessContext accessContext = createAccessContext();
    ImmutableReferencesResponse.Builder resp = ReferencesResponse.builder();
    super.getAllReferences(params).getReferences().stream()
        .filter(
            ref -> {
              try {
                getAccessChecker().canViewReference(accessContext, RefUtil.toNamedRef(ref));
                return true;
              } catch (AccessControlException e) {
                return false;
              }
            })
        .forEach(resp::addReferences);
    return resp.build();
  }

  @Override
  public Reference getReferenceByName(GetReferenceParams params) throws NessieNotFoundException {
    Reference ref = super.getReferenceByName(params);
    getAccessChecker().canViewReference(createAccessContext(), RefUtil.toNamedRef(ref));
    return ref;
  }

  @Override
  public Reference createReference(@Nullable String sourceRefName, Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    getAccessChecker().canCreateReference(createAccessContext(), RefUtil.toNamedRef(reference));

    try {
      getAccessChecker()
          .canViewReference(
              createAccessContext(),
              namedRefWithHashOrThrow(sourceRefName, reference.getHash()).getValue());
    } catch (NessieNotFoundException e) {
      // If the default-branch does not exist and hashOnRef points to the "beginning of time",
      // then do not throw a NessieNotFoundException, but re-create the default branch. In all
      // cases, re-throw the exception.
      if (!(reference instanceof Branch
          && reference.getName().equals(getConfig().getDefaultBranch())
          && (null == reference.getHash()
              || getStore().noAncestorHash().asString().equals(reference.getHash())))) {
        throw e;
      }
    }
    return super.createReference(sourceRefName, reference);
  }

  @Override
  protected void assignReference(NamedRef ref, String oldHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    ServerAccessContext accessContext = createAccessContext();
    getAccessChecker()
        .canViewReference(
            accessContext,
            namedRefWithHashOrThrow(assignTo.getName(), assignTo.getHash()).getValue());
    getAccessChecker().canAssignRefToHash(accessContext, ref);
    super.assignReference(ref, oldHash, assignTo);
  }

  @Override
  protected void deleteReference(NamedRef ref, String hash)
      throws NessieConflictException, NessieNotFoundException {
    getAccessChecker().canDeleteReference(createAccessContext(), ref);
    super.deleteReference(ref, hash);
  }

  @Override
  public EntriesResponse getEntries(String namedRef, EntriesParams params)
      throws NessieNotFoundException {
    getAccessChecker()
        .canReadEntries(
            createAccessContext(),
            namedRefWithHashOrThrow(namedRef, params.hashOnRef()).getValue());
    return super.getEntries(namedRef, params);
  }

  @Override
  public LogResponse getCommitLog(String namedRef, CommitLogParams params)
      throws NessieNotFoundException {
    String checkHash = params.pageToken() == null ? params.endHash() : params.pageToken();
    NamedRef validatedNamedRef;
    try {
      getAccessChecker().canViewRefLog(createAccessContext());
      // when the user can access reflog,
      // allow them to bypass the validation to fetch commits from dead reference.
      validatedNamedRef = BranchName.of(namedRef);
    } catch (AccessControlException ex) {
      // validate the named reference with hash
      validatedNamedRef = namedRefWithHashOrThrow(namedRef, checkHash).getValue();
    }
    getAccessChecker().canListCommitLog(createAccessContext(), validatedNamedRef);
    return super.getCommitLog(namedRef, params);
  }

  @Override
  public void transplantCommitsIntoBranch(
      String branchName, String hash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    if (transplant.getHashesToTransplant().isEmpty()) {
      throw new IllegalArgumentException("No hashes given to transplant.");
    }

    ServerAccessContext accessContext = createAccessContext();
    getAccessChecker()
        .canViewReference(
            accessContext,
            namedRefWithHashOrThrow(
                    transplant.getFromRefName(),
                    transplant
                        .getHashesToTransplant()
                        .get(transplant.getHashesToTransplant().size() - 1))
                .getValue());
    getAccessChecker().canCommitChangeAgainstReference(accessContext, BranchName.of(branchName));
    super.transplantCommitsIntoBranch(branchName, hash, message, transplant);
  }

  @Override
  public void mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    ServerAccessContext accessContext = createAccessContext();
    getAccessChecker()
        .canViewReference(
            accessContext,
            namedRefWithHashOrThrow(merge.getFromRefName(), merge.getFromHash()).getValue());
    getAccessChecker().canCommitChangeAgainstReference(accessContext, BranchName.of(branchName));
    super.mergeRefIntoBranch(branchName, hash, merge);
  }

  @Override
  public Branch commitMultipleOperations(String branch, String hash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    BranchName branchName = BranchName.of(branch);
    ServerAccessContext accessContext = createAccessContext();
    getAccessChecker().canCommitChangeAgainstReference(accessContext, branchName);
    operations
        .getOperations()
        .forEach(
            op -> {
              if (op instanceof Delete) {
                getAccessChecker().canDeleteEntity(accessContext, branchName, op.getKey(), null);
              } else if (op instanceof Put) {
                getAccessChecker().canUpdateEntity(accessContext, branchName, op.getKey(), null);
              }
            });
    return super.commitMultipleOperations(branch, hash, operations);
  }
}
