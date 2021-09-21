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

import static org.projectnessie.model.Operation.Delete;
import static org.projectnessie.model.Operation.Put;

import java.security.AccessControlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Default;
import javax.inject.Inject;
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
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.authz.ServerAccessContext;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;

/** Does authorization checks (if enabled) on the {@link TreeResource} endpoints. */
@RequestScoped
@Default
public class TreeResourceWithAuthorizationChecks extends TreeResource {

  @Inject
  public TreeResourceWithAuthorizationChecks(
      ServerConfig config,
      VersionStore<Contents, CommitMeta, Type> store,
      AccessChecker accessChecker) {
    super(config, store, accessChecker);
  }

  @Override
  public List<Reference> getAllReferences() {
    List<Reference> allReferences = super.getAllReferences();
    ServerAccessContext accessContext = createAccessContext();
    return allReferences.stream()
        .filter(
            ref -> {
              try {
                if (ref instanceof Branch) {
                  getAccessChecker().canViewReference(accessContext, BranchName.of(ref.getName()));
                } else if (ref instanceof Tag) {
                  getAccessChecker().canViewReference(accessContext, TagName.of(ref.getName()));
                }
                return true;
              } catch (AccessControlException e) {
                return false;
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public Reference getReferenceByName(String refName) throws NessieNotFoundException {
    Reference ref = super.getReferenceByName(refName);
    if (ref instanceof Branch) {
      getAccessChecker().canViewReference(createAccessContext(), BranchName.of(ref.getName()));
    } else if (ref instanceof Tag) {
      getAccessChecker().canViewReference(createAccessContext(), TagName.of(ref.getName()));
    }
    return ref;
  }

  @Override
  public Reference createReference(@Nullable String sourceRefName, Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    if (reference instanceof Branch) {
      getAccessChecker()
          .canCreateReference(createAccessContext(), BranchName.of(reference.getName()));
    } else if (reference instanceof Tag) {
      getAccessChecker().canCreateReference(createAccessContext(), TagName.of(reference.getName()));
    }

    namedRefWithHashOrThrow(sourceRefName, reference.getHash());
    return super.createReference(sourceRefName, reference);
  }

  protected void checkHashOnRef(@Nullable String refName, @Nullable String hash)
      throws NessieNotFoundException {
    System.out.println("refName = " + refName);
    System.out.println("hash = " + hash);
    if (refName == null) {
      refName = getConfig().getDefaultBranch();
    }

    try {
      NamedRef sourceRef;
      try {
        Ref ref = getStore().toRef(refName).getValue();
        if (!(ref instanceof NamedRef)) {
          throw new ReferenceNotFoundException(
              String.format("Named reference '%s' not found", refName));
        }
        sourceRef = (NamedRef) ref;
      } catch (ReferenceNotFoundException e) {
        // Special case, mostly for tests:
        // When the default branch is deleted, we cannot verify whether the branch already exists
        // (it does not, so the check would fail).
        System.out.println("EX refName = " + refName);
        System.out.println("EX hash = " + hash);
        System.out.println("getStore().noAncestorHash() = " + getStore().noAncestorHash());

        if (Objects.equals(getConfig().getDefaultBranch(), refName) && hash == null
            || Objects.equals(getStore().noAncestorHash().asString(), hash)) {
          // prevent the "hash-on-ref" check, but still do the "can-view" check
          hash = null;
          return;
        } else {
          throw e;
        }
      }
      getAccessChecker().canViewReference(createAccessContext(), sourceRef);
      if (hash != null) {
        getStore().hashOnReference(sourceRef, Optional.of(Hash.of(hash)));
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(e.getMessage());
    }
  }

  @Override
  protected void assignReference(NamedRef ref, String oldHash, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    namedRefWithHashOrThrow(assignTo.getName(), assignTo.getHash());

    getAccessChecker().canAssignRefToHash(createAccessContext(), ref);
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
    if (null == params.pageToken()) {
      // we should only allow named references when no paging is defined
      getAccessChecker()
          .canListCommitLog(
              createAccessContext(),
              namedRefWithHashOrThrow(namedRef, params.endHash()).getValue());
    }
    return super.getCommitLog(namedRef, params);
  }

  @Override
  public void transplantCommitsIntoBranch(
      String branchName, String hash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    if (transplant.getHashesToTransplant().isEmpty()) {
      throw new IllegalArgumentException("No hashes given to transplant.");
    }

    namedRefWithHashOrThrow(
        transplant.getFromRefName(),
        transplant.getHashesToTransplant().get(transplant.getHashesToTransplant().size() - 1));
    getAccessChecker()
        .canCommitChangeAgainstReference(createAccessContext(), BranchName.of(branchName));
    super.transplantCommitsIntoBranch(branchName, hash, message, transplant);
  }

  @Override
  public void mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    getAccessChecker()
        .canCommitChangeAgainstReference(createAccessContext(), BranchName.of(branchName));
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
