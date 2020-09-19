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

package com.dremio.nessie.services.rest;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.metrics.annotation.Metered;
import org.eclipse.microprofile.metrics.annotation.Timed;

import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.Contents.Type;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableHash;
import com.dremio.nessie.model.ImmutableLogResponse;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.Merge;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.Operation;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.Transplant;
import com.dremio.nessie.services.config.ServerConfig;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.WithHash;
import com.google.common.collect.ImmutableList;

/**
 * REST endpoint for trees.
 */
@RequestScoped
public class TreeResource extends BaseResource implements TreeApi {

  @Inject
  private ServerConfig config;

  @Metered
  @Timed(name = "timed-tree-all")
  @Override
  public List<Reference> getAllReferences() {
    return store.getNamedRefs().map(TreeResource::makeNamedRef).collect(Collectors.toList());
  }

  @Metered
  @Timed(name = "timed-tree-get-byname")
  @Override
  public Reference getReferenceByName(String refName) throws NessieNotFoundException {
    try {
      return makeRef(store.toRef(refName));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(String.format("Unable to find reference [%s].", refName), e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-get-defaultbranch")
  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    Reference r = getReferenceByName(config.getDefaultBranch());
    if (!(r instanceof Branch)) {
      throw new IllegalStateException("Default branch isn't a branch");
    }
    return (Branch) r;
  }

  @Metered
  @Timed(name = "timed-tree-tag")
  public void createNewTag(String tagName, String hash) throws NessieNotFoundException, NessieConflictException {
    createNewReference(TagName.of(tagName), hash);
  }

  @Metered
  @Timed(name = "timed-assign-tag")
  @Override
  public void assignTag(String tagName, String oldHash, String newHash)
      throws NessieNotFoundException, NessieConflictException {
    assignReference(TagName.of(tagName), oldHash, newHash);
  }

  @Metered
  @Timed(name = "timed-delete-tag")
  @Override
  public void deleteTag(String tagName, String hash) throws NessieConflictException, NessieNotFoundException {
    deleteReference(TagName.of(tagName), hash);
  }

  @Metered
  @Timed(name = "timed-create-branch")
  @Override
  public void createNewBranch(String branchName, String hash) throws NessieNotFoundException, NessieConflictException {
    createNewReference(BranchName.of(branchName), hash);
  }

  @Metered
  @Timed(name = "timed-assign-branch")
  @Override
  public void assignBranch(String branchName, String oldHash, String newHash)
      throws NessieNotFoundException, NessieConflictException {
    assignReference(BranchName.of(branchName), oldHash, newHash);
  }

  @Metered
  @Timed(name = "timed-delete-branch")
  @Override
  public void deleteBranch(String branchName, String hash) throws NessieConflictException, NessieNotFoundException {
    deleteReference(BranchName.of(branchName), hash);
  }

  @Metered
  @Timed(name = "timed-tree-log")
  @Override
  public LogResponse getCommitLog(String ref) throws NessieNotFoundException {
    // TODO: pagination.
    Hash hash = getHashOrThrow(ref);
    try {
      List<CommitMeta> items = store.getCommits(hash)
          .map(cwh -> cwh.getValue().toBuilder().hash(cwh.getHash().asString()).build()).collect(Collectors.toList());
      return ImmutableLogResponse.builder().addAllOperations(items).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(String.format("Unable to find the requested ref [%s].", ref), e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-transplant")
  @Override
  public void transplantCommitsIntoBranch(String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    try {
      List<Hash> transplants = transplant.getHashesToTransplant().stream().map(Hash::of).collect(Collectors.toList());
      store.transplant(BranchName.of(transplant.getBranch().getName()), toHash(transplant.getBranch(), true), transplants);
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format("Unable to find the requested branch we're transplanting to of [%s].", transplant.getBranch().getName()), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format("The hash provided %s does not match the current status of the branch %s.",
              transplant.getBranch().getHash(), transplant.getBranch().getName()), e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-merge")
  @Override
  public void mergeRefIntoBranch(Merge merge) throws NessieNotFoundException, NessieConflictException {
    try {
      store.merge(toHash(merge.getFromHash(), true).get(), BranchName.of(merge.getTo().getName()), toHash(merge.getTo(), true));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(String.format("At least one of the references provided does not exist."), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format("The branch [%s] does not have the expected hash [%s].", merge.getTo().getName(), merge.getTo().getHash()), e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-names")
  @Override
  public EntriesResponse getEntries(String refName) throws NessieNotFoundException {
    final Hash hash = getHashOrThrow(refName);
    try {
      List<EntriesResponse.Entry> entries = store.getKeys(hash)
          .map(key -> EntriesResponse.Entry.builder().name(fromKey(key)).type(Type.UNKNOWN).build())
          .collect(ImmutableList.toImmutableList());
      return EntriesResponse.builder().addAllEntries(entries).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(String.format("Unable to find the reference [%s].", refName), e);
    }
  }

  @Override
  public void commitMultipleOperations(String hash, String message, MultiContents operations)
      throws NessieNotFoundException, NessieConflictException {
    commitMultipleOperations(config.getDefaultBranch(), hash, message, operations);
  }

  @Metered
  @Timed(name = "timed-contents-multi")
  @Override
  public void commitMultipleOperations(String branch, String hash, String message, MultiContents operations)
      throws NessieNotFoundException, NessieConflictException {
    List<com.dremio.nessie.versioned.Operation<Contents>> ops = operations.getOperations()
        .stream()
        .map(TreeResource::toOp)
        .collect(ImmutableList.toImmutableList());
    doOps(branch, hash, message, ops);
  }

  void doOps(String branch,
      String hash, String message, List<com.dremio.nessie.versioned.Operation<Contents>> operations)
      throws NessieConflictException, NessieNotFoundException {
    try {
      store.commit(
          BranchName.of(branch),
          Optional.of(Hash.of(hash)),
          ContentsResource.meta(principal, message),
          operations);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException("Failed to commit data. Provided hash does not match current value.", e);
    } catch (ReferenceNotFoundException e) {
      throw new NessieConflictException("Failed to commit data. Provided ref was not found.", e);
    }
  }

  private void createNewReference(NamedRef reference, String hash) throws NessieNotFoundException, NessieConflictException {
    try {
      store.create(reference, toHash(hash, false));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(reference.getName(), e);
    } catch (ReferenceAlreadyExistsException e) {
      throw new NessieConflictException(String.format("A reference of name [%s] already exists.", reference.getName()), e);
    }
  }

  private static Optional<Hash> toHash(Reference reference, boolean required) throws NessieConflictException {
    return toHash(reference.getHash(), required);
  }

  private static Optional<Hash> toHash(String hash, boolean required) throws NessieConflictException {
    if (hash == null) {
      if (required) {
        throw new NessieConflictException("Must provide expected hash value for operation.");
      }
      return Optional.empty();
    }
    return Optional.of(Hash.of(hash));
  }

  private void deleteReference(NamedRef name, String hash) throws NessieConflictException, NessieNotFoundException {
    try {
      store.delete(name, toHash(hash, true));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(String.format("Unable to find reference [%s] to delete.", name.getName()), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format("The hash provided %s does not match the current status of the reference %s.",
              hash, name.getName()), e);
    }
  }

  private void assignReference(NamedRef ref, String oldHash, String newHash)
      throws NessieNotFoundException, NessieConflictException {
    try {
      WithHash<Ref> resolved = store.toRef(ref.getName());
      Ref resolvedRef = resolved.getValue();
      if (resolvedRef instanceof NamedRef) {
        store.assign((NamedRef) resolvedRef, toHash(oldHash, true), toHash(oldHash, true)
            .orElseThrow(() -> new NessieConflictException("Must provide target hash value for operation.")));
      } else {
        throw new IllegalArgumentException("Can only assign branch and tag types.");
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException("Unable to find a ref or hash provided.", e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format("The hash provided %s does not match the current status of the reference %s.",
              oldHash, ref), e);
    }
  }

  private static ContentsKey fromKey(Key key) {
    return new ContentsKey(key.getElements());
  }

  private static Reference makeNamedRef(WithHash<NamedRef> refWithHash) {
    return makeRef(refWithHash);
  }

  private static Reference makeRef(WithHash<? extends Ref> refWithHash) {
    Ref ref = refWithHash.getValue();
    //todo do we want to send back Hash object or the string. I don't want internal API escaping so maybe an external representation of hash
    if (ref instanceof TagName) {
      return ImmutableTag.builder().name(((NamedRef)ref).getName()).hash(refWithHash.getHash().asString()).build();
    } else if (ref instanceof BranchName) {
      return ImmutableBranch.builder().name(((NamedRef)ref).getName()).hash(refWithHash.getHash().asString()).build();
    } else if (ref instanceof Hash) {
      String hash = refWithHash.getHash().asString();
      return ImmutableHash.builder().name(hash).build();
    } else {
      throw new UnsupportedOperationException("only converting tags or branches"); //todo
    }
  }

  private static com.dremio.nessie.versioned.Operation<Contents> toOp(Operation o) {
    Key key = Key.of(o.getKey().getElements().toArray(new String[0]));
    if (o instanceof Operation.Delete) {
      return Delete.of(key);
    } else if (o instanceof Operation.Put) {
      return Put.of(key, ((Operation.Put)o).getContents());
    } else if (o instanceof Operation.Unchanged) {
      return Unchanged.of(key);
    } else {
      throw new IllegalStateException("Unknown operation " + o);
    }
  }
}
