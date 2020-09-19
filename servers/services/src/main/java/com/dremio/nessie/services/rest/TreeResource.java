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
import com.dremio.nessie.error.NessieAlreadyExistsException;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents.Type;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableHash;
import com.dremio.nessie.model.ImmutableLogResponse;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.Merge;
import com.dremio.nessie.model.NessieObjectKey;
import com.dremio.nessie.model.ObjectsResponse;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.ReferenceUpdate;
import com.dremio.nessie.model.Tag;
import com.dremio.nessie.model.Transplant;
import com.dremio.nessie.services.config.ServerConfig;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.TagName;
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
      throw new NessieNotFoundException(e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-get-defaultbranch")
  @Override
  public Branch getDefaultBranch() {
    Reference r = getReferenceByName(config.getDefaultBranch());
    if (!(r instanceof Branch)) {
      throw new IllegalStateException("Default branch isn't a branch");
    }
    return (Branch) r;
  }

  @Metered
  @Timed(name = "timed-tree-create")
  @Override
  public void createNewReference(Reference reference)
      throws NessieAlreadyExistsException, NessieNotFoundException, NessieConflictException {
    try {
      if (reference instanceof Branch) {
        store.create(BranchName.of(reference.getName()), toHash(reference, false));
      } else if (reference instanceof Tag) {
        store.create(TagName.of(reference.getName()), toHash(reference, true));
      } else {
        throw new IllegalArgumentException("Can only create a new reference for branch and tag types.");
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(reference.getName(), e);
    } catch (ReferenceAlreadyExistsException e) {
      throw new NessieAlreadyExistsException(e);
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

  @Metered
  @Timed(name = "timed-tree-delete")
  @Override
  public void deleteReference(Reference reference) throws NessieConflictException, NessieNotFoundException {
    try {
      if (reference instanceof Branch) {
        store.delete(BranchName.of(reference.getName()), toHash(reference, true));
      } else if (reference instanceof Tag) {
        store.delete(TagName.of(reference.getName()), toHash(reference, true));
      } else {
        throw new IllegalArgumentException("Can only delete branch and tag types.");
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(reference.getName(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-assign")
  @Override
  public void assignReference(String ref, ReferenceUpdate reference)
      throws NessieNotFoundException, NessieConflictException {
    try {
      WithHash<Ref> resolved = store.toRef(ref);
      Ref resolvedRef = resolved.getValue();
      if (resolvedRef instanceof NamedRef) {
        store.assign((NamedRef) resolvedRef, toHash(reference.getExpectedId(), true), toHash(reference.getUpdateId(), false)
            .orElseThrow(() -> new NessieConflictException("Must provide target hash value for operation.")));
      } else {
        throw new IllegalArgumentException("Can only assign branch and tag types.");
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(ref, e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(e);
    }
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
      throw new NessieNotFoundException(e);
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
      throw new NessieNotFoundException(transplant.getBranch().getName(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-merge")
  @Override
  public void mergeRefIntoBranch(String branchName, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    try {
      store.merge(toHash(merge.getFromHash(), true).get(), BranchName.of(merge.getTo().getName()), toHash(merge.getTo(), true));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(e);
    }
  }

  @Metered
  @Timed(name = "timed-tree-names")
  @Override
  public ObjectsResponse getObjects(String refName) throws NessieNotFoundException {
    final Hash hash = getHashOrThrow(refName);
    try {
      List<ObjectsResponse.Entry> entries = store.getKeys(hash)
          .map(key -> ObjectsResponse.Entry.builder().name(fromKey(key)).type(Type.UNKNOWN).build())
          .collect(ImmutableList.toImmutableList());
      return ObjectsResponse.builder().addAllEntries(entries).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(refName, e);
    }
  }

  private static NessieObjectKey fromKey(Key key) {
    return new NessieObjectKey(key.getElements());
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

}
