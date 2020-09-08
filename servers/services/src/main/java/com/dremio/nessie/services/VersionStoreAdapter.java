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
package com.dremio.nessie.services;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableHash;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;

/**
 * Adapter to help translate REST calls into version store calls.
 *
 * <p>This is used as a translation layer between the REST api and the version store.</p>
 */
@ApplicationScoped
public class VersionStoreAdapter {
  private static final String SLASH = "/";

  private final VersionStore<Table, CommitMeta> versionStore;

  @Inject
  public VersionStoreAdapter(VersionStore<Table, CommitMeta> versionStore) {
    this.versionStore = versionStore;
  }

  /**
   * Return all refs in the store and convert them to model References.
   */
  public List<Reference> getAllReferences() {
    return versionStore.getNamedRefs().map(VersionStoreAdapter::makeRef).collect(Collectors.toList());
  }

  /**
   * Get all keys for a ref. This first checks if a reference exists then tries to return all tables on that Ref.
   *
   * <p>
   *   note: the keys are looked up based on the Hash as opposed to Tag or Branch. Any changes that happen between finding the Hash
   *   and iterating the keys will not be visible.
   * </p>
   *
   * @param refName name of ref to find. Could be Branch, Tag or Hash
   * @return stream of key objects from data store
   * @throws ReferenceNotFoundException if ref is not found
   */
  public Stream<Key> getAllTables(String refName) throws ReferenceNotFoundException {
    Optional<Hash> hash = getHash(refName);
    if (!hash.isPresent()) {
      throw notFound(refName);
    }
    try {
      return versionStore.getKeys(hash.get());
    } catch (ReferenceNotFoundException e) {
      //can't happen - we just built the hash above
      throw new IllegalStateException(String.format("Unable to find keys for ref %s", hash.get()), e);
    }
  }

  /**
   * get a Reference for a string. If a Hash it must be a valid hash.
   *
   * @param refName name of reference (hash, branch, tag)
   * @return Reference object
   * @throws ReferenceNotFoundException refName not found in data store
   */
  public Reference getReference(String refName) throws ReferenceNotFoundException {
    WithHash<Ref> ref = getRef(refName).orElseThrow(() -> notFound(refName));
    if (ref.getValue() instanceof BranchName) {
      return ImmutableBranch.builder().id(ref.getHash().asString()).name(((BranchName) ref.getValue()).getName()).build();
    }
    if (ref.getValue() instanceof TagName) {
      return ImmutableTag.builder().id(ref.getHash().asString()).name(((TagName) ref.getValue()).getName()).build();
    }
    String hash = ref.getHash().asString();
    return ImmutableHash.builder().id(hash).build();
  }


  /**
   * Get a table for a particular Reference.
   * @param refName name of Hash, Tag or Branch to get table from
   * @param table fully qualified table name URL encoded and / delimited
   * @return table object for refName or null if not found
   * @throws ReferenceNotFoundException if refName is not in version store
   */
  public Table getTableOnReference(String refName, String table) throws ReferenceNotFoundException {
    Hash ref = getHash(refName).orElseThrow(() -> new ReferenceNotFoundException(String.format("Ref for %s not found", refName)));
    return versionStore.getValue(ref, keyFromUrlString(table));
  }

  /**
   * Create a branch or tag off of the referenced Hash.
   * @param refName name of branch/tag to create
   * @param targetHash targetHash on which the branch/tag is based. If null will be based off of start of history
   * @param type create a tag or a branch
   * @return the newly created branch/tag
   * @throws ReferenceNotFoundException targetHash is not a valid targetHash in this data store
   * @throws ReferenceAlreadyExistsException the branch/tag with name refName already exists
   */
  public Reference createRef(String refName, @Nullable String targetHash, String type)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    NamedRef ref;
    if (type.equals("TAG")) {
      ref = TagName.of(refName);
    } else if (type.equals("BRANCH")) {
      ref = BranchName.of(refName);
    } else {
      throw new IllegalStateException(String.format("Unknown refName type: %s", type));
    }
    versionStore.create(ref, Optional.ofNullable(targetHash).map(Hash::of));
    try {
      Hash newHash = versionStore.toHash(ref);
      if (type.equals("TAG")) {
        return ImmutableTag.builder().id(newHash.asString()).name(refName).build();
      } else {
        return ImmutableBranch.builder().id(newHash.asString()).name(refName).build();
      }
    } catch (ReferenceNotFoundException e) {
      //cant happen
      throw new IllegalStateException(String.format("Freshly created refName %s no longer exists", ref), e);
    }
  }

  /**
   * Delete the ref with name refName.
   * @param refName name of branch or tag to delete
   * @param expectedHash expected hash. for optimistic concurrency
   * @throws ReferenceNotFoundException refName does not exsit
   * @throws ReferenceConflictException expectedHash is out of date
   */
  public void deleteRef(String refName, String expectedHash) throws ReferenceNotFoundException, ReferenceConflictException {
    NamedRef ref = getRef(refName).map(WithHash::getValue).map(x -> x instanceof NamedRef ? (NamedRef) x : null)
                                  .orElseThrow(() -> notFound(refName));
    versionStore.delete(ref, Optional.ofNullable(expectedHash).map(Hash::of));
  }

  /**
   * Commit a change or set of changes to a branch.
   * @param branch branch to commit to
   * @param expectedHash expected hash. for optimistic concurrency
   * @param meta commit metadata for audit
   * @param ops operations to perform in this atomic commit
   * @throws ReferenceNotFoundException if branch is not an existing branch
   * @throws ReferenceConflictException if expectedHash is not up to date
   */
  public void commit(String branch, String expectedHash, CommitMeta meta, List<Operation<Table>> ops)
      throws ReferenceNotFoundException, ReferenceConflictException {
    versionStore.commit(BranchName.of(branch), Optional.ofNullable(expectedHash).map(Hash::of), meta, ops);
  }

  /**
   * Assign a Branch to a target commit.
   *
   * @param refName name of branch to assign. This will be created if it doesn't exist
   * @param expectedHash expected hash. for optimistic concurrency
   * @param target hash of target to assign this branch to
   * @throws ReferenceNotFoundException target doesn't exist in data store
   * @throws ReferenceConflictException expected hash is not up to date with current refName
   */
  public void assign(String refName, String expectedHash, String target) throws ReferenceNotFoundException, ReferenceConflictException {
    NamedRef ref = getRef(refName).map(WithHash::getValue).map(x -> x instanceof NamedRef ? (NamedRef) x : null)
                                  .orElseThrow(() -> notFound(refName));
    versionStore.assign(ref, Optional.ofNullable(expectedHash).map(Hash::of), Hash.of(target));
  }

  /**
   * merge a branch or hash onto a target branch.
   *
   * @param targetRef branch name to add the commit hashes to
   * @param fromHash hash onto which targetRef should be merged
   * @param expectedHash expected hash. for optimistic concurrency
   * @throws ReferenceConflictException if expectedHash doesn't match current hash of targetRef
   * @throws ReferenceNotFoundException targetRef or fromHash don't exist
   */
  public void merge(String targetRef, String fromHash, String expectedHash) throws ReferenceConflictException, ReferenceNotFoundException {
    BranchName target = getRef(targetRef).map(WithHash::getValue)
                                         .map(x -> x instanceof BranchName ? (BranchName) x : null)
                                         .orElseThrow(() -> notFound(targetRef));
    versionStore.merge(Hash.of(fromHash), target, Optional.ofNullable(expectedHash).map(Hash::of));
  }

  /**
   * transplant a list of hashes onto a target branch.
   *
   * @param targetRef branch name to add the commit hashes to
   * @param expectedHash expected hash. for optimistic concurrency
   * @param transplantNames list of hashes in string format for tranplanting
   * @throws ReferenceConflictException if expectedHash doesn't match current hash of targetRef
   * @throws ReferenceNotFoundException targetRef doesn't exist
   */
  public void transplant(String targetRef, String expectedHash, List<String> transplantNames)
      throws ReferenceConflictException, ReferenceNotFoundException {
    List<Hash> transplants = transplantNames.stream().map(Hash::of).collect(Collectors.toList());
    BranchName target = getRef(targetRef).map(WithHash::getValue).map(x -> x instanceof BranchName ? (BranchName) x : null)
                                  .orElseThrow(() -> notFound(targetRef));
    versionStore.transplant(target, Optional.ofNullable(expectedHash).map(Hash::of), transplants);
  }

  public Map<String, CommitMeta> log(String refName) throws ReferenceNotFoundException {
    Hash hash = getHash(refName).orElseThrow(() -> notFound(refName));
    return versionStore.getCommits(hash).collect(Collectors.toMap(x -> x.getHash().asString(), WithHash::getValue));
  }

  private static ReferenceNotFoundException notFound(String ref) {
    return new ReferenceNotFoundException(String.format("Reference %s not found", ref));
  }

  private static Reference makeRef(WithHash<NamedRef> refWithHash) {
    NamedRef ref = refWithHash.getValue();
    //todo do we want to send back Hash object or the string. I don't want internal API escaping so maybe an external representation of hash
    if (ref instanceof TagName) {
      return ImmutableTag.builder().name(ref.getName()).id(refWithHash.getHash().asString()).build();
    } else if (ref instanceof BranchName) {
      return ImmutableBranch.builder().name(ref.getName()).id(refWithHash.getHash().asString()).build();
    } else {
      throw new UnsupportedOperationException("only converting tags or branches"); //todo
    }
  }

  /**
   * URL Encode each portion of the key and join into '/' separated string.
   */
  public static Key keyFromUrlString(String path) {
    return Key.of(StreamSupport.stream(Arrays.spliterator(path.split(SLASH)), false)
                               .map(x -> {
                                 try {
                                   return URLDecoder.decode(x, StandardCharsets.UTF_8.toString());
                                 } catch (UnsupportedEncodingException e) {
                                   throw new RuntimeException(String.format("Unable to decode string %s", x), e);
                                 }
                               })
                               .toArray(String[]::new));
  }

  /**
   * URL Encode each portion of the key and join into '/' separated string.
   */
  public static String urlStringFromKey(Key key) {
    return key.getElements().stream().map(x -> {
      try {
        return URLEncoder.encode(x, StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(String.format("Unable to decode string %s", x), e);
      }
    }).collect(Collectors.joining(SLASH));
  }

  /**
   * Go from string to known hash.
   *
   * <p>
   *   Following the pattern in JGit findRefs from a string by trying all possibilities in order.
   * </p>
   *
   * @param name branch, tag, hash name
   * @return hash of name. Empty if not found
   */
  private Optional<Hash> getHash(String name) {
    Hash hash = null;
    try {
      BranchName branch = BranchName.of(name);
      hash = versionStore.toHash(branch);
    } catch (ReferenceNotFoundException e) {
      //pass no branch
    }
    try {
      TagName tag = TagName.of(name);
      hash = versionStore.toHash(tag);
    } catch (ReferenceNotFoundException e) {
      //pass no tag
    }
    try {
      hash = Hash.of(name);
    } catch (IllegalArgumentException e) {
      //pass not a valid hash
    }
    return Optional.ofNullable(hash);
  }

  /**
   * Go from string to known ref.
   *
   * <p>
   *   Following the pattern in JGit findRefs from a string by trying all possibilities in order.
   * </p>
   *
   * @param name branch, tag, hash name
   * @return converted ref or emtpy if doesn't exist
   */
  private Optional<WithHash<Ref>> getRef(String name) {
    try {
      BranchName branch = BranchName.of(name);
      Hash hash = versionStore.toHash(branch);
      return Optional.of(WithHash.of(hash, branch));
    } catch (ReferenceNotFoundException e) {
      //pass no branch
    }
    try {
      TagName tag = TagName.of(name);
      Hash hash = versionStore.toHash(tag);
      return Optional.of(WithHash.of(hash, tag));
    } catch (ReferenceNotFoundException e) {
      //pass no tag
    }
    try {
      Hash ref = Hash.of(name);
      return Optional.of(WithHash.of(ref, ref));
    } catch (IllegalArgumentException e) {
      //pass not a valid hash
    }
    return Optional.empty();
  }

}
