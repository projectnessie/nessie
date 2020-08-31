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
package com.dremio.nessie.versioned.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.dremio.nessie.backend.TableConverter;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;

/**
 * VersionStore interface for JGit backend.
 */
public class JGitVersionStore<TABLE, METADATA> implements VersionStore<TABLE, METADATA> {

  private final JGitStore<TABLE, METADATA> store;

  /**
   * Construct a JGitVersionStore.
   */
  public JGitVersionStore(StoreWorker<TABLE, CommitMeta> storeWorker, JGitStore<TABLE, METADATA> store) {
    this.store = store;
  }

  @Nonnull
  @Override
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    return store.getRef(ref.getName());
  }

  @Override
  public void commit(BranchName branch, Optional<Hash> expectedHash, METADATA metadata,
                     List<Operation<TABLE>> operations) throws ReferenceNotFoundException, ReferenceConflictException {
    store.getRef(branch.getName());
    boolean shouldMatch = operations.stream().anyMatch(Operation::shouldMatchHash);
    if (shouldMatch && !expectedHash.isPresent()) {
      throw new ReferenceConflictException("Expected hash is not present and at least one operation should match hash");
    } else if (!shouldMatch) {
      throw new UnsupportedOperationException("JGit must always have a matching hash"); //todo relax this in jgit?
    }
    try {
      List<TABLE> tables = new ArrayList<>();
      Set<TABLE> deletes = new HashSet<>();
      Map<TABLE, String> keys = new HashMap<>();
      for (Operation<TABLE> o: operations) {
        if (o instanceof Delete) {
          Delete<TABLE> d = (Delete<TABLE>) o;
          TABLE table = getValue(branch, d.getKey());
          tables.add(table);
          deletes.add(table);
          keys.put(table, stringFromKey(d.getKey()));
        } else if (o instanceof Put) {
          Put<TABLE> p = (Put<TABLE>) o;
          tables.add(p.getValue());
          keys.put(p.getValue(), stringFromKey(p.getKey()));
        } else if (o instanceof Unchanged) {
          Unchanged<TABLE> u = (Unchanged<TABLE>) o;
          TABLE expectedTable = getValue(expectedHash.get(), u.getKey());
          TABLE currentTable = getValue(branch, u.getKey());
          if (!expectedTable.equals(currentTable)) {
            throw new ReferenceConflictException(String.format("Unchanged operation is violated by key: %s", u.getKey()));
          }
        } else {
          throw new UnsupportedOperationException("unknown operation");
        }
      }
      TableConverter<TABLE> tableConverter = new TableConverter<TABLE>() {

        @Override
        public boolean isDeleted(TABLE branchTable) {
          return deletes.contains(branchTable);
        }

        @Override
        public String getId(TABLE branchTable) {
          return keys.get(branchTable);
        }

        @Override
        public String getNamespace(TABLE branchTable) {
          return null; //todo ignore namespace until #53 is addressed
        }
      };
      store.commit(branch.getName(), expectedHash.map(Hash::asString).orElse(null), metadata, tableConverter, tables);
    } catch (NessieConflictException e) {
      throw ReferenceConflictException.forReference(branch, expectedHash, Optional.empty(), e);
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> expectedHash,
                         List<Hash> sequenceToTransplant) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      Hash existingHash = toHash(ref);
      if (expectedHash.isPresent() && !existingHash.equals(expectedHash.get())) {
        throw new ReferenceConflictException(String.format("expected hash %s does not match current hash %s", expectedHash, existingHash));
      }
    } catch (ReferenceNotFoundException e) {
      //ref doesn't exist so create it
      if (expectedHash.isPresent()) {
        throw new ReferenceNotFoundException(String.format("Ref %s does not exist and expected hash does", ref));
      }
      try {
        create(ref, Optional.of(targetHash));
      } catch (ReferenceAlreadyExistsException pass) {
        //can't happen
      }
    }
    try {
      store.updateRef(ref.getName(), expectedHash.map(Hash::asString).orElse(null), targetHash.asString());
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash) throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    if (!targetHash.isPresent() && ref instanceof TagName) {
      throw new ReferenceNotFoundException("You must provide a target hash to create a tag.");
    }
    try {
      store.getRef(ref.getName());
      throw new ReferenceAlreadyExistsException(String.format("ref %s already exists", ref));
    } catch (ReferenceNotFoundException e) {
      //pass expected
    }
    try {
      TableConverter<TABLE> tableConverter = new TableConverter<TABLE>() {

        @Override
        public boolean isDeleted(TABLE branchTable) {
          return false; //assume will never be deleted
        }

        @Override
        public String getId(TABLE branchTable) {
          throw new IllegalStateException(String.format("Should not need id for table: %s while creating a ref", branchTable));
        }

        @Override
        public String getNamespace(TABLE branchTable) {
          return null; //todo ignore namespace until #53 is addressed
        }
      };
      store.createRef(ref.getName(), targetHash.map(Hash::asString).orElse(null), tableConverter);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unknown error while creating %s", ref), e);
    }
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    store.getRef(ref.getName());
    try {
      store.delete(ref.getName(), hash.map(Hash::asString).orElse(null));
    } catch (NessieConflictException e) {
      throw ReferenceConflictException.forReference(ref, hash, Optional.empty(), e);
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    try {
      return store.getRefs();
    } catch (IOException e) {
      throw new RuntimeException("Unknown error", e);
    }
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    if (ref instanceof BranchName) {
      try {
        return store.getCommits(((BranchName) ref).getName());
      } catch (IllegalStateException e) {
        throw new ReferenceNotFoundException(String.format("Ref %s not found", ref), e.getCause());
      } catch (IOException e) {
        throw new RuntimeException("Unknown error", e);
      }
    } else if (ref instanceof TagName) {
      //todo tags
      throw new UnsupportedOperationException("Not yet implemented.");
    } else if (ref instanceof Hash) {
      //todo key
      throw new UnsupportedOperationException("Not yet implemented.");
    } else {
      throw new RuntimeException(String.format("unknown ref type: %s", ref));
    }
  }

  @Override
  public Stream<Key> getKeys(Ref ref) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public TABLE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    if (ref instanceof BranchName) {
      return store.getValue(((BranchName) ref).getName(), stringFromKey(key));
    } else if (ref instanceof TagName) {
      //todo tags
      throw new UnsupportedOperationException("Not yet implemented.");
    } else if (ref instanceof Hash) {
      //todo key
      throw new UnsupportedOperationException("Not yet implemented.");
    } else {
      throw new RuntimeException(String.format("unknown ref type: %s", ref));
    }
  }

  @Override
  public List<Optional<TABLE>> getValue(Ref ref, List<Key> key) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public Collector collectGarbage() {
    throw new IllegalStateException("Not yet implemented.");
  }

  /**
   * URL Encode each portion of the key and join into '/' separated string.
   */
  private static String stringFromKey(Key key) {
    return key.getElements().stream().map(k -> {
      try {
        return URLEncoder.encode(k, StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(String.format("Unable to encode key %s", key), e);
      }
    }).collect(Collectors.joining("/"));
  }
}
