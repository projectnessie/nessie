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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;

/**
 * VersionStore interface for JGit backend.
 */
public class JGitVersionStore<TABLE, METADATA> implements VersionStore<TABLE, METADATA> {

  private final JGitStore<TABLE, METADATA> store;
  private final Serializer<TABLE> serializer;
  private final Serializer<CommitMeta> metadataSerializer;

  /**
   * Construct a JGitVersionStore.
   */
  public JGitVersionStore(StoreWorker<TABLE, CommitMeta> storeWorker, JGitStore<TABLE, METADATA> store) {
    this.store = store;
    this.serializer = storeWorker.getValueSerializer();
    this.metadataSerializer = storeWorker.getMetadataSerializer();
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
    try {
      List<TABLE> tables = new ArrayList<>();
      Map<TABLE, Operation<TABLE>> reverseMap = new HashMap<>();
      for (Operation<TABLE> o: operations) {
        if (o instanceof Delete) {
          Delete<TABLE> d = (Delete<TABLE>) o;
          TABLE table = getValue(branch, d.getKey());
          tables.add(table);
          reverseMap.put(table, d);
        } else if (o instanceof Put) {
          Put<TABLE> p = (Put<TABLE>) o;
          tables.add(p.getValue());
          reverseMap.put(p.getValue(), p);
        }
        //todo assert unchanged
      }
      TableConverter<TABLE> tableConverter = new TableConverter<TABLE>() {
        @Override
        public long getUpdateTime(TABLE table) {
          return 0; //todo not really needed anymore
        }

        @Override
        public boolean isDeleted(TABLE branchTable) {
          return reverseMap.get(branchTable) instanceof Delete;
        }

        @Override
        public String getId(TABLE branchTable) {
          return null; //todo
        }

        @Override
        public String getNamespace(TABLE branchTable) {
          return null; //todo
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
    //todo
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
        public long getUpdateTime(TABLE o) {
          return 0; //todo not needed, remove
        }

        @Override
        public boolean isDeleted(TABLE branchTable) {
          return false; //assume will never be deleted
        }

        @Override
        public String getId(TABLE branchTable) {
          return null; //todo
        }

        @Override
        public String getNamespace(TABLE branchTable) {
          return null; //todo
        }
      };
      store.createRef(ref.getName(), targetHash.map(Hash::asString).orElse(null), tableConverter);//todo tableConverter is null?
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
    return null; //todo
  }

  @Override
  public Stream<Key> getKeys(Ref ref) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public TABLE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    if (ref instanceof BranchName) {
      return store.getValue(((BranchName) ref).getName(), key.toString());
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
}
