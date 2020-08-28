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
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Table;
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

public class JGitVersionStore implements VersionStore<Table, CommitMeta> {

  private final JGitStore store;
  private final Serializer<Table> serializer;
  private final Serializer<CommitMeta> metadataSerializer;

  public JGitVersionStore(StoreWorker<Table, CommitMeta> storeWorker, JGitStore store) {
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
  public void commit(BranchName branch, Optional<Hash> expectedHash, CommitMeta metadata,
                     List<Operation<Table>> operations) throws ReferenceNotFoundException, ReferenceConflictException {
    store.getRef(branch.getName());
    try {
      List<Table> tables = new ArrayList<>();
      for (Operation<Table> o: operations) {
        if (o instanceof Delete) {
          Delete<Table> d = (Delete<Table>) o;
          Table table = getValue(branch, d.getKey());
          tables.add(ImmutableTable.copyOf(table).withIsDeleted(true));
        } else if (o instanceof Put) {
          Put<Table> p = (Put<Table>) o;
          tables.add(p.getValue());
        }
      }
      store.commit(branch.getName(), expectedHash.map(Hash::asString).orElse(null), metadata, tables.toArray(new Table[0]));
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e.getMessage());
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
      store.createRef(ref.getName(), targetHash.map(Hash::asString).orElse(null));
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
      throw new ReferenceConflictException(e.getMessage());
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
  public Stream<WithHash<CommitMeta>> getCommits(Ref ref) throws ReferenceNotFoundException {
    return null; //todo
  }

  @Override
  public Stream<Key> getKeys(Ref ref) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public Table getValue(Ref ref, Key key) throws ReferenceNotFoundException {
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
  public List<Optional<Table>> getValue(Ref ref, List<Key> key) {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public Collector collectGarbage() {
    throw new IllegalStateException("Not yet implemented.");
  }
}
