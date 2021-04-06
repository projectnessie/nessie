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
package org.projectnessie.versioned.impl;

import static org.mockito.Mockito.spy;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;
import org.projectnessie.versioned.store.Store;

public abstract class AbstractTieredStoreFixture<S extends Store, C> implements VersionStore<String, String, StringSerializer.TestEnum>,
    AutoCloseable {
  protected static final StoreWorker<String, String, StringSerializer.TestEnum> WORKER =
      StoreWorker.of(StringSerializer.getInstance(), StringSerializer.getInstance());

  private final C config;

  private final S store;
  private final VersionStore<String, String, StringSerializer.TestEnum> versionStore;

  /**
   * Create a new fixture.
   */
  protected AbstractTieredStoreFixture(C config) {
    this.config = config;
    S storeImpl = createStoreImpl();
    storeImpl.start();

    store = spy(storeImpl);

    VersionStore<String, String, StringSerializer.TestEnum> versionStoreImpl = new TieredVersionStore<>(WORKER, store,
        ImmutableTieredVersionStoreConfig.builder()
            .enableTracing(true)
            .waitOnCollapse(true)
            .build());
    versionStore = spy(versionStoreImpl);
  }

  public C getConfig() {
    return config;
  }

  public abstract S createStoreImpl();

  public S getStore() {
    return store;
  }

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    return versionStore.toHash(ref);
  }

  @Override
  public void commit(BranchName branch, Optional<Hash> expectedHash, String metadata,
      List<Operation<String>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    versionStore.commit(branch, expectedHash, metadata, operations);
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> expectedHash,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    versionStore.transplant(targetBranch, expectedHash, sequenceToTransplant);
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    versionStore.merge(fromHash, toBranch, expectedHash);
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    versionStore.assign(ref, expectedHash, targetHash);
  }

  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    versionStore.create(ref, targetHash);
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    versionStore.delete(ref, hash);
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return versionStore.getNamedRefs();
  }

  @Override
  public Stream<WithHash<String>> getCommits(Ref ref) throws ReferenceNotFoundException {
    return versionStore.getCommits(ref);
  }

  @Override
  public Stream<WithType<Key, StringSerializer.TestEnum>> getKeys(Ref ref) throws ReferenceNotFoundException {
    return versionStore.getKeys(ref);
  }

  @Override
  public String getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return versionStore.getValue(ref, key);
  }

  @Override
  public List<Optional<String>> getValues(Ref ref, List<Key> key)
      throws ReferenceNotFoundException {
    return versionStore.getValues(ref, key);
  }

  @Override
  public WithHash<Ref> toRef(String refOfUnknownType) throws ReferenceNotFoundException {
    return versionStore.toRef(refOfUnknownType);
  }

  @Override
  public Stream<Diff<String>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    return versionStore.getDiffs(from, to);
  }

  @Override
  public Collector collectGarbage() {
    return versionStore.collectGarbage();
  }
}
