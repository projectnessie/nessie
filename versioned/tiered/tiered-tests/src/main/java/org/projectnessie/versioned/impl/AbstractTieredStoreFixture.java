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
import javax.annotation.Nonnull;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.StringSerializer.TestEnum;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;
import org.projectnessie.versioned.store.Store;

public abstract class AbstractTieredStoreFixture<S extends Store, C>
    implements VersionStore<String, String, StringSerializer.TestEnum>, AutoCloseable {
  protected static final StoreWorker<String, String, StringSerializer.TestEnum> WORKER =
      StoreWorker.of(StringSerializer.getInstance(), StringSerializer.getInstance());

  private final C config;

  private final S store;
  private final VersionStore<String, String, StringSerializer.TestEnum> versionStore;

  /** Create a new fixture. */
  protected AbstractTieredStoreFixture(C config) {
    this.config = config;
    S storeImpl = createStoreImpl();
    storeImpl.start();

    store = spy(storeImpl);

    VersionStore<String, String, StringSerializer.TestEnum> versionStoreImpl =
        new TieredVersionStore<>(
            WORKER,
            store,
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
  @Nonnull
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    return versionStore.toHash(ref);
  }

  @Override
  public Hash commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> expectedHash,
      @Nonnull String metadata,
      @Nonnull List<Operation<String>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return versionStore.commit(branch, expectedHash, metadata, operations);
  }

  @Override
  public Hash transplant(
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      NamedRef source,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return versionStore.transplant(targetBranch, referenceHash, source, sequenceToTransplant);
  }

  @Override
  public Hash merge(
      NamedRef from, Optional<Hash> fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return versionStore.merge(from, fromHash, toBranch, expectedHash);
  }

  @Override
  public void assign(
      NamedRef ref, Optional<Hash> expectedHash, NamedRef target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    versionStore.assign(ref, expectedHash, target, targetHash);
  }

  @Override
  public Hash create(NamedRef ref, Optional<NamedRef> target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return versionStore.create(ref, target, targetHash);
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
  public Stream<WithHash<String>> getCommits(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException {
    return versionStore.getCommits(ref, offset, untilIncluding);
  }

  @Override
  public Stream<WithType<Key, TestEnum>> getKeys(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return versionStore.getKeys(ref, hashOnRef);
  }

  @Override
  public String getValue(NamedRef ref, Optional<Hash> hashOnRef, Key key)
      throws ReferenceNotFoundException {
    return versionStore.getValue(ref, hashOnRef, key);
  }

  @Override
  public List<Optional<String>> getValues(NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys)
      throws ReferenceNotFoundException {
    return versionStore.getValues(ref, hashOnRef, keys);
  }

  @Override
  public WithHash<NamedRef> toRef(@Nonnull String refOfUnknownType)
      throws ReferenceNotFoundException {
    return versionStore.toRef(refOfUnknownType);
  }

  @Override
  public Stream<Diff<String>> getDiffs(
      NamedRef from, Optional<Hash> hashOnFrom, NamedRef to, Optional<Hash> hashOnTo)
      throws ReferenceNotFoundException {
    return versionStore.getDiffs(from, hashOnFrom, to, hashOnTo);
  }

  @Override
  public Collector collectGarbage() {
    return versionStore.collectGarbage();
  }
}
