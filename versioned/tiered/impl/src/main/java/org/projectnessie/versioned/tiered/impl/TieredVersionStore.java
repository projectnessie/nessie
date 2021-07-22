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
package org.projectnessie.versioned.tiered.impl;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentsAndState;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.KeyWithBytes;

public class TieredVersionStore<
        CONTENTS, STATE, METADATA, CONTENTS_TYPE extends Enum<CONTENTS_TYPE>>
    implements VersionStore<CONTENTS, STATE, METADATA, CONTENTS_TYPE> {

  private final DatabaseAdapter databaseAdapter;
  protected final StoreWorker<CONTENTS, STATE, METADATA, CONTENTS_TYPE> storeWorker;

  public TieredVersionStore(
      DatabaseAdapter databaseAdapter,
      StoreWorker<CONTENTS, STATE, METADATA, CONTENTS_TYPE> storeWorker) {
    this.databaseAdapter = databaseAdapter;
    this.storeWorker = storeWorker;
  }

  @Nonnull
  @Override
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    return databaseAdapter.toHash(ref);
  }

  @Override
  public WithHash<Ref> toRef(@Nonnull String refOfUnknownType) throws ReferenceNotFoundException {
    try {
      BranchName t = BranchName.of(refOfUnknownType);
      Hash h = toHash(t);
      return WithHash.of(h, t);
    } catch (ReferenceNotFoundException e) {
      TagName t = TagName.of(refOfUnknownType);
      Hash h = toHash(t);
      return WithHash.of(h, t);
    }
  }

  @Override
  public Hash commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> expectedHead,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<CONTENTS, STATE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {

    Map<Key, ByteString> expectedStates = new HashMap<>();
    List<KeyWithBytes> puts = new ArrayList<>();
    Map<Key, ByteString> global = new HashMap<>();
    List<Key> unchanged = new ArrayList<>();
    List<Key> deletes = new ArrayList<>();
    Set<Key> operationsKeys = new HashSet<>();

    for (Operation<CONTENTS, STATE> operation : operations) {
      operationsKeys.add(operation.getKey());
      if (operation instanceof Put) {
        Put<CONTENTS, STATE> op = (Put<CONTENTS, STATE>) operation;
        puts.add(
            KeyWithBytes.of(
                op.getKey(),
                storeWorker.getValueSerializer().getPayload(op.getValue()),
                storeWorker.getValueSerializer().toBytes(op.getValue())));
        if (op.getExpectedState() != null) {
          expectedStates.put(
              op.getKey(), storeWorker.getStateSerializer().toBytes(op.getExpectedState()));
        }
        STATE newStateObj = storeWorker.extractGlobalState(op.getValue());
        ByteString newState =
            newStateObj != null
                ? storeWorker.getStateSerializer().toBytes(newStateObj)
                : ByteString.EMPTY;
        global.put(op.getKey(), newState);
      } else if (operation instanceof Delete) {
        deletes.add(operation.getKey());
      } else if (operation instanceof Unchanged) {
        unchanged.add(operation.getKey());
      } else {
        throw new IllegalArgumentException(String.format("Unknown operation type '%s'", operation));
      }
    }

    ByteString commitMetaSerialized = storeWorker.getMetadataSerializer().toBytes(metadata);

    return databaseAdapter.commit(
        branch,
        expectedHead,
        expectedStates,
        puts,
        global,
        unchanged,
        deletes,
        operationsKeys,
        commitMetaSerialized);
  }

  @Override
  public Hash transplant(
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      NamedRef source,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return databaseAdapter.transplant(targetBranch, referenceHash, source, sequenceToTransplant);
  }

  @Override
  public Hash merge(
      NamedRef from,
      Optional<Hash> fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      boolean commonAncestorRequired)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return databaseAdapter.merge(from, fromHash, toBranch, expectedHash, commonAncestorRequired);
  }

  @Override
  public void assign(
      NamedRef ref, Optional<Hash> expectedHash, NamedRef target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.assign(ref, expectedHash, target, targetHash);
  }

  @Override
  public Hash create(NamedRef ref, Optional<NamedRef> target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return databaseAdapter.create(ref, target, targetHash);
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.delete(ref, hash);
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return databaseAdapter.namedRefs();
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException {
    return databaseAdapter
        .commitLog(ref, offset, untilIncluding)
        .map(
            e ->
                WithHash.of(
                    e.getHash(), storeWorker.getMetadataSerializer().fromBytes(e.getMetadata())));
  }

  @Override
  public Stream<WithType<Key, CONTENTS_TYPE>> getKeys(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return databaseAdapter
        .keys(ref, hashOnRef)
        .map(
            kt -> WithType.of(storeWorker.getValueSerializer().getType(kt.getType()), kt.getKey()));
  }

  @Override
  public CONTENTS getValue(NamedRef ref, Optional<Hash> hashOnRef, Key key)
      throws ReferenceNotFoundException {
    return getValues(ref, hashOnRef, Collections.singletonList(key)).get(0).orElse(null);
  }

  @Override
  public List<Optional<CONTENTS>> getValues(NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys)
      throws ReferenceNotFoundException {
    try (Stream<Optional<CONTENTS>> values =
        databaseAdapter
            .values(ref, hashOnRef, keys)
            .map(
                v ->
                    v.map(
                        s ->
                            s.getState() != null && !s.getState().isEmpty()
                                ? ContentsAndState.of(
                                    storeWorker.getValueSerializer().fromBytes(s.getContents()),
                                    storeWorker.getStateSerializer().fromBytes(s.getState()))
                                : ContentsAndState.<CONTENTS, STATE>of(
                                    storeWorker.getValueSerializer().fromBytes(s.getContents()))))
            .map(
                csOpt ->
                    csOpt.map(
                        cs -> storeWorker.mergeGlobalState(cs.getContents(), cs.getState())))) {
      return values.collect(Collectors.toList());
    }
  }

  @Override
  public Stream<Diff<CONTENTS>> getDiffs(
      NamedRef from, Optional<Hash> hashOnFrom, NamedRef to, Optional<Hash> hashOnTo)
      throws ReferenceNotFoundException {
    return databaseAdapter
        .diff(from, hashOnFrom, to, hashOnTo)
        .map(
            d ->
                Diff.of(
                    d.getKey(),
                    d.getFromValue().map(v -> storeWorker.getValueSerializer().fromBytes(v)),
                    d.getToValue().map(v -> storeWorker.getValueSerializer().fromBytes(v))));
  }

  @Override
  public Collector collectGarbage() {
    throw new UnsupportedOperationException();
  }
}
