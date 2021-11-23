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
package org.projectnessie.versioned.persist.store;

import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;

public class PersistVersionStore<CONTENT, METADATA, CONTENT_TYPE extends Enum<CONTENT_TYPE>>
    implements VersionStore<CONTENT, METADATA, CONTENT_TYPE> {

  private final DatabaseAdapter databaseAdapter;
  protected final StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker;

  public PersistVersionStore(
      DatabaseAdapter databaseAdapter, StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker) {
    this.databaseAdapter = databaseAdapter;
    this.storeWorker = storeWorker;
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return databaseAdapter.hashOnReference(namedReference, hashOnReference);
  }

  @Nonnull
  @Override
  public Hash noAncestorHash() {
    return databaseAdapter.noAncestorHash();
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
      @Nonnull List<Operation<CONTENT>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {

    ImmutableCommitAttempt.Builder commitAttempt =
        ImmutableCommitAttempt.builder().commitToBranch(branch).expectedHead(expectedHead);

    for (Operation<CONTENT> operation : operations) {
      if (operation instanceof Put) {
        Put<CONTENT> op = (Put<CONTENT>) operation;
        ContentId contentId = ContentId.of(storeWorker.getId(op.getValue()));
        commitAttempt.addPuts(
            KeyWithBytes.of(
                op.getKey(),
                contentId,
                storeWorker.getPayload(op.getValue()),
                storeWorker.toStoreOnReferenceState(op.getValue())));
        if (storeWorker.requiresGlobalState(op.getValue())) {
          ByteString newState = storeWorker.toStoreGlobalState(op.getValue());
          Optional<ByteString> expectedValue;
          if (op.getExpectedValue() != null) {
            if (storeWorker.getType(op.getValue()) != storeWorker.getType(op.getExpectedValue())) {
              throw new IllegalArgumentException(
                  String.format(
                      "Content-type for conditional put-operation for key '%s' for 'value' and 'expectedValue' must be the same, but are '%s' and '%s'.",
                      op.getKey(),
                      storeWorker.getType(op.getValue()),
                      storeWorker.getType(op.getExpectedValue())));
            }
            if (!contentId.equals(ContentId.of(storeWorker.getId(op.getExpectedValue())))) {
              throw new IllegalArgumentException(
                  String.format(
                      "Conditional put-operation key '%s' has different content-ids.",
                      op.getKey()));
            }

            expectedValue = Optional.of(storeWorker.toStoreGlobalState(op.getExpectedValue()));
          } else {
            expectedValue = Optional.empty();
          }
          commitAttempt.putExpectedStates(contentId, expectedValue);
          commitAttempt.putGlobal(contentId, newState);
        } else {
          if (op.getExpectedValue() != null) {
            throw new IllegalArgumentException(
                String.format(
                    "Content-type '%s' for put-operation for key '%s' does not support global state, expected-value not supported for this content-type.",
                    storeWorker.getType(op.getValue()), op.getKey()));
          }
        }
      } else if (operation instanceof Delete) {
        commitAttempt.addDeletes(operation.getKey());
      } else if (operation instanceof Unchanged) {
        commitAttempt.addUnchanged(operation.getKey());
      } else {
        throw new IllegalArgumentException(String.format("Unknown operation type '%s'", operation));
      }
    }

    commitAttempt.commitMetaSerialized(storeWorker.getMetadataSerializer().toBytes(metadata));

    return databaseAdapter.commit(commitAttempt.build());
  }

  @Override
  public void transplant(
      BranchName targetBranch, Optional<Hash> referenceHash, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.transplant(targetBranch, referenceHash, sequenceToTransplant);
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.merge(fromHash, toBranch, expectedHash);
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.assign(ref, expectedHash, targetHash);
  }

  @Override
  public Hash create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return databaseAdapter.create(ref, targetHash.orElseGet(databaseAdapter::noAncestorHash));
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.delete(ref, hash);
  }

  @Override
  public Stream<ReferenceInfo<METADATA>> getNamedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return databaseAdapter
        .namedRefs(params)
        .map(
            namedRef ->
                namedRef.withUpdatedCommitMeta(deserializeMetadata(namedRef.getHeadCommitMeta())));
  }

  private METADATA deserializeMetadata(ByteString commitMeta) {
    return commitMeta != null ? storeWorker.getMetadataSerializer().fromBytes(commitMeta) : null;
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    Hash hash = ref instanceof NamedRef ? toHash((NamedRef) ref) : (Hash) ref;
    Stream<CommitLogEntry> stream = databaseAdapter.commitLog(hash);

    return stream.map(e -> WithHash.of(e.getHash(), deserializeMetadata(e.getMetadata())));
  }

  @Override
  public Stream<WithType<Key, CONTENT_TYPE>> getKeys(Ref ref) throws ReferenceNotFoundException {
    Hash hash = ref instanceof NamedRef ? toHash((NamedRef) ref) : (Hash) ref;
    return databaseAdapter
        .keys(hash, KeyFilterPredicate.ALLOW_ALL)
        .map(kt -> WithType.of(storeWorker.getType(kt.getType()), kt.getKey()));
  }

  @Override
  public CONTENT getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return getValues(ref, Collections.singletonList(key)).get(key);
  }

  @Override
  public Map<Key, CONTENT> getValues(Ref ref, Collection<Key> keys)
      throws ReferenceNotFoundException {
    Hash hash = ref instanceof NamedRef ? toHash((NamedRef) ref) : (Hash) ref;
    return databaseAdapter.values(hash, keys, KeyFilterPredicate.ALLOW_ALL).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> mapContentAndState(e.getValue())));
  }

  private CONTENT mapContentAndState(ContentAndState<ByteString> cs) {
    return storeWorker.valueFromStore(cs.getRefState(), Optional.ofNullable(cs.getGlobalState()));
  }

  @Override
  public Stream<Diff<CONTENT>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    Hash fromHash = from instanceof NamedRef ? toHash((NamedRef) from) : (Hash) from;
    Hash toHash = to instanceof NamedRef ? toHash((NamedRef) to) : (Hash) to;
    return databaseAdapter
        .diff(fromHash, toHash, KeyFilterPredicate.ALLOW_ALL)
        .map(
            d ->
                Diff.of(
                    d.getKey(),
                    d.getFromValue().map(v -> storeWorker.valueFromStore(v, d.getGlobal())),
                    d.getToValue().map(v -> storeWorker.valueFromStore(v, d.getGlobal()))));
  }
}
