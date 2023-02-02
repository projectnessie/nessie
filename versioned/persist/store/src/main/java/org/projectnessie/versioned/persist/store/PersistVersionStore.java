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

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableRefLogDetails;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeType;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.RefLogDetails;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.FilteringPaginationIterator;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;
import org.projectnessie.versioned.store.DefaultStoreWorker;

public class PersistVersionStore implements VersionStore {

  private final DatabaseAdapter databaseAdapter;
  protected static final StoreWorker STORE_WORKER = DefaultStoreWorker.instance();

  @SuppressWarnings("unused") // Keep StoreWorker parameter for compatibiltiy reasons
  public PersistVersionStore(DatabaseAdapter databaseAdapter, StoreWorker storeWorker) {
    this(databaseAdapter);
  }

  public PersistVersionStore(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
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

  @Override
  public Hash commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> expectedHead,
      @Nonnull CommitMeta metadata,
      @Nonnull List<Operation> operations,
      @Nonnull Callable<Void> validator,
      BiConsumer<Key, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException {

    ImmutableCommitParams.Builder commitAttempt =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .expectedHead(expectedHead)
            .commitMetaSerialized(serializeMetadata(metadata))
            .validator(validator);

    for (Operation operation : operations) {
      if (operation instanceof Put) {
        Put op = (Put) operation;
        Content content = op.getValue();
        Content expected = op.getExpectedValue();

        if (content.getId() == null) {
          // No content-ID --> New content

          checkArgument(
              expected == null,
              "Expected content must not be set when creating new content. "
                  + "The put operation's content has no content ID and is considered as new. "
                  + "Key: '%s'",
              op.getKey());

          // assign content-ID
          String cid = UUID.randomUUID().toString();
          content = STORE_WORKER.applyId(content, cid);
          addedContents.accept(op.getKey(), cid);
        }

        ContentId contentId = ContentId.of(content.getId());
        commitAttempt.addPuts(
            KeyWithBytes.of(
                op.getKey(),
                contentId,
                payloadForContent(content),
                STORE_WORKER.toStoreOnReferenceState(content, commitAttempt::addAttachments)));

        if (expected != null) {
          String expectedId = expected.getId();
          checkArgument(
              expectedId != null,
              "Content id for expected content must not be null, key '%s'",
              op.getKey());
          ContentId expectedContentId = ContentId.of(expectedId);
          checkArgument(
              contentId.equals(expectedContentId),
              "Content ids for new ('%s') and expected ('%s') content differ for key '%s'",
              contentId,
              expectedContentId,
              op.getKey());
        }

        Preconditions.checkState(
            !STORE_WORKER.requiresGlobalState(content),
            "Nessie no longer supports content with global state");
      } else if (operation instanceof Delete) {
        commitAttempt.addDeletes(operation.getKey());
      } else if (operation instanceof Unchanged) {
        commitAttempt.addUnchanged(operation.getKey());
      } else {
        throw new IllegalArgumentException(String.format("Unknown operation type '%s'", operation));
      }
    }

    return databaseAdapter.commit(commitAttempt.build());
  }

  @Override
  public MergeResult<Commit> transplant(
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<Key, MergeType> mergeTypes,
      MergeType defaultMergeType,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      MergeResult<CommitLogEntry> adapterMergeResult =
          databaseAdapter.transplant(
              TransplantParams.builder()
                  .toBranch(targetBranch)
                  .expectedHead(referenceHash)
                  .sequenceToTransplant(sequenceToTransplant)
                  .updateCommitMetadata(updateCommitMetadataFunction(updateCommitMetadata))
                  .keepIndividualCommits(keepIndividualCommits)
                  .mergeTypes(mergeTypes)
                  .defaultMergeType(defaultMergeType)
                  .isDryRun(dryRun)
                  .build());
      return storeMergeResult(adapterMergeResult, fetchAdditionalInfo);
    } catch (MergeConflictException mergeConflict) {
      @SuppressWarnings("unchecked")
      MergeResult<CommitLogEntry> adapterMergeResult =
          (MergeResult<CommitLogEntry>) mergeConflict.getMergeResult();
      throw new MergeConflictException(
          mergeConflict.getMessage(), storeMergeResult(adapterMergeResult, fetchAdditionalInfo));
    }
  }

  @Override
  public MergeResult<Commit> merge(
      Hash fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<Key, MergeType> mergeTypes,
      MergeType defaultMergeType,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      MergeResult<CommitLogEntry> adapterMergeResult =
          databaseAdapter.merge(
              MergeParams.builder()
                  .toBranch(toBranch)
                  .expectedHead(expectedHash)
                  .mergeFromHash(fromHash)
                  .updateCommitMetadata(updateCommitMetadataFunction(updateCommitMetadata))
                  .keepIndividualCommits(keepIndividualCommits)
                  .mergeTypes(mergeTypes)
                  .defaultMergeType(defaultMergeType)
                  .isDryRun(dryRun)
                  .build());
      return storeMergeResult(adapterMergeResult, fetchAdditionalInfo);
    } catch (MergeConflictException mergeConflict) {
      @SuppressWarnings("unchecked")
      MergeResult<CommitLogEntry> adapterMergeResult =
          (MergeResult<CommitLogEntry>) mergeConflict.getMergeResult();
      throw new MergeConflictException(
          mergeConflict.getMessage(), storeMergeResult(adapterMergeResult, fetchAdditionalInfo));
    }
  }

  private MergeResult<Commit> storeMergeResult(
      MergeResult<CommitLogEntry> adapterMergeResult, boolean fetchAdditionalInfo) {
    ImmutableMergeResult.Builder<Commit> storeResult =
        ImmutableMergeResult.<Commit>builder()
            .targetBranch(adapterMergeResult.getTargetBranch())
            .effectiveTargetHash(adapterMergeResult.getEffectiveTargetHash())
            .commonAncestor(adapterMergeResult.getCommonAncestor())
            .resultantTargetHash(adapterMergeResult.getResultantTargetHash())
            .expectedHash(adapterMergeResult.getExpectedHash())
            .wasApplied(adapterMergeResult.wasApplied())
            .wasSuccessful(adapterMergeResult.wasSuccessful())
            .details(adapterMergeResult.getDetails());

    BiConsumer<ImmutableCommit.Builder, CommitLogEntry> enhancer =
        enhancerForCommitLog(fetchAdditionalInfo);

    Function<CommitLogEntry, Commit> mapper =
        logEntry -> {
          ImmutableCommit.Builder commit = Commit.builder();
          commit.hash(logEntry.getHash()).commitMeta(deserializeMetadata(logEntry.getMetadata()));
          enhancer.accept(commit, logEntry);
          return commit.build();
        };

    if (adapterMergeResult.getSourceCommits() != null) {
      adapterMergeResult.getSourceCommits().stream()
          .map(mapper)
          .forEach(storeResult::addSourceCommits);
    }
    if (adapterMergeResult.getTargetCommits() != null) {
      adapterMergeResult.getTargetCommits().stream()
          .map(mapper)
          .forEach(storeResult::addTargetCommits);
    }

    return storeResult.build();
  }

  private MetadataRewriter<ByteString> updateCommitMetadataFunction(
      MetadataRewriter<CommitMeta> updateCommitMetadata) {
    return new MetadataRewriter<ByteString>() {
      @Override
      public ByteString rewriteSingle(ByteString metadata) {
        return serializeMetadata(updateCommitMetadata.rewriteSingle(deserializeMetadata(metadata)));
      }

      @Override
      public ByteString squash(List<ByteString> metadata) {
        return serializeMetadata(
            updateCommitMetadata.squash(
                metadata.stream()
                    .map(PersistVersionStore.this::deserializeMetadata)
                    .collect(Collectors.toList())));
      }
    };
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
  public Hash delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return databaseAdapter.delete(ref, hash);
  }

  @Nonnull
  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(@Nonnull String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    ReferenceInfo<ByteString> namedRef = databaseAdapter.namedRef(ref, params);
    return namedRef.withUpdatedCommitMeta(deserializeMetadata(namedRef.getHeadCommitMeta()));
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    checkArgument(pagingToken == null, "Paging not supported");

    @SuppressWarnings("MustBeClosedChecker")
    Stream<ReferenceInfo<ByteString>> source = databaseAdapter.namedRefs(params);

    return new FilteringPaginationIterator<ReferenceInfo<ByteString>, ReferenceInfo<CommitMeta>>(
        source.iterator(),
        namedRef ->
            namedRef.withUpdatedCommitMeta(deserializeMetadata(namedRef.getHeadCommitMeta()))) {
      @Override
      protected String computeTokenForCurrent() {
        return null;
      }

      @Override
      public String tokenForEntry(ReferenceInfo<CommitMeta> entry) {
        return null;
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  private ByteString serializeMetadata(CommitMeta metadata) {
    return metadata != null ? CommitMetaSerializer.METADATA_SERIALIZER.toBytes(metadata) : null;
  }

  private CommitMeta deserializeMetadata(ByteString commitMeta) {
    return commitMeta != null
        ? CommitMetaSerializer.METADATA_SERIALIZER.fromBytes(commitMeta)
        : null;
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    Hash hash = refToHash(ref);

    BiConsumer<ImmutableCommit.Builder, CommitLogEntry> enhancer =
        enhancerForCommitLog(fetchAdditionalInfo);

    @SuppressWarnings("MustBeClosedChecker")
    Stream<CommitLogEntry> source = databaseAdapter.commitLog(hash);

    return new FilteringPaginationIterator<CommitLogEntry, Commit>(
        source.iterator(),
        e -> {
          ImmutableCommit.Builder commit =
              Commit.builder()
                  .hash(e.getHash())
                  .addAllAdditionalParents(e.getAdditionalParents())
                  .commitMeta(deserializeMetadata(e.getMetadata()));
          if (!e.getParents().isEmpty()) {
            commit.parentHash(e.getParents().get(0));
          }
          enhancer.accept(commit, e);
          return commit.build();
        }) {
      @Override
      protected String computeTokenForCurrent() {
        return current() != null ? current().getHash().asString() : null;
      }

      @Override
      public String tokenForEntry(Commit entry) {
        return entry != null ? entry.getHash().asString() : null;
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  /**
   * Utility function for {@link #getCommits(Ref, boolean)} to optionally enhance the returned
   * {@link Commit} instances with the parent hash and operations per commit.
   */
  private BiConsumer<ImmutableCommit.Builder, CommitLogEntry> enhancerForCommitLog(
      boolean fetchAdditionalInfo) {
    if (!fetchAdditionalInfo) {
      return (commitBuilder, logEntry) -> {};
    }

    // Memoize already retrieved global-content
    Map<ContentId, ByteString> globalContents = new HashMap<>();
    Function<KeyWithBytes, ByteString> getGlobalContents =
        (put) ->
            globalContents.computeIfAbsent(
                put.getContentId(),
                cid ->
                    databaseAdapter
                        .globalContent(put.getContentId())
                        .map(ContentIdAndBytes::getValue)
                        .orElse(null));

    return (commitBuilder, logEntry) -> {
      logEntry.getDeletes().forEach(delete -> commitBuilder.addOperations(Delete.of(delete)));
      logEntry
          .getPuts()
          .forEach(
              put ->
                  commitBuilder.addOperations(
                      Put.of(
                          put.getKey(),
                          STORE_WORKER.valueFromStore(
                              put.getPayload(),
                              put.getValue(),
                              () -> getGlobalContents.apply(put),
                              databaseAdapter::mapToAttachment))));
    };
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(Ref ref, String pagingToken, boolean withContent)
      throws ReferenceNotFoundException {
    checkArgument(pagingToken == null, "Paging not supported");
    Hash hash = refToHash(ref);

    @SuppressWarnings("MustBeClosedChecker")
    Stream<KeyListEntry> source = databaseAdapter.keys(hash, KeyFilterPredicate.ALLOW_ALL);

    return new FilteringPaginationIterator<KeyListEntry, KeyEntry>(
        source.iterator(),
        entry -> {
          if (withContent) {
            try {
              ContentAndState cs =
                  databaseAdapter
                      .values(
                          hash, Collections.singleton(entry.getKey()), KeyFilterPredicate.ALLOW_ALL)
                      .get(entry.getKey());
              if (cs != null) {
                Content content = mapContentAndState(cs);
                return KeyEntry.of(
                    DefaultStoreWorker.contentTypeForPayload(entry.getPayload()),
                    entry.getKey(),
                    content);
              }
            } catch (ReferenceNotFoundException e) {
              throw new IllegalStateException("Reference no longer exists", e);
            }
          }
          return KeyEntry.of(
              DefaultStoreWorker.contentTypeForPayload(entry.getPayload()),
              entry.getKey(),
              entry.getContentId().getId());
        }) {
      @Override
      protected String computeTokenForCurrent() {
        return null;
      }

      @Override
      public String tokenForEntry(KeyEntry entry) {
        return null;
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  @Override
  public Content getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return getValues(ref, Collections.singletonList(key)).get(key);
  }

  @Override
  public Map<Key, Content> getValues(Ref ref, Collection<Key> keys)
      throws ReferenceNotFoundException {
    Hash hash = refToHash(ref);
    return databaseAdapter.values(hash, keys, KeyFilterPredicate.ALLOW_ALL).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> mapContentAndState(e.getValue())));
  }

  private Content mapContentAndState(ContentAndState cs) {
    return STORE_WORKER.valueFromStore(
        cs.getPayload(), cs.getRefState(), cs::getGlobalState, databaseAdapter::mapToAttachment);
  }

  @Override
  public PaginationIterator<Diff> getDiffs(Ref from, Ref to, String pagingToken)
      throws ReferenceNotFoundException {
    checkArgument(pagingToken == null, "Paging not supported");
    Hash fromHash = refToHash(from);
    Hash toHash = refToHash(to);

    @SuppressWarnings("MustBeClosedChecker")
    Stream<Difference> source =
        databaseAdapter.diff(fromHash, toHash, KeyFilterPredicate.ALLOW_ALL);

    return new FilteringPaginationIterator<Difference, Diff>(
        source.iterator(),
        d ->
            Diff.of(
                d.getKey(),
                d.getFromValue()
                    .map(
                        v ->
                            STORE_WORKER.valueFromStore(
                                d.getPayload(),
                                v,
                                () -> d.getGlobal().orElse(null),
                                databaseAdapter::mapToAttachment)),
                d.getToValue()
                    .map(
                        v ->
                            STORE_WORKER.valueFromStore(
                                d.getPayload(),
                                v,
                                () -> d.getGlobal().orElse(null),
                                databaseAdapter::mapToAttachment)))) {
      @Override
      protected String computeTokenForCurrent() {
        return null;
      }

      @Override
      public String tokenForEntry(Diff entry) {
        return null;
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  private Hash refToHash(Ref ref) throws ReferenceNotFoundException {
    if (ref instanceof NamedRef) {
      return hashOnReference((NamedRef) ref, Optional.empty());
    }
    if (ref instanceof Hash) {
      return (Hash) ref;
    }
    throw new IllegalArgumentException(String.format("Unsupported reference '%s'", ref));
  }

  @Override
  @MustBeClosed
  public Stream<RefLogDetails> getRefLog(Hash refLogId) throws RefLogNotFoundException {
    return databaseAdapter
        .refLog(refLogId)
        .map(
            e ->
                ImmutableRefLogDetails.builder()
                    .refLogId(e.getRefLogId())
                    .refName(e.getRefName())
                    .refType(e.getRefType())
                    .commitHash(e.getCommitHash())
                    .parentRefLogId(e.getParents().get(0))
                    .operationTime(e.getOperationTime())
                    .operation(e.getOperation())
                    .sourceHashes(e.getSourceHashes())
                    .build());
  }
}
