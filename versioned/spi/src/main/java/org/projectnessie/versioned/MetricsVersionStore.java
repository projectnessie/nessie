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
package org.projectnessie.versioned;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.versioned.paging.PaginationIterator;

/** A {@link VersionStore} wrapper that publishes metrics via Micrometer. */
@SuppressWarnings("MustBeClosedChecker")
public final class MetricsVersionStore implements VersionStore {

  private final VersionStore delegate;
  private final MeterRegistry registry;
  private final Clock clock;

  /**
   * Constructor taking the delegate version-store and the metrics-registry.
   *
   * @param delegate delegate version-store
   * @param registry metrics-registry
   */
  public MetricsVersionStore(VersionStore delegate, MeterRegistry registry, Clock clock) {
    this.delegate = delegate;
    this.registry = registry;
    this.clock = clock;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public RepositoryInformation getRepositoryInformation() {
    return delegate("repositoryInformation", delegate::getRepositoryInformation);
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return delegate1Ex(
        "hashonreference", () -> delegate.hashOnReference(namedReference, hashOnReference));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @Override
  public CommitResult<Commit> commit(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull CommitMeta metadata,
      @Nonnull @jakarta.annotation.Nonnull List<Operation> operations,
      @Nonnull @jakarta.annotation.Nonnull CommitValidator validator,
      @Nonnull @jakarta.annotation.Nonnull BiConsumer<ContentKey, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return this
        .<CommitResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>delegate2ExR(
            "commit",
            () ->
                delegate.commit(
                    branch, referenceHash, metadata, operations, validator, addedContents));
  }

  @Override
  public MergeResult<Commit> transplant(
      NamedRef sourceRef,
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors,
      MergeBehavior defaultMergeBehavior,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return this
        .<MergeResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>delegate2ExR(
            "transplant",
            () ->
                delegate.transplant(
                    sourceRef,
                    targetBranch,
                    referenceHash,
                    sequenceToTransplant,
                    updateCommitMetadata,
                    keepIndividualCommits,
                    mergeKeyBehaviors,
                    defaultMergeBehavior,
                    dryRun,
                    fetchAdditionalInfo));
  }

  @Override
  public MergeResult<Commit> merge(
      NamedRef fromRef,
      Hash fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors,
      MergeBehavior defaultMergeBehavior,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return this
        .<MergeResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>delegate2ExR(
            "merge",
            () ->
                delegate.merge(
                    fromRef,
                    fromHash,
                    toBranch,
                    expectedHash,
                    updateCommitMetadata,
                    keepIndividualCommits,
                    mergeKeyBehaviors,
                    defaultMergeBehavior,
                    dryRun,
                    fetchAdditionalInfo));
  }

  @Override
  public ReferenceAssignedResult assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return this
        .<ReferenceAssignedResult, ReferenceNotFoundException, ReferenceConflictException>
            delegate2ExR("assign", () -> delegate.assign(ref, expectedHash, targetHash));
  }

  @Override
  public ReferenceCreatedResult create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return this
        .<ReferenceCreatedResult, ReferenceNotFoundException, ReferenceAlreadyExistsException>
            delegate2ExR("create", () -> delegate.create(ref, targetHash));
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return this
        .<ReferenceDeletedResult, ReferenceNotFoundException, ReferenceConflictException>
            delegate2ExR("delete", () -> delegate.delete(ref, hash));
  }

  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return delegate1Ex("getnamedref", () -> delegate.getNamedRef(ref, params));
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    return delegatePaginationIterator(
        "getnamedrefs", () -> delegate.getNamedRefs(params, pagingToken));
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return delegatePaginationIterator(
        "getcommits", () -> delegate.getCommits(ref, fetchAdditionalInfo));
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(
      Ref ref,
      String pagingToken,
      boolean withContent,
      ContentKey minKey,
      ContentKey maxKey,
      ContentKey prefixKey,
      Predicate<ContentKey> contentKeyPredicate)
      throws ReferenceNotFoundException {
    return delegatePaginationIterator(
        "getkeys",
        () ->
            delegate.getKeys(
                ref, pagingToken, withContent, minKey, maxKey, prefixKey, contentKeyPredicate));
  }

  @Override
  public List<IdentifiedContentKey> getIdentifiedKeys(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    return delegate1Ex("identifiedKeys", () -> delegate.getIdentifiedKeys(ref, keys));
  }

  @Override
  public ContentResult getValue(Ref ref, ContentKey key) throws ReferenceNotFoundException {
    return delegate1Ex("getvalue", () -> delegate.getValue(ref, key));
  }

  @Override
  public Map<ContentKey, ContentResult> getValues(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    return delegate1Ex("getvalues", () -> delegate.getValues(ref, keys));
  }

  @Override
  public PaginationIterator<Diff> getDiffs(
      Ref from,
      Ref to,
      String pagingToken,
      ContentKey minKey,
      ContentKey maxKey,
      ContentKey prefixKey,
      Predicate<ContentKey> contentKeyPredicate)
      throws ReferenceNotFoundException {
    return delegatePaginationIterator(
        "getdiffs",
        () ->
            delegate.getDiffs(
                from, to, pagingToken, minKey, maxKey, prefixKey, contentKeyPredicate));
  }

  @Override
  @Deprecated
  public Stream<RefLogDetails> getRefLog(Hash refLogId) throws RefLogNotFoundException {
    return delegateStream1Ex("getreflog", () -> delegate.getRefLog(refLogId));
  }

  private void measure(String requestName, Sample sample, Exception failure) {
    Timer timer =
        Timer.builder("nessie.versionstore.request")
            .tag("request", requestName)
            .tag("error", Boolean.toString(failure != null))
            .publishPercentileHistogram()
            .register(registry);
    sample.stop(timer);
  }

  private <R, E extends VersionStoreException> PaginationIterator<R> delegatePaginationIterator(
      String requestName, DelegateWith1<PaginationIterator<R>, E> delegate) throws E {
    Sample sample = Timer.start(clock);
    try {
      @SuppressWarnings("resource")
      PaginationIterator<R> r = delegate.handle();
      return new PaginationIterator<R>() {
        @Override
        public String tokenForCurrent() {
          return r.tokenForCurrent();
        }

        @Override
        public String tokenForEntry(R entry) {
          return r.tokenForEntry(entry);
        }

        @Override
        public void close() {
          try {
            r.close();
          } finally {
            measure(requestName, sample, null);
          }
        }

        @Override
        public boolean hasNext() {
          return r.hasNext();
        }

        @Override
        public R next() {
          return r.next();
        }
      };
    } catch (IllegalArgumentException | VersionStoreException e) {
      // IllegalArgumentException indicates a user-error, not a server error
      measure(requestName, sample, null);
      throw e;
    } catch (RuntimeException e) {
      measure(requestName, sample, e);
      throw e;
    }
  }

  private <R, E extends VersionStoreException> Stream<R> delegateStream1Ex(
      String requestName, DelegateWith1<Stream<R>, E> delegate) throws E {
    Sample sample = Timer.start(clock);
    try {
      return delegate.handle().onClose(() -> measure(requestName, sample, null));
    } catch (IllegalArgumentException | VersionStoreException e) {
      // IllegalArgumentException indicates a user-error, not a server error
      measure(requestName, sample, null);
      throw e;
    } catch (RuntimeException e) {
      measure(requestName, sample, e);
      throw e;
    }
  }

  private <R> R delegate(String requestName, Supplier<R> delegate) {
    try {
      return delegate2ExR(requestName, delegate::get);
    } catch (VersionStoreException e) {
      throw new RuntimeException(e);
    }
  }

  private <R, E1 extends VersionStoreException> R delegate1Ex(
      String requestName, DelegateWith1<R, E1> delegate) throws E1 {
    return delegate2ExR(requestName, delegate::handle);
  }

  private <R, E1 extends VersionStoreException, E2 extends VersionStoreException> R delegate2ExR(
      String requestName, DelegateWith2R<R, E1, E2> delegate) throws E1, E2 {
    Sample sample = Timer.start(clock);
    Exception failure = null;
    try {
      return delegate.handle();
    } catch (IllegalArgumentException e) {
      // IllegalArgumentException indicates a user-error, not a server error
      throw e;
    } catch (RuntimeException e) {
      failure = e;
      throw e;
    } finally {
      measure(requestName, sample, failure);
    }
  }

  @FunctionalInterface
  interface DelegateWith1<R, E1 extends VersionStoreException> {
    R handle() throws E1;
  }

  @FunctionalInterface
  interface DelegateWith2R<R, E1 extends VersionStoreException, E2 extends VersionStoreException> {
    R handle() throws E1, E2;
  }
}
