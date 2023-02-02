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

import static org.projectnessie.versioned.TracingUtil.safeSize;
import static org.projectnessie.versioned.TracingUtil.safeToString;
import static org.projectnessie.versioned.TracingUtil.traceError;

import com.google.common.annotations.VisibleForTesting;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.util.GlobalTracer;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.paging.PaginationIterator;

/**
 * A {@link VersionStore} wrapper that publishes tracing information via OpenTracing/OpenTelemetry.
 */
@SuppressWarnings("MustBeClosedChecker")
public class TracingVersionStore implements VersionStore {

  private static final String TAG_OPERATION = "nessie.version-store.operation";
  private static final String TAG_REF = "nessie.version-store.ref";
  private static final String TAG_BRANCH = "nessie.version-store.branch";
  private static final String TAG_HASH = "nessie.version-store.hash";
  private static final String TAG_NUM_OPS = "nessie.version-store.num-ops";
  private static final String TAG_TARGET_BRANCH = "nessie.version-store.target-branch";
  private static final String TAG_TRANSPLANTS = "nessie.version-store.transplants";
  private static final String TAG_FROM_HASH = "nessie.version-store.from-hash";
  private static final String TAG_TO_BRANCH = "nessie.version-store.to-branch";
  private static final String TAG_EXPECTED_HASH = "nessie.version-store.expected-hash";
  private static final String TAG_TARGET_HASH = "nessie.version-store.target-hash";
  private static final String TAG_KEY = "nessie.version-store.key";
  private static final String TAG_KEYS = "nessie.version-store.keys";
  private static final String TAG_FROM = "nessie.version-store.from";
  private static final String TAG_TO = "nessie.version-store.to";

  private final VersionStore delegate;

  /**
   * Takes the {@link VersionStore} instance to trace.
   *
   * @param delegate backing/delegate {@link VersionStore}
   */
  public TracingVersionStore(VersionStore delegate) {
    this.delegate = delegate;
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return callWithOneException(
        "HashOnReference",
        b ->
            b.withTag(TAG_REF, safeRefName(namedReference))
                .withTag(TAG_HASH, safeToString(hashOnReference)),
        () -> delegate.hashOnReference(namedReference, hashOnReference));
  }

  @Nonnull
  @Override
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @Override
  public Hash commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull CommitMeta metadata,
      @Nonnull List<Operation> operations,
      @Nonnull Callable<Void> validator,
      @Nonnull BiConsumer<Key, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return TracingVersionStore
        .<Hash, ReferenceNotFoundException, ReferenceConflictException>callWithTwoExceptions(
            "Commit",
            b ->
                b.withTag(TAG_BRANCH, safeRefName(branch))
                    .withTag(TAG_HASH, safeToString(referenceHash))
                    .withTag(TAG_NUM_OPS, safeSize(operations)),
            () ->
                delegate.commit(
                    branch, referenceHash, metadata, operations, validator, addedContents));
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
    return TracingVersionStore
        .<MergeResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>
            callWithTwoExceptions(
                "Transplant",
                b ->
                    b.withTag(TAG_TARGET_BRANCH, safeRefName(targetBranch))
                        .withTag(TAG_HASH, safeToString(referenceHash))
                        .withTag(TAG_TRANSPLANTS, safeSize(sequenceToTransplant)),
                () ->
                    delegate.transplant(
                        targetBranch,
                        referenceHash,
                        sequenceToTransplant,
                        updateCommitMetadata,
                        keepIndividualCommits,
                        mergeTypes,
                        defaultMergeType,
                        dryRun,
                        fetchAdditionalInfo));
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
    return TracingVersionStore
        .<MergeResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>
            callWithTwoExceptions(
                "Merge",
                b ->
                    b.withTag(TAG_FROM_HASH, safeToString(fromHash))
                        .withTag(TAG_TO_BRANCH, safeRefName(toBranch))
                        .withTag(TAG_EXPECTED_HASH, safeToString(expectedHash)),
                () ->
                    delegate.merge(
                        fromHash,
                        toBranch,
                        expectedHash,
                        updateCommitMetadata,
                        keepIndividualCommits,
                        mergeTypes,
                        defaultMergeType,
                        dryRun,
                        fetchAdditionalInfo));
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    TracingVersionStore
        .<ReferenceNotFoundException, ReferenceConflictException>callWithTwoExceptions(
            "Assign",
            b ->
                b.withTag(TAG_REF, safeToString(ref))
                    .withTag(TracingVersionStore.TAG_EXPECTED_HASH, safeToString(expectedHash))
                    .withTag(TAG_TARGET_HASH, safeToString(targetHash)),
            () -> delegate.assign(ref, expectedHash, targetHash));
  }

  @Override
  public Hash create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return TracingVersionStore
        .<Hash, ReferenceNotFoundException, ReferenceAlreadyExistsException>callWithTwoExceptions(
            "Create",
            b ->
                b.withTag(TAG_REF, safeToString(ref))
                    .withTag(TAG_TARGET_HASH, safeToString(targetHash)),
            () -> delegate.create(ref, targetHash));
  }

  @Override
  public Hash delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return TracingVersionStore
        .<Hash, ReferenceNotFoundException, ReferenceConflictException>callWithTwoExceptions(
            "Delete",
            b -> b.withTag(TAG_REF, safeToString(ref)).withTag(TAG_HASH, safeToString(hash)),
            () -> delegate.delete(ref, hash));
  }

  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return callWithOneException(
        "GetNamedRef", b -> b.withTag(TAG_REF, ref), () -> delegate.getNamedRef(ref, params));
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    return callPaginationIterator(
        "GetNamedRefs", b -> {}, () -> delegate.getNamedRefs(params, pagingToken));
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return callPaginationIterator(
        "GetCommits",
        b -> b.withTag(TAG_REF, safeToString(ref)),
        () -> delegate.getCommits(ref, fetchAdditionalInfo));
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(Ref ref, String pagingToken, boolean withContent)
      throws ReferenceNotFoundException {
    return callPaginationIterator(
        "GetKeys",
        b -> b.withTag(TAG_REF, safeToString(ref)),
        () -> delegate.getKeys(ref, pagingToken, withContent));
  }

  @Override
  public Content getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return callWithOneException(
        "GetValue",
        b -> b.withTag(TAG_REF, safeToString(ref)).withTag(TAG_KEY, safeToString(key)),
        () -> delegate.getValue(ref, key));
  }

  @Override
  public Map<Key, Content> getValues(Ref ref, Collection<Key> keys)
      throws ReferenceNotFoundException {
    return callWithOneException(
        "GetValues",
        b -> b.withTag(TAG_REF, safeToString(ref)).withTag(TAG_KEYS, safeToString(keys)),
        () -> delegate.getValues(ref, keys));
  }

  @Override
  public PaginationIterator<Diff> getDiffs(Ref from, Ref to, String pagingToken)
      throws ReferenceNotFoundException {
    return callPaginationIterator(
        "GetDiffs",
        b -> b.withTag(TAG_FROM, safeToString(from)).withTag(TAG_TO, safeToString(to)),
        () -> delegate.getDiffs(from, to, pagingToken));
  }

  @Override
  public Stream<RefLogDetails> getRefLog(Hash refLogId) throws RefLogNotFoundException {
    return delegate.getRefLog(refLogId);
  }

  private static SpanHolder createSpan(String name, Consumer<SpanBuilder> spanBuilder) {
    Tracer tracer = GlobalTracer.get();
    String spanName = makeSpanName(name);
    SpanBuilder builder =
        tracer.buildSpan(spanName).asChildOf(tracer.activeSpan()).withTag(TAG_OPERATION, name);
    spanBuilder.accept(builder);
    return new SpanHolder(builder.start());
  }

  private static Scope activeScope(Span span) {
    Tracer tracer = GlobalTracer.get();
    return tracer.activateSpan(span);
  }

  @VisibleForTesting
  static String makeSpanName(String name) {
    return "VersionStore." + Character.toLowerCase(name.charAt(0)) + name.substring(1);
  }

  private static <R, E1 extends VersionStoreException> PaginationIterator<R> callPaginationIterator(
      String spanName,
      Consumer<SpanBuilder> spanBuilder,
      InvokerWithOneException<PaginationIterator<R>, E1> invoker)
      throws E1 {
    try (SpanHolder span = createSpan(spanName + ".stream", spanBuilder);
        Scope ignore = activeScope(span.get())) {
      try {
        return invoker.handle();
      } catch (IllegalArgumentException e) {
        // IllegalArgumentException is a special kind of exception that indicates a user-error.
        throw e;
      } catch (RuntimeException e) {
        throw traceError(span.get(), e);
      }
    }
  }

  private static <R, E1 extends VersionStoreException> R callWithOneException(
      String spanName, Consumer<SpanBuilder> spanBuilder, InvokerWithOneException<R, E1> invoker)
      throws E1 {
    try (SpanHolder span = createSpan(spanName, spanBuilder);
        Scope ignore = activeScope(span.get())) {
      try {
        return invoker.handle();
      } catch (IllegalArgumentException e) {
        // IllegalArgumentException is a special kind of exception that indicates a user-error.
        throw e;
      } catch (RuntimeException e) {
        throw traceError(span.get(), e);
      }
    }
  }

  private static <E1 extends VersionStoreException, E2 extends VersionStoreException>
      void callWithTwoExceptions(
          String spanName,
          Consumer<SpanBuilder> spanBuilder,
          InvokerWithTwoExceptions<E1, E2> invoker)
          throws E1, E2 {
    try (SpanHolder span = createSpan(spanName, spanBuilder);
        Scope ignore = activeScope(span.get())) {
      try {
        invoker.handle();
      } catch (IllegalArgumentException e) {
        // IllegalArgumentException is a special kind of exception that indicates a user-error.
        throw e;
      } catch (RuntimeException e) {
        throw traceError(span.get(), e);
      }
    }
  }

  private static <R, E1 extends VersionStoreException, E2 extends VersionStoreException>
      R callWithTwoExceptions(
          String spanName,
          Consumer<SpanBuilder> spanBuilder,
          InvokerWithTwoExceptionsR<R, E1, E2> invoker)
          throws E1, E2 {
    try (SpanHolder span = createSpan(spanName, spanBuilder);
        Scope ignore = activeScope(span.get())) {
      try {
        return invoker.handle();
      } catch (IllegalArgumentException e) {
        // IllegalArgumentException is a special kind of exception that indicates a user-error.
        throw e;
      } catch (RuntimeException e) {
        throw traceError(span.get(), e);
      }
    }
  }

  @FunctionalInterface
  interface InvokerWithOneException<R, E1 extends VersionStoreException> {
    R handle() throws E1;
  }

  @FunctionalInterface
  interface InvokerWithTwoExceptions<
      E1 extends VersionStoreException, E2 extends VersionStoreException> {
    void handle() throws E1, E2;
  }

  @FunctionalInterface
  interface InvokerWithTwoExceptionsR<
      R, E1 extends VersionStoreException, E2 extends VersionStoreException> {
    R handle() throws E1, E2;
  }

  private static String safeRefName(NamedRef ref) {
    return ref != null ? ref.getName() : "<null>";
  }

  private static class SpanHolder implements Closeable {
    private final Span span;

    private SpanHolder(Span span) {
      this.span = span;
    }

    private Span get() {
      return span;
    }

    @Override
    public void close() {
      span.finish();
    }
  }
}
