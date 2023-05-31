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
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
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

  private final Tracer tracer;

  /**
   * Takes the {@link VersionStore} instance to trace.
   *
   * @param delegate backing/delegate {@link VersionStore}
   */
  public TracingVersionStore(VersionStore delegate) {
    this(GlobalOpenTelemetry.getTracer("database-adapter"), delegate);
  }

  public TracingVersionStore(Tracer tracer, VersionStore delegate) {
    this.tracer = tracer;
    this.delegate = delegate;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public RepositoryInformation getRepositoryInformation() {
    return callWithNoException(
        tracer, "RepositoryInformation", b -> {}, delegate::getRepositoryInformation);
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return callWithOneException(
        tracer,
        "HashOnReference",
        b ->
            b.setAttribute(TAG_REF, safeRefName(namedReference))
                .setAttribute(TAG_HASH, safeToString(hashOnReference)),
        () -> delegate.hashOnReference(namedReference, hashOnReference));
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
    return TracingVersionStore
        .<CommitResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>
            callWithTwoExceptions(
                tracer,
                "Commit",
                b ->
                    b.setAttribute(TAG_BRANCH, safeRefName(branch))
                        .setAttribute(TAG_HASH, safeToString(referenceHash))
                        .setAttribute(TAG_NUM_OPS, safeSize(operations)),
                () ->
                    delegate.commit(
                        branch, referenceHash, metadata, operations, validator, addedContents));
  }

  @Override
  public MergeResult<Commit> transplant(TransplantOp transplantOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return TracingVersionStore
        .<MergeResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>
            callWithTwoExceptions(
                tracer,
                "Transplant",
                b ->
                    b.setAttribute(TAG_TARGET_BRANCH, safeRefName(transplantOp.toBranch()))
                        .setAttribute(TAG_HASH, safeToString(transplantOp.expectedHash()))
                        .setAttribute(
                            TAG_TRANSPLANTS, safeSize(transplantOp.sequenceToTransplant())),
                () -> delegate.transplant(transplantOp));
  }

  @Override
  public MergeResult<Commit> merge(MergeOp mergeOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return TracingVersionStore
        .<MergeResult<Commit>, ReferenceNotFoundException, ReferenceConflictException>
            callWithTwoExceptions(
                tracer,
                "Merge",
                b ->
                    b.setAttribute(TAG_FROM_HASH, safeToString(mergeOp.fromHash()))
                        .setAttribute(TAG_TO_BRANCH, safeRefName(mergeOp.toBranch()))
                        .setAttribute(TAG_EXPECTED_HASH, safeToString(mergeOp.expectedHash())),
                () -> delegate.merge(mergeOp));
  }

  @Override
  public ReferenceAssignedResult assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return TracingVersionStore
        .<ReferenceAssignedResult, ReferenceNotFoundException, ReferenceConflictException>
            callWithTwoExceptions(
                tracer,
                "Assign",
                b ->
                    b.setAttribute(TAG_REF, safeToString(ref))
                        .setAttribute(
                            TracingVersionStore.TAG_EXPECTED_HASH, safeToString(expectedHash))
                        .setAttribute(TAG_TARGET_HASH, safeToString(targetHash)),
                () -> delegate.assign(ref, expectedHash, targetHash));
  }

  @Override
  public ReferenceCreatedResult create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return TracingVersionStore
        .<ReferenceCreatedResult, ReferenceNotFoundException, ReferenceAlreadyExistsException>
            callWithTwoExceptions(
                tracer,
                "Create",
                b ->
                    b.setAttribute(TAG_REF, safeToString(ref))
                        .setAttribute(TAG_TARGET_HASH, safeToString(targetHash)),
                () -> delegate.create(ref, targetHash));
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return TracingVersionStore
        .<ReferenceDeletedResult, ReferenceNotFoundException, ReferenceConflictException>
            callWithTwoExceptions(
                tracer,
                "Delete",
                b ->
                    b.setAttribute(TAG_REF, safeToString(ref))
                        .setAttribute(TAG_HASH, safeToString(hash)),
                () -> delegate.delete(ref, hash));
  }

  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return callWithOneException(
        tracer,
        "GetNamedRef",
        b -> b.setAttribute(TAG_REF, ref),
        () -> delegate.getNamedRef(ref, params));
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    return callPaginationIterator(
        tracer, "GetNamedRefs", b -> {}, () -> delegate.getNamedRefs(params, pagingToken));
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return callPaginationIterator(
        tracer,
        "GetCommits",
        b -> b.setAttribute(TAG_REF, safeToString(ref)),
        () -> delegate.getCommits(ref, fetchAdditionalInfo));
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(
      Ref ref, String pagingToken, boolean withContent, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    return callPaginationIterator(
        tracer,
        "GetKeys",
        b -> b.setAttribute(TAG_REF, safeToString(ref)),
        () -> delegate.getKeys(ref, pagingToken, withContent, keyRestrictions));
  }

  @Override
  public List<IdentifiedContentKey> getIdentifiedKeys(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    return callWithOneException(
        tracer,
        "IdentifiedKeys",
        b -> b.setAttribute(TAG_REF, safeToString(ref)),
        () -> delegate.getIdentifiedKeys(ref, keys));
  }

  @Override
  public ContentResult getValue(Ref ref, ContentKey key) throws ReferenceNotFoundException {
    return callWithOneException(
        tracer,
        "GetValue",
        b -> b.setAttribute(TAG_REF, safeToString(ref)).setAttribute(TAG_KEY, safeToString(key)),
        () -> delegate.getValue(ref, key));
  }

  @Override
  public Map<ContentKey, ContentResult> getValues(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    return callWithOneException(
        tracer,
        "GetValues",
        b -> b.setAttribute(TAG_REF, safeToString(ref)).setAttribute(TAG_KEYS, safeToString(keys)),
        () -> delegate.getValues(ref, keys));
  }

  @Override
  public PaginationIterator<Diff> getDiffs(
      Ref from, Ref to, String pagingToken, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    return callPaginationIterator(
        tracer,
        "GetDiffs",
        b -> b.setAttribute(TAG_FROM, safeToString(from)).setAttribute(TAG_TO, safeToString(to)),
        () -> delegate.getDiffs(from, to, pagingToken, keyRestrictions));
  }

  @Override
  @Deprecated
  public Stream<RefLogDetails> getRefLog(Hash refLogId) throws RefLogNotFoundException {
    return delegate.getRefLog(refLogId);
  }

  private static SpanHolder createSpan(
      Tracer tracer, String name, Consumer<SpanBuilder> spanBuilder) {
    String spanName = makeSpanName(name);
    SpanBuilder builder = tracer.spanBuilder(spanName).setAttribute(TAG_OPERATION, name);
    spanBuilder.accept(builder);
    return new SpanHolder(builder.startSpan());
  }

  @VisibleForTesting
  static String makeSpanName(String name) {
    return "VersionStore." + Character.toLowerCase(name.charAt(0)) + name.substring(1);
  }

  private static <R, E1 extends VersionStoreException> PaginationIterator<R> callPaginationIterator(
      Tracer tracer,
      String spanName,
      Consumer<SpanBuilder> spanBuilder,
      InvokerWithOneException<PaginationIterator<R>, E1> invoker)
      throws E1 {
    try (SpanHolder span = createSpan(tracer, spanName + ".stream", spanBuilder)) {
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

  private <R> R callWithNoException(
      Tracer tracer, String spanName, Consumer<SpanBuilder> spanBuilder, Supplier<R> invoker) {
    try (SpanHolder span = createSpan(tracer, spanName, spanBuilder)) {
      try {
        return invoker.get();
      } catch (IllegalArgumentException e) {
        // IllegalArgumentException is a special kind of exception that indicates a user-error.
        throw e;
      } catch (RuntimeException e) {
        throw traceError(span.get(), e);
      }
    }
  }

  private static <R, E1 extends VersionStoreException> R callWithOneException(
      Tracer tracer,
      String spanName,
      Consumer<SpanBuilder> spanBuilder,
      InvokerWithOneException<R, E1> invoker)
      throws E1 {
    try (SpanHolder span = createSpan(tracer, spanName, spanBuilder)) {
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

  private static <R, E1 extends VersionStoreException, E2 extends VersionStoreException>
      R callWithTwoExceptions(
          Tracer tracer,
          String spanName,
          Consumer<SpanBuilder> spanBuilder,
          InvokerWithTwoExceptionsR<R, E1, E2> invoker)
          throws E1, E2 {
    try (SpanHolder span = createSpan(tracer, spanName, spanBuilder)) {
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
  interface InvokerWithTwoExceptionsR<
      R, E1 extends VersionStoreException, E2 extends VersionStoreException> {
    R handle() throws E1, E2;
  }

  private static String safeRefName(NamedRef ref) {
    return ref != null ? ref.getName() : "<null>";
  }

  private static class SpanHolder implements Closeable {
    private final Span span;
    private final Scope scope;

    private SpanHolder(Span span) {
      this.span = span;
      this.scope = span.makeCurrent();
    }

    private Span get() {
      return span;
    }

    @Override
    public void close() {
      try {
        scope.close();
      } finally {
        span.end();
      }
    }
  }
}
