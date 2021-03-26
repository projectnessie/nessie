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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

/**
 * A {@link VersionStore} wrapper that publishes tracing information via OpenTracing/OpenTelemetry.
 *
 * @param <VALUE> see {@link VersionStore}
 * @param <METADATA> see {@link VersionStore}
 */
public class TracingVersionStore<VALUE, METADATA> implements VersionStore<VALUE, METADATA> {

  private final VersionStore<VALUE, METADATA> delegate;
  private final Supplier<Tracer> tracerSupplier;

  /**
   * Calls {@link #TracingVersionStore(VersionStore, Supplier)} with the registered
   * {@link GlobalTracer#get() GlobalTracer}.
   * @param delegate backing/delegate {@link VersionStore}
   */
  public TracingVersionStore(VersionStore<VALUE, METADATA> delegate) {
    this(delegate, GlobalTracer::get);
  }

  /**
   * Constructor taking the backing/delegate {@link VersionStore} and a {@link Supplier} for
   * the {@link Tracer} to use.
   * @param delegate backing/delegate {@link VersionStore}
   * @param tracerSupplier {@link Supplier} for the {@link Tracer} to use
   */
  @VisibleForTesting
  public TracingVersionStore(VersionStore<VALUE, METADATA> delegate, Supplier<Tracer> tracerSupplier) {
    this.tracerSupplier = tracerSupplier;
    this.delegate = delegate;
  }

  @Override
  @Nonnull
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    return delegate1Ex("VersionStore.toHash", b -> b
        .withTag("nessie.version-store.operation", "ToHash")
        .withTag("nessie.version-store.ref", safeRefName(ref)),
        () -> delegate.toHash(ref));
  }

  @Override
  public WithHash<Ref> toRef(@Nonnull String refOfUnknownType) throws ReferenceNotFoundException {
    return delegate1Ex("VersionStore.toRef", b -> b
        .withTag("nessie.version-store.operation", "ToRef")
        .withTag("nessie.version-store.ref", refOfUnknownType),
        () -> delegate.toRef(refOfUnknownType));
  }

  @Override
  public void commit(@Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<VALUE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("VersionStore.commit", b -> b
        .withTag("nessie.version-store.operation", "Commit")
        .withTag("nessie.version-store.branch", safeRefName(branch))
        .withTag("nessie.version-store.hash", safeToString(referenceHash))
        .withTag("nessie.version-store.num-ops", safeSize(operations)),
        () -> delegate.commit(branch, referenceHash, metadata, operations));
  }

  @Override
  public void transplant(BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("VersionStore.transplant", b -> b
        .withTag("nessie.version-store.operation", "Transplant")
        .withTag("nessie.version-store.target-branch", safeRefName(targetBranch))
        .withTag("nessie.version-store.hash", safeToString(referenceHash))
        .withTag("nessie.version-store.transplants", safeSize(sequenceToTransplant)),
        () -> delegate.transplant(targetBranch, referenceHash, sequenceToTransplant));
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch,
      Optional<Hash> expectedHash) throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("VersionStore.merge", b -> b
        .withTag("nessie.version-store.operation", "Merge")
        .withTag("nessie.version-store.from-hash", safeToString(fromHash))
        .withTag("nessie.version-store.to-branch", safeRefName(toBranch))
        .withTag("nessie.version-store.expected-hash", safeToString(expectedHash)),
        () -> delegate.merge(fromHash, toBranch, expectedHash));
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash,
      Hash targetHash) throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("VersionStore.assign", b -> b
        .withTag("nessie.version-store.operation", "Assign")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.expected-hash", safeToString(expectedHash))
        .withTag("nessie.version-store.target-hash", safeToString(targetHash)),
        () -> delegate.assign(ref, expectedHash, targetHash));
  }

  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    this.<ReferenceNotFoundException, ReferenceAlreadyExistsException>delegate2Ex("VersionStore.create", b -> b
        .withTag("nessie.version-store.operation", "Create")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.target-hash", safeToString(targetHash)),
        () -> delegate.create(ref, targetHash));
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("VersionStore.delete", b -> b
        .withTag("nessie.version-store.operation", "Delete")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.hash", safeToString(hash)),
        () -> delegate.delete(ref, hash));
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return delegateStream("VersionStore.getNamedRefs", b -> b
        .withTag("nessie.version-store.operation", "GetNamedRefs"),
        delegate::getNamedRefs);
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    return delegateStream1Ex("VersionStore.getCommits", b -> b
        .withTag("nessie.version-store.operation", "GetCommits")
        .withTag("nessie.version-store.ref", safeToString(ref)),
        () ->  delegate.getCommits(ref));
  }

  @Override
  public Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException {
    return delegateStream1Ex("VersionStore.getKeys", b -> b
        .withTag("nessie.version-store.operation", "GetKeys")
        .withTag("nessie.version-store.ref", safeToString(ref)),
        () -> delegate.getKeys(ref));
  }

  @Override
  public VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return delegate1Ex("VersionStore.getValue", b -> b
        .withTag("nessie.version-store.operation", "GetValue")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.key", safeToString(key)),
        () -> delegate.getValue(ref, key));
  }

  @Override
  public List<Optional<VALUE>> getValues(Ref ref, List<Key> keys) throws ReferenceNotFoundException {
    return delegate1Ex("VersionStore.getValues", b -> b
        .withTag("nessie.version-store.operation", "GetValues")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.keys", safeToString(keys)),
        () -> delegate.getValues(ref, keys));
  }

  @Override
  public Stream<Diff<VALUE>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    return delegateStream1Ex("VersionStore.getDiffs", b -> b
        .withTag("nessie.version-store.operation", "GetDiffs")
        .withTag("nessie.version-store.from", safeToString(from))
        .withTag("to", safeToString(to)),
        () -> delegate.getDiffs(from, to));
  }

  @Override
  public Collector collectGarbage() {
    return delegate("VersionStore.collectGarbage", b -> b
        .withTag("nessie.version-store.operation", "CollectGarbage"),
        delegate::collectGarbage);
  }

  private Scope createActiveScope(String spanName, Consumer<SpanBuilder> spanBuilder) {
    Tracer tracer = tracerSupplier.get();
    SpanBuilder builder = tracer.buildSpan(spanName)
        .asChildOf(tracer.activeSpan());
    spanBuilder.accept(builder);
    return builder.startActive(true);
  }

  private <R> Stream<R> delegateStream(String spanName, Consumer<SpanBuilder> spanBuilder, Delegate<Stream<R>> delegate) {
    Scope scope = createActiveScope(spanName, spanBuilder);
    Stream<R> result = null;
    try {
      result = delegate.handle().onClose(scope::close);
      return result;
    } catch (RuntimeException e) {
      throw traceRuntimeException(scope, e);
    } finally {
      // See below (delegateStream1Ex)
      if (result == null) {
        scope.close();
      }
    }
  }

  private <R, E1 extends VersionStoreException> Stream<R> delegateStream1Ex(String spanName, Consumer<SpanBuilder> spanBuilder,
      DelegateWith1<Stream<R>, E1> delegate) throws E1 {
    Scope scope = createActiveScope(spanName, spanBuilder);
    Stream<R> result = null;
    try {
      result = delegate.handle().onClose(scope::close);
      return result;
    } catch (RuntimeException e) {
      throw traceRuntimeException(scope, e);
    } finally {
      // We cannot `catch (E1 e)`, so assume that the delegate threw an exception, when result==null
      // and then close the trace-scope.
      if (result == null) {
        scope.close();
      }
    }
  }

  private <R> R delegate(String spanName, Consumer<SpanBuilder> spanBuilder, Delegate<R> delegate) {
    try (Scope scope = createActiveScope(spanName, spanBuilder)) {
      try {
        return delegate.handle();
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  private <R, E1 extends VersionStoreException> R delegate1Ex(String spanName, Consumer<SpanBuilder> spanBuilder,
      DelegateWith1<R, E1> delegate) throws E1 {
    try (Scope scope = createActiveScope(spanName, spanBuilder)) {
      try {
        return delegate.handle();
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  private <E1 extends VersionStoreException, E2 extends VersionStoreException> void delegate2Ex(String spanName,
      Consumer<SpanBuilder> spanBuilder, DelegateWith2<E1, E2> delegate) throws E1, E2 {
    try (Scope scope = createActiveScope(spanName, spanBuilder)) {
      try {
        delegate.handle();
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @FunctionalInterface
  interface Delegate<R> {
    R handle();
  }

  @FunctionalInterface
  interface DelegateWith1<R, E1 extends VersionStoreException> {
    R handle() throws E1;
  }

  @FunctionalInterface
  interface DelegateWith2<E1 extends VersionStoreException, E2 extends VersionStoreException> {
    void handle() throws E1, E2;
  }

  private static String safeToString(Object o) {
    return o != null ? o.toString() : "<null>";
  }

  private static String safeRefName(NamedRef ref) {
    return ref != null ? ref.getName() : "<null>";
  }

  private static int safeSize(Collection<?> collection) {
    return collection != null ? collection.size() : -1;
  }

  private static RuntimeException traceRuntimeException(Scope scope, RuntimeException e) {
    Tags.ERROR.set(scope.span().log(ImmutableMap.of(Fields.EVENT, Tags.ERROR.getKey(),
        Fields.ERROR_OBJECT, e.toString())), true);
    return e;
  }
}
