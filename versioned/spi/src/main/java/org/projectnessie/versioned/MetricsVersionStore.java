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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;

/**
 * A {@link VersionStore} wrapper that publishes metrics via Micrometer.
 *
 * @param <VALUE> see {@link VersionStore}
 * @param <METADATA> see {@link VersionStore}
 */
public final class MetricsVersionStore<VALUE, METADATA, VALUE_TYPE extends Enum<VALUE_TYPE>>
    implements VersionStore<VALUE, METADATA, VALUE_TYPE> {

  private final VersionStore<VALUE, METADATA, VALUE_TYPE> delegate;
  private final MeterRegistry registry;
  private final Clock clock;
  private final Iterable<Tag> commonTags;

  /**
   * Constructor taking the delegate version-store, the common tags for the emitted metrics and
   * the metrics-registry.
   *
   * @param delegate delegate version-store
   * @param metricsCommonTags common metrics tags
   * @param registry metrics-registry
   */
  MetricsVersionStore(VersionStore<VALUE, METADATA, VALUE_TYPE> delegate, Map<String, String> metricsCommonTags,
      MeterRegistry registry, Clock clock) {
    this.delegate = delegate;
    this.registry = registry;
    this.clock = clock;
    this.commonTags = metricsCommonTags.entrySet().stream()
        .map(e -> Tag.of(e.getKey(), e.getValue())).collect(Collectors.toList());

    gauges().forEach((name, gauge) -> Gauge.builder("nessie.version-store." + name, gauge).tags(commonTags).register(registry));
  }

  public MetricsVersionStore(VersionStore<VALUE, METADATA, VALUE_TYPE> delegate, Map<String, String> metricsCommonTags) {
    this(delegate, metricsCommonTags, Metrics.globalRegistry, Clock.SYSTEM);
  }

  @Override
  @Nonnull
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    return delegate1Ex("to-hash", () -> delegate.toHash(ref));
  }

  @Override
  public WithHash<Ref> toRef(@Nonnull String refOfUnknownType) throws ReferenceNotFoundException {
    return delegate1Ex("to-ref", () -> delegate.toRef(refOfUnknownType));
  }

  @Override
  public void commit(@Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<VALUE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("commit",
        () -> delegate.commit(branch, referenceHash, metadata, operations));
  }

  @Override
  public void transplant(BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("transplant",
        () -> delegate.transplant(targetBranch, referenceHash, sequenceToTransplant));
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch,
      Optional<Hash> expectedHash) throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("merge",
        () -> delegate.merge(fromHash, toBranch, expectedHash));
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash,
      Hash targetHash) throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("assign",
        () -> delegate.assign(ref, expectedHash, targetHash));
  }

  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    this.<ReferenceNotFoundException, ReferenceAlreadyExistsException>delegate2Ex("create",
        () -> delegate.create(ref, targetHash));
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    this.<ReferenceNotFoundException, ReferenceConflictException>delegate2Ex("delete",
        () -> delegate.delete(ref, hash));
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return delegateStream("get-named-refs", delegate::getNamedRefs);
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    return delegateStream1Ex("get-commits", () -> delegate.getCommits(ref));
  }

  @Override
  public Stream<WithType<Key, VALUE_TYPE>> getKeys(Ref ref) throws ReferenceNotFoundException {
    return delegateStream1Ex("get-keys", () -> delegate.getKeys(ref));
  }

  @Override
  public VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return delegate1Ex("get-value", () -> delegate.getValue(ref, key));
  }

  @Override
  public List<Optional<VALUE>> getValues(Ref ref,
      List<Key> keys) throws ReferenceNotFoundException {
    return delegate1Ex("get-values", () -> delegate.getValues(ref, keys));
  }

  @Override
  public Stream<Diff<VALUE>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    return delegateStream1Ex("get-diffs", () -> delegate.getDiffs(from, to));
  }

  @Override
  public Collector collectGarbage() {
    return delegate("collect-garbage", delegate::collectGarbage);
  }

  @Override
  public Map<String, Supplier<Number>> gauges() {
    return delegate.gauges();
  }

  private void measure(String requestName, Sample sample, Exception failure) {
    Timer timer = Timer.builder("nessie.version-store.request")
        .tags(commonTags)
        .tag("request", requestName)
        .tag("error", Boolean.toString(failure != null))
        .publishPercentileHistogram()
        .distributionStatisticBufferLength(3)
        .distributionStatisticExpiry(Duration.ofMinutes(1))
        .register(registry);
    sample.stop(timer);
  }

  private <R> Stream<R> delegateStream(String requestName, Delegate<Stream<R>> delegate) {
    Sample sample = Timer.start(clock);
    try {
      return delegate.handle().onClose(() -> measure(requestName, sample, null));
    } catch (IllegalArgumentException e) {
      // IllegalArgumentException indicates a user-error, not a server error
      measure(requestName, sample, null);
      throw e;
    } catch (RuntimeException e) {
      measure(requestName, sample, e);
      throw e;
    }
  }

  private <R> Stream<R> delegateStream1Ex(String requestName,
      DelegateWith1<Stream<R>, ReferenceNotFoundException> delegate) throws ReferenceNotFoundException {
    Sample sample = Timer.start(clock);
    try {
      return delegate.handle().onClose(() -> measure(requestName, sample, null));
    } catch (IllegalArgumentException | ReferenceNotFoundException e) {
      // IllegalArgumentException indicates a user-error, not a server error
      measure(requestName, sample, null);
      throw e;
    } catch (RuntimeException e) {
      measure(requestName, sample, e);
      throw e;
    }
  }

  private <R> R delegate(String requestName, Delegate<R> delegate) {
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

  private <R, E1 extends VersionStoreException> R delegate1Ex(String requestName,
      DelegateWith1<R, E1> delegate) throws E1 {
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

  private <E1 extends VersionStoreException, E2 extends VersionStoreException> void delegate2Ex(String requestName,
      DelegateWith2<E1, E2> delegate) throws E1, E2 {
    Sample sample = Timer.start(clock);
    Exception failure = null;
    try {
      delegate.handle();
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
}
