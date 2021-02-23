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

import com.google.common.base.Throwables;

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
public final class MetricsVersionStore<VALUE, METADATA> implements VersionStore<VALUE, METADATA> {

  private final VersionStore<VALUE, METADATA> delegate;
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
  MetricsVersionStore(VersionStore<VALUE, METADATA> delegate, Map<String, String> metricsCommonTags, MeterRegistry registry, Clock clock) {
    this.delegate = delegate;
    this.registry = registry;
    this.clock = clock;
    this.commonTags = metricsCommonTags.entrySet().stream()
        .map(e -> Tag.of(e.getKey(), e.getValue())).collect(Collectors.toList());

    gauges().forEach((name, gauge) -> Gauge.builder("nessie.version-store." + name, gauge).tags(commonTags).register(registry));
  }

  public MetricsVersionStore(VersionStore<VALUE, METADATA> delegate, Map<String, String> metricsCommonTags) {
    this(delegate, metricsCommonTags, Metrics.globalRegistry, Clock.SYSTEM);
  }

  @Override
  @Nonnull
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    return measureWithNotFound(() -> delegate.toHash(ref), "to-hash");
  }

  @Override
  public WithHash<Ref> toRef(@Nonnull String refOfUnknownType) throws ReferenceNotFoundException {
    return measureWithNotFound(() -> delegate.toRef(refOfUnknownType), "to-ref");
  }

  @Override
  public void commit(@Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<VALUE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    measureWithNotFoundAndConflict(() -> delegate.commit(branch, referenceHash, metadata, operations), "commit");
  }

  @Override
  public void transplant(BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    measureWithNotFoundAndConflict(() -> delegate.transplant(targetBranch, referenceHash, sequenceToTransplant), "transplant");
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch,
      Optional<Hash> expectedHash) throws ReferenceNotFoundException, ReferenceConflictException {
    measureWithNotFoundAndConflict(() -> delegate.merge(fromHash, toBranch, expectedHash), "merge");
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash,
      Hash targetHash) throws ReferenceNotFoundException, ReferenceConflictException {
    measureWithNotFoundAndConflict(() -> delegate.assign(ref, expectedHash, targetHash), "assign");
  }

  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    measureWithNotFoundAndAlreadyExists(() -> delegate.create(ref, targetHash), "create");
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    measureWithNotFoundAndConflict(() -> delegate.delete(ref, hash), "delete");
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    Sample sample = Timer.start(clock);
    try {
      return delegate.getNamedRefs().onClose(() -> measure("get-named-refs", sample, null));
    } catch (RuntimeException e) {
      measure("get-named-refs", sample, e);
      throw e;
    }
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    Sample sample = Timer.start(clock);
    try {
      return delegate.getCommits(ref).onClose(() -> measure("get-commits", sample, null));
    } catch (RuntimeException | ReferenceNotFoundException e) {
      measure("get-commits", sample, e);
      throw e;
    }
  }

  @Override
  public Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException {
    Sample sample = Timer.start(clock);
    try {
      return delegate.getKeys(ref).onClose(() -> measure("get-keys", sample, null));
    } catch (RuntimeException | ReferenceNotFoundException e) {
      measure("get-keys", sample, e);
      throw e;
    }
  }

  @Override
  public VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return measureWithNotFound(() -> delegate.getValue(ref, key), "get-value");
  }

  @Override
  public List<Optional<VALUE>> getValues(Ref ref,
      List<Key> keys) throws ReferenceNotFoundException {
    return measureWithNotFound(() -> delegate.getValues(ref, keys), "get-values");
  }

  @Override
  public Stream<Diff<VALUE>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    Sample sample = Timer.start(clock);
    try {
      return delegate.getDiffs(from, to).onClose(() -> measure("get-diffs", sample, null));
    } catch (RuntimeException | ReferenceNotFoundException e) {
      measure("get-diffs", sample, e);
      throw e;
    }
  }

  @Override
  public Collector collectGarbage() {
    return measureNoCheckedException(delegate::collectGarbage, "collect-garbage");
  }

  @Override
  public Map<String, Supplier<Number>> gauges() {
    return delegate.gauges();
  }

  @FunctionalInterface
  interface Work<R> {
    R work() throws Exception;
  }

  @FunctionalInterface
  interface WorkVoid {
    void work() throws Exception;
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

  private <R> R measureNoCheckedException(Work<R> work, String requestName) {
    Sample sample = Timer.start(clock);
    try {
      R r = work.work();
      measure(requestName, sample, null);
      return r;
    } catch (Exception e) {
      measure(requestName, sample, e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private <R> R measureWithNotFound(Work<R> work, String requestName) throws ReferenceNotFoundException {
    Sample sample = Timer.start(clock);
    try {
      R r = work.work();
      measure(requestName, sample, null);
      return r;
    } catch (ReferenceNotFoundException e) {
      measure(requestName, sample, e);
      throw e;
    } catch (Exception e) {
      measure(requestName, sample, e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private void measureWithNotFoundAndConflict(WorkVoid work, String requestName)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Sample sample = Timer.start(clock);
    try {
      work.work();
      measure(requestName, sample, null);
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      measure(requestName, sample, e);
      throw e;
    } catch (Exception e) {
      measure(requestName, sample, e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private void measureWithNotFoundAndAlreadyExists(WorkVoid work, String requestName)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    Sample sample = Timer.start(clock);
    try {
      work.work();
      measure(requestName, sample, null);
    } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException e) {
      measure(requestName, sample, e);
      throw e;
    } catch (Exception e) {
      measure(requestName, sample, e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
