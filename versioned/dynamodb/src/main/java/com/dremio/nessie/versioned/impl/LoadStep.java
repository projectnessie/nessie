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
package com.dremio.nessie.versioned.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.impl.LoadOp.LoadOpKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;

class LoadStep {

  private Collection<LoadOp<?>> ops;
  private Supplier<Optional<LoadStep>> next;

  public LoadStep(Collection<LoadOp<?>> ops) {
    this(ops, () -> Optional.empty());
  }

  public LoadStep(Collection<LoadOp<?>> ops, Supplier<Optional<LoadStep>> next) {
    this.ops = consolidate(ops);
    this.next = next;
  }

  private static Collection<LoadOp<?>> consolidate(Collection<LoadOp<?>> ops) {
    // dynamodb doesn't let a single request ask for the same value multiple times. We need to collapse any loadops that do this.
    ListMultimap<LoadOpKey, LoadOp<?>> mm = Multimaps.index(ImmutableList.copyOf(ops), LoadOp::toKey);
    List<LoadOp<?>> consolidated = mm.keySet()
        .stream()
        .map(key -> mm.get(key).stream().collect(LoadOp.toLoadOp()))
        .collect(ImmutableList.toImmutableList());
    return consolidated;
  }

  Stream<LoadOp<?>> getOps() {
    return ops.stream();
  }

  public LoadStep combine(final LoadStep b) {
    final LoadStep a = this;
    Collection<LoadOp<?>> newOps = Streams.concat(ops.stream(), b.ops.stream()).collect(Collectors.toList());
    return new LoadStep(newOps, () -> {
      Optional<LoadStep> nextA = a.next.get();
      Optional<LoadStep> nextB = b.next.get();
      if (nextA.isPresent()) {
        if (nextB.isPresent()) {
          return Optional.of(nextA.get().combine(nextB.get()));
        }

        return nextA;
      }

      return nextB;
    });
  }

  public Optional<LoadStep> getNext() {
    return next.get();
  }

  public static LoadStep of(LoadOp<?>...ops) {
    return new LoadStep(Arrays.asList(ops), () -> Optional.empty());
  }

  public static Collector<LoadStep, StepCollectorState, LoadStep> toLoadStep() {
    return COLLECTOR;
  }

  private static final Collector<LoadStep, StepCollectorState, LoadStep> COLLECTOR = Collector.of(
      StepCollectorState::new,
      (o1, l1) -> o1.plus(l1),
      (o1, o2) -> o1.plus(o2),
      StepCollectorState::getStep
      );

  private static class StepCollectorState {

    private LoadStep step = new LoadStep(Collections.emptyList());

    private StepCollectorState() {
    }

    public LoadStep getStep() {
      return step;
    }

    public StepCollectorState plus(StepCollectorState s) {
      plus(s.step);
      return this;
    }

    public void plus(LoadStep s) {
      step = step.combine(s);
    }
  }
}
