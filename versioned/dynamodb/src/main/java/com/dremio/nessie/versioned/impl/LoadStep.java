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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.impl.LoadOp.LoadOpKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;

public class LoadStep {

  private Collection<LoadOp<?>> ops;
  private Supplier<Optional<LoadStep>> next;

  public LoadStep(Collection<LoadOp<?>> ops, Supplier<Optional<LoadStep>> next) {
    this.ops = consolidate(ops);
    this.next = next;
  }

  private static Collection<LoadOp<?>> consolidate(Collection<LoadOp<?>> ops) {
    // dynamodb doesn't let a single request ask for the same value multiple times. We need to collapse any loadops that do this.
    ListMultimap<LoadOpKey, LoadOp<?>> mm = Multimaps.index(ImmutableList.copyOf(ops), LoadOp::toKey);
    List<LoadOp<?>> consolidated = mm.keySet().stream().map(key -> mm.get(key).stream().collect(LoadOp.toLoadOp())).collect(ImmutableList.toImmutableList());
    return consolidated;
  }

  public LoadStep combine(final LoadStep b) {
    final LoadStep a = this;
    Collection<LoadOp<?>> newOps = Streams.concat(ops.stream(), b.ops.stream()).collect(Collectors.toList());
    return new LoadStep(newOps, () -> {
      Optional<LoadStep> aNext = a.next.get();
      Optional<LoadStep> bNext = b.next.get();
      if(aNext.isPresent()) {
        if(bNext.isPresent()) {
          return Optional.of(aNext.get().combine(bNext.get()));
        }

        return aNext;
      }

      return bNext;
    });
  }

  public List<ListMultimap<String, LoadOp<?>>> paginateLoads(int size) {

    List<LoadOp<?>> ops = this.ops.stream().collect(Collectors.toList());

    List<ListMultimap<String, LoadOp<?>>> paginated = new ArrayList<>();
    for(int i =0; i < ops.size(); i+=size) {
      ListMultimap<String, LoadOp<?>> mm = Multimaps.index(ops.subList(i, Math.min(i + size, ops.size())), l -> l.getValueType().getTableName());
      paginated.add(mm);
    }
    return paginated;
  }

  public Optional<LoadStep> getNext() {
    return next.get();
  }

  public static LoadStep of(LoadOp<?>...ops) {
    return new LoadStep(Arrays.asList(ops), () -> Optional.empty());
  }
}
