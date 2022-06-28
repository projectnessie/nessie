/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.persist.nontx;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.Function;

final class BatchSpliterator<SRC, DST> extends AbstractSpliterator<DST> {

  private final Spliterator<SRC> source;
  private final int batchSize;
  private List<SRC> batch;
  private final Function<List<SRC>, Spliterator<DST>> batchMapper;

  private SRC nextSource;
  private boolean sourceEof;

  private void setNextSource(SRC nextSource) {
    this.nextSource = nextSource;
  }

  private DST nextMapped;
  private boolean mappedEof;

  private void setNextMapped(DST nextMapped) {
    this.nextMapped = nextMapped;
  }

  private Spliterator<DST> mapped = Spliterators.emptySpliterator();

  BatchSpliterator(
      int batchSize,
      Spliterator<SRC> source,
      Function<List<SRC>, Spliterator<DST>> batchMapper,
      int characteristics) {
    super(Long.MAX_VALUE, characteristics);
    this.batchSize = batchSize;
    this.batch = new ArrayList<>();
    this.source = source;
    this.batchMapper = batchMapper;
  }

  @Override
  public boolean tryAdvance(Consumer<? super DST> action) {

    while (true) {
      if (nextMapped != null) {
        try {
          action.accept(nextMapped);
          return true;
        } finally {
          nextMapped = null;
        }
      }

      if (!mappedEof && !mapped.tryAdvance(this::setNextMapped)) {
        mappedEof = true;
      }
      if (nextMapped != null) {
        continue;
      }
      if (sourceEof) {
        return false;
      }

      while (true) {
        if (!source.tryAdvance(this::setNextSource)) {
          sourceEof = true;
        }

        if (nextSource != null) {
          batch.add(nextSource);
          nextSource = null;
        }

        if (batch.size() == batchSize || sourceEof) {
          mappedEof = false;
          try {
            mapped = batch.isEmpty() ? Spliterators.emptySpliterator() : batchMapper.apply(batch);
          } finally {
            this.batch = new ArrayList<>(batchSize);
          }
          break;
        }
      }
    }
  }
}
