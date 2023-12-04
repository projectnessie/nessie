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
package org.projectnessie.versioned.transfer;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Buffers a configurable amount of objects and passes those as batches to a consumer, should be
 * used in a <em>try-with-resource</em>.
 */
final class Batcher<T> implements AutoCloseable {
  private final Set<T> buffer;
  private final int capacity;
  private final Consumer<List<T>> flush;

  Batcher(int capacity, Consumer<List<T>> flush) {
    this.capacity = capacity;
    this.flush = flush;
    this.buffer = Sets.newLinkedHashSetWithExpectedSize(capacity);
  }

  void add(T entity) {
    buffer.add(entity);
    if (buffer.size() == capacity) {
      flush();
    }
  }

  private void flush() {
    if (!buffer.isEmpty()) {
      ArrayList<T> list = new ArrayList<>(buffer);
      buffer.clear();
      flush.accept(list);
    }
  }

  @Override
  public void close() {
    flush();
  }
}
