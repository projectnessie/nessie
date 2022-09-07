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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachmentKey;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/**
 * Buffers a configurable amount of objects and passes those as batches to a consumer, should be
 * used in a <em>try-with-resource</em>.
 */
final class BatchWriter<K, T> implements AutoCloseable {
  private final Map<K, T> buffer;
  private final int capacity;
  private final Consumer<List<T>> flush;
  private final Function<T, K> keyExtractor;

  static BatchWriter<Hash, CommitLogEntry> commitBatchWriter(
      int batchSize, DatabaseAdapter databaseAdapter) {
    return new BatchWriter<>(
        batchSize,
        entries -> {
          try {
            databaseAdapter.writeMultipleCommits(entries);
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        CommitLogEntry::getHash);
  }

  static BatchWriter<ContentAttachmentKey, ContentAttachment> attachmentsBatchWriter(
      int batchSize, DatabaseAdapter databaseAdapter) {
    return new BatchWriter<>(
        batchSize,
        attachments -> databaseAdapter.putAttachments(attachments.stream()),
        ContentAttachment::getKey);
  }

  BatchWriter(int capacity, Consumer<List<T>> flush, Function<T, K> keyExtractor) {
    this.capacity = capacity;
    this.flush = flush;
    this.keyExtractor = keyExtractor;
    this.buffer = Maps.newHashMapWithExpectedSize(capacity);
  }

  void add(T entity) {
    K key = keyExtractor.apply(entity);
    buffer.put(key, entity);
    if (buffer.size() == capacity) {
      flush();
    }
  }

  T useBuffered(T other) {
    K key = keyExtractor.apply(other);
    return buffer.getOrDefault(key, other);
  }

  T get(K key) {
    return buffer.get(key);
  }

  private void flush() {
    if (!buffer.isEmpty()) {
      ArrayList<T> list = new ArrayList<>(buffer.values());
      buffer.clear();
      flush.accept(list);
    }
  }

  @Override
  public void close() {
    flush();
  }
}
