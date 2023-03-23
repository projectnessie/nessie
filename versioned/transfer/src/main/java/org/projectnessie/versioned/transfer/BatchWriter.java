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
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * Buffers a configurable amount of objects and passes those as batches to a consumer, should be
 * used in a <em>try-with-resource</em>.
 */
final class BatchWriter<T> implements AutoCloseable {
  private final Set<T> buffer;
  private final int capacity;
  private final Consumer<List<T>> flush;

  static BatchWriter<CommitLogEntry> commitBatchWriter(
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
        });
  }

  static BatchWriter<ContentAttachment> attachmentsBatchWriter(
      int batchSize, DatabaseAdapter databaseAdapter) {
    return new BatchWriter<>(
        batchSize, attachments -> databaseAdapter.putAttachments(attachments.stream()));
  }

  static BatchWriter<Obj> objWriter(int batchSize, Persist persist) {
    return new BatchWriter<>(
        batchSize,
        objs -> {
          try {
            persist.storeObjs(objs.toArray(new Obj[0]));
          } catch (ObjTooLargeException e) {
            throw new RuntimeException(e);
          }
        });
  }

  BatchWriter(int capacity, Consumer<List<T>> flush) {
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
