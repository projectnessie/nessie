/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.batching;

import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Persist;

@Value.Immutable
public interface WriteBatching {
  int DEFAULT_BATCH_SIZE = 100;
  boolean DEFAULT_OPTIMISTIC = true;

  static ImmutableWriteBatching.Builder builder() {
    return ImmutableWriteBatching.builder();
  }

  Persist persist();

  /**
   * The maximum number of objects to be stored in a single batch.
   *
   * <p>A value of {@code 0} or fewer means "infinite batching" and effectively disables flushes,
   * preventing all writes from being persisted. This can be useful for testing in dry-run mode, but
   * should not be used in production.
   */
  @Value.Default
  default int batchSize() {
    return DEFAULT_BATCH_SIZE;
  }

  /**
   * Optimistic batching means, that the {@link BatchingPersist batching} implementation assumes
   * that objects to be stored do not already exist, all row-level checks are not in effect, the
   * {@link BatchingPersist batching} implementation effectively "trusts" the called to do the right
   * thing.
   *
   * <p><em>IMPORTANT NOTE:</em> there is currently no implementation that supports strict checks
   * that would provide the same guarantees as live-production {@code Persist} implementations.
   */
  @Value.Default
  default boolean optimistic() {
    return DEFAULT_OPTIMISTIC;
  }

  default BatchingPersist create() {
    return new BatchingPersistImpl(this);
  }
}
