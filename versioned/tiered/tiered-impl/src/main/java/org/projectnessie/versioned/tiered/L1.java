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
package org.projectnessie.versioned.tiered;

import java.util.stream.Stream;
import org.projectnessie.versioned.store.Id;

/**
 * Interface to create an L1 Builder. To be implemented by each {@link
 * org.projectnessie.versioned.store.Store} implementation.
 *
 * <p>Implementations must return a shared state ({@code this}) from its method.
 */
public interface L1 extends BaseValue<L1> {

  /**
   * The commit metadata id for this l1.
   *
   * <p>Must be called exactly once.
   *
   * @param id The id to reference.
   * @return This consumer.
   */
  default L1 commitMetadataId(Id id) {
    return this;
  }

  /**
   * Add ancestors associated with this L1.
   *
   * <p>Must be called exactly once.
   *
   * @param ids A list of ancestors ordered by most recent recent first.
   * @return This consumer.
   */
  default L1 ancestors(Stream<Id> ids) {
    ids.forEach(ignored -> {});
    return this;
  }

  /**
   * Add a list of children ids indexed by position.
   *
   * <p>Must be called exactly once.
   *
   * @param ids The list of ids. List must be {@link
   *     org.projectnessie.versioned.impl.InternalL1#SIZE} in length.
   * @return This consumer.
   */
  default L1 children(Stream<Id> ids) {
    ids.forEach(ignored -> {});
    return this;
  }

  /**
   * Keys that were added and removed as part of this commit.
   *
   * <p>Can only be called once.
   *
   * @param keyMutations The key that was added.
   * @return This consumer.
   */
  default L1 keyMutations(Stream<Mutation> keyMutations) {
    keyMutations.forEach(ignored -> {});
    return this;
  }

  /**
   * States that this L1 has an incremental key list. Can only be called once and cannot be called
   * if {@link #completeKeyList(Stream)} is called.
   *
   * <p>Either this method or {@link #completeKeyList(Stream)} can only be called exactly once.
   *
   * @param checkpointId The id of the last checkpoint.
   * @param distanceFromCheckpoint The number of commits between this commit and the last
   *     checkpoint.
   * @return This consumer.
   */
  default L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    return this;
  }

  /**
   * States that this L1 has a complete key list.
   *
   * <p>Either this method or {@link #incrementalKeyList} can only be called exactly once.
   *
   * <p>Add a list of fragments associated with this complete list of keys.
   *
   * @param fragmentIds The ids of each of the key list fragments given in a meaningful/to be
   *     maintained order.
   * @return This consumer.
   */
  default L1 completeKeyList(Stream<Id> fragmentIds) {
    fragmentIds.forEach(ignored -> {});
    return this;
  }
}
