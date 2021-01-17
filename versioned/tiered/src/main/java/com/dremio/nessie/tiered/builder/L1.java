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
package com.dremio.nessie.tiered.builder;

import java.util.stream.Stream;

import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;

/**
 * Interface to create an L1 Builder. To be implemented by each
 * {@link com.dremio.nessie.versioned.store.Store} implementation.
 */
public interface L1 extends BaseValue<L1> {

  /**
   * The commit metadata id for this l1.
   * <p>Must be called exactly once.</p>
   *
   * @param id The id to reference.
   * @return This consumer.
   */
  L1 commitMetadataId(Id id);

  /**
   * Add ancestors associated with this L1.
   * <p>Must be called exactly once.</p>
   *
   * @param ids A list of ancestors ordered by most recent recent first.
   * @return This consumer.
   */
  L1 ancestors(Stream<Id> ids);

  /**
   * Add a list of children ids indexed by position.
   * <p>Must be called exactly once.</p>
   *
   * @param ids The list of ids. List must be {@link com.dremio.nessie.versioned.impl.InternalL1#SIZE} in length.
   * @return This consumer.
   */
  L1 children(Stream<Id> ids);

  /**
   * Keys that were added and removed as part of this commit.
   * <p>Can only be called once.</p>
   *
   * @param keyMutations The key that was added.
   * @return This consumer.
   */
  L1 keyMutations(Stream<Key.Mutation> keyMutations);

  /**
   * States that this L1 has an incremental key list.
   * Can only be called once and cannot be called if {@link #completeKeyList(Stream)} is called.
   * <p>Either this method or {@link #completeKeyList(Stream)} can only be called exactly once.</p>
   *
   * @param checkpointId The id of the last checkpoint.
   * @param distanceFromCheckpoint The number of commits between this commit and the last checkpoint.
   * @return This consumer.
   */
  L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint);

  /**
   * States that this L1 has a complete key list.
   * <p>Either this method or {@link #incrementalKeyList} can only be called exactly once.</p>
   *
   * <p>Add a list of fragments associated with this complete list of keys.
   * @param fragmentIds The ids of each of the key list fragments given in a meaningful/to be maintained order.
   * @return This consumer.
   */
  L1 completeKeyList(Stream<Id> fragmentIds);

}
