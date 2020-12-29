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

import java.util.List;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;

/**
 * Interface to create an L1 Builder. To be implemented by each {@link Store} implementation.
 *
 * @param <T> The public class of this builder for fluent code development.
 */
public interface L1Consumer<T extends L1Consumer<T>> {

  /**
   * The commit metadata id for this l1.
   *
   * <p>Can be called once.
   * @param id The id to reference.
   * @return This consumer.
   */
  T commitMetadataId(Id id);

  /**
   * Add ancestors associated with this L1.
   *
   * <p>Can be called once.
   * @param ids A list of ancestors ordered by my recent to oldest.
   * @return This consumer.
   */
  T addAncestors(List<Id> ids);

  /**
   * Add a list of children ids indexed by position.
   *
   * <p>Can be called once.
   * @param ids The list of ids. List must be {@link L1.SIZE} in length.
   * @return This consumer.
   */
  T children(List<Id> ids);

  /**
   * The id defined for this L1.
   *
   * <p>Can be called once.
   * @param id The id.
   * @return This consumer.
   */
  T id(Id id);

  /**
   * Add a key that was added as part of this commit.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param key The key that was added.
   * @return This consumer.
   */
  T addKeyAddition(Key key);

  /**
   * Add a key that was removed as part of this commit.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param key The key that was added.
   * @return This consumer.
   */
  T addKeyRemoval(Key key);

  /**
   * States that this L1 has an incremental key list. Can only be called once and cannot be called if {@link completelKeyList is called}.
   *
   * <p>Can be called once.
   *
   * @param checkpointId The id of the last checkpoint.
   * @param distanceFromCheckpoint The number of commits between this commit and the last checkpoint.
   * @return This consumer.
   */
  T incrementalKeyList(Id checkpointId, int distanceFromCheckpoint);

  /**
   * States that this L1 has a complete key list. Can only be called once and cannot be called if {@link incrementalKeyList is called}.
   *
   * <p>Add a list of fragments associated with this complete list of keys.
   * @param fragmentIds The ids of each of the key list fragments given in a meaningful/to be maintained order.
   * @return This consumer.
   */
  T completeKeyList(List<Id> fragmentIds);

}
