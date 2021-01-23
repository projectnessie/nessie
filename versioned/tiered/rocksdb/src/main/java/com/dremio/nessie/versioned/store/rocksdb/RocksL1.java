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
package com.dremio.nessie.versioned.store.rocksdb;

import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;

public class RocksL1 extends RocksBaseValue<L1> implements L1 {

  private Id commitMetadataId;
  private Stream<Id> ancestors;
  private Stream<Id> children;

  private Stream<Key.Mutation> keyMutations;
  private Id checkpointId;
  private int distanceFromCheckpoint;
  private Stream<Id> fragmentIds;

  @Override
  public L1 commitMetadataId(Id id) {
    this.commitMetadataId = id;
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    this.ancestors = ids;
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    this.children = ids;
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    this.keyMutations = keyMutations;
    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    this.checkpointId = checkpointId;
    this.distanceFromCheckpoint = distanceFromCheckpoint;
    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    this.fragmentIds = fragmentIds;
    return this;
  }

  public Id getMetadataId() {
    return commitMetadataId;
  }

  public Stream<Id> getAncestors() {
    return ancestors;
  }

  public Stream<Id> getChildren() {
    return children;
  }

  public Stream<Key.Mutation> getKeyMutations() {
    return keyMutations;
  }

  public Id getCheckpointId() {
    return checkpointId;
  }

  public int getDistanceFromCheckpoint() {
    return distanceFromCheckpoint;
  }

  public Stream<Id> getFragmentIds() {
    return fragmentIds;
  }
}
