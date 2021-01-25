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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;

public class RocksL1 extends RocksBaseValue<L1> implements L1, Evaluator {

  static final int SIZE = 43;
  private Id metadataId; // commitMetadataId
  private Stream<Id> parentList; // ancestors
  private Stream<Id> tree; // children

  private Stream<Key.Mutation> keyMutations; // keylist
  private Id checkpointId; // incrementalKeyList
  private int distanceFromCheckpoint; // incrementalKeyList
  private Stream<Id> fragmentIds; // completeKeyList

  private static final String COMMIT_METADATA = "metadataId";
  private static final String ANCESTORS = "ancestors";
  private static final String CHILDREN = "children";
  private static final String KEY_LIST = "keylist";
  private static final String INCREMENTAL_KEY_LIST = "incrementalKeyList";
  private static final String COMPLETE_KEY_LIST = "completeKeyList";

  static RocksL1 EMPTY =
      new RocksL1(Id.EMPTY, null, null, null, null, 0L);

  static Id EMPTY_ID = EMPTY.getId();

  public RocksL1() {
    super(EMPTY_ID, 0);
  }

  // TODO: convert streams to maps
  private RocksL1(Id commitId, Stream<Id> tree, Id id, Stream<Key.Mutation> keyList, Stream<Id> parentList, Long dt) {
    super(id, dt);
    this.metadataId = commitId;
    this.parentList = parentList;
    this.keyMutations = keyList;
    this.tree = tree;

    //    if (tree.size() != SIZE) {
    //      throw new AssertionError("tree.size(" + tree.size() + ") != " + SIZE);
    //    }
    //    if (id != null && !id.equals(generateId())) {
    //      throw new AssertionError("wrong id=" + id + ", expected=" + generateId());
    //    }
  }

  @Override
  public L1 commitMetadataId(Id id) {
    this.metadataId = id;
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    this.parentList = ids;
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    this.tree = ids;
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
    return metadataId;
  }

  @Override
  public boolean evaluate(Condition condition) {
    for (Function function: condition.functionList) {
      // Retrieve entity at function.path
      List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));
      for (String segment : path) {
        if (segment.equals(COMMIT_METADATA)) {
          if ((path.size() == 1)
            && (function.getOperator().equals(Function.EQUALS))
            && (!this.metadataId.toEntity().equals(function.getValue()))) {
            return false;
          }
        } else if (segment.equals(ANCESTORS)) {
          // Is a Stream

        } else if (segment.equals(CHILDREN)) {
          return false;
        } else if (segment.equals(KEY_LIST)) {
          return false;
        } else if (segment.equals(INCREMENTAL_KEY_LIST)) {
          return false;
        } else if (segment.equals(COMPLETE_KEY_LIST)) {
          return false;
        }
      }
    }
    return true;
  }

  public Stream<Id> getAncestors() {
    return parentList;
  }

  public Stream<Id> getChildren() {
    return tree;
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
