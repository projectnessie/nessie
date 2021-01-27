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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
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

  public static final String COMMIT_METADATA = "metadataId";
  public static final String ANCESTORS = "ancestors";
  public static final String CHILDREN = "children";
  public static final String KEY_LIST = "keylist";
  public static final String INCREMENTAL_KEY_LIST = "incrementalKeyList";
  public static final String COMPLETE_KEY_LIST = "completeKeyList";
  public static final String CHECKPOINT_ID = "checkpointId";
  public static final String DISTANCE_FROM_CHECKPOINT = "distanceFromCheckpoint";

  static RocksL1 EMPTY =
      new RocksL1(Id.EMPTY, null, null, null, null, 0L);

  static Id EMPTY_ID = EMPTY.getId();

  public RocksL1() {
    super(EMPTY_ID, 0);
  }

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
      String segment = path.get(0);
      if (segment.equals(COMMIT_METADATA)) {
        return ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (metadataId.toEntity().equals(function.getValue())));
      } else if (segment.startsWith(ANCESTORS)) {
        return evaluateStream(function, parentList);
      } else if (segment.startsWith(CHILDREN)) {
        return evaluateStream(function, tree);
      } else if (segment.equals(KEY_LIST)) {
        return false;
      } else if (segment.equals(INCREMENTAL_KEY_LIST)) {
        if (path.size() == 2 && (function.getOperator().equals(Function.EQUALS))) {
          if (path.get(1).equals(CHECKPOINT_ID)) {
            return (checkpointId.toEntity().equals(function.getValue()));
          } else if (path.get(1).equals((DISTANCE_FROM_CHECKPOINT))) {
            return (Entity.ofNumber(distanceFromCheckpoint).equals(function.getValue()));
          }
        }
      } else if (segment.startsWith(COMPLETE_KEY_LIST)) {
        return evaluateStream(function, fragmentIds);
      }
    }
    return false;
  }

  /**
   * Evaluates if the stream of Id meets the Condition Function
   * @param function the function
   * @param stream
   * @return
   */
  boolean evaluateStream(Function function, Stream<Id> stream) {
    // stream is a list. EQUALS will either compare a specified position or the whole list.
    List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));
    String segment = path.get(0);
    if (path.size() == 1) {
      if (function.getOperator().equals(Function.EQUALS)) {
        List<String> arguments = splitArrayString(segment);
        if (arguments.size() == 1) { // compare complete list
          return (toEntity(stream).equals(function.getValue()));
        } else if (arguments.size() == 2) { // compare individual element of list
          int position = Integer.parseInt(arguments.get(1));
          return (toEntity(stream, position).equals(function.getValue()));
        }
      } else if (function.getOperator().equals(Function.SIZE)) {
        return (tree.collect(Collectors.toList()).size() == (int)function.getValue().getNumber());
      }
    }
    return false;
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
