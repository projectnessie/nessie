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
package org.projectnessie.versioned.rocksdb;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.L1;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.L1} providing
 * SerDe and Condition evaluation.
 */
class RocksL1 extends RocksBaseValue<L1> implements L1 {

  static final int SIZE = 43;
  static final String COMMIT_METADATA = "metadataId";
  static final String ANCESTORS = "ancestors";
  static final String CHILDREN = "children";
  static final String KEY_LIST = "keylist";
  static final String INCREMENTAL_KEY_LIST = "incrementalKeyList";
  static final String COMPLETE_KEY_LIST = "completeKeyList";
  static final String CHECKPOINT_ID = "checkpointId";
  static final String DISTANCE_FROM_CHECKPOINT = "distanceFromCheckpoint";

  private Id metadataId; // commitMetadataId
  private List<Id> parentList; // ancestors
  private List<Id> tree; // children

  private List<Key.Mutation> keyMutations; // keylist
  private Id checkpointId; // checkpointId
  private int distanceFromCheckpoint; // distanceFromCheckpoint
  private List<Id> fragmentIds; // completeKeyList

  RocksL1() {
    super();
  }

  @Override
  public L1 commitMetadataId(Id id) {
    this.metadataId = id;
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    this.parentList = ids.collect(Collectors.toList());
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    this.tree = ids.collect(Collectors.toList());
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    this.keyMutations = keyMutations.collect(Collectors.toList());
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
    this.fragmentIds = fragmentIds.collect(Collectors.toList());
    return this;
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final ExpressionPath.NameSegment nameSegment = function.getRootPathAsNameSegment();
    final String segment = nameSegment.getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case COMMIT_METADATA:
        if (!function.isRootNameSegmentChildlessAndEquals()
          || !metadataId.toEntity().equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case ANCESTORS:
        evaluate(function, parentList);
        break;
      case CHILDREN:
        evaluate(function, tree);
        break;
      case KEY_LIST:
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
      case INCREMENTAL_KEY_LIST:
        if (!nameSegment.getChild().isPresent() || !function.getOperator().equals(Function.Operator.EQUALS)) {
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        }
        final String childName = nameSegment.getChild().get().asName().getName();
        if (childName.equals(CHECKPOINT_ID)) {
          if (!checkpointId.toEntity().equals(function.getValue())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else if (childName.equals((DISTANCE_FROM_CHECKPOINT))) {
          if (!Entity.ofNumber(distanceFromCheckpoint).equals(function.getValue())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else {
          // Invalid Condition Function.
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        }
        break;
      case COMPLETE_KEY_LIST:
        evaluate(function, fragmentIds);
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  @Override
  byte[] build() {
    checkPresent(metadataId, COMMIT_METADATA);
    checkPresent(parentList, ANCESTORS);
    checkPresent(tree, CHILDREN);
    checkPresent(keyMutations, KEY_LIST);

    final ValueProtos.L1.Builder builder = ValueProtos.L1.newBuilder()
        .setBase(buildBase())
        .setMetadataId(metadataId.getValue())
        .addAllAncestors(buildIds(parentList))
        .addAllTree(buildIds(tree))
        .addAllKeyMutations(keyMutations.stream().map(RocksBaseValue::buildKeyMutation).collect(Collectors.toList()));

    if (null == fragmentIds) {
      checkPresent(checkpointId, CHECKPOINT_ID);
      builder.setIncrementalList(ValueProtos.IncrementalList.newBuilder()
          .setCheckpointId(checkpointId.getValue())
          .setDistanceFromCheckpointId(distanceFromCheckpoint)
          .build());
    } else {
      checkPresent(fragmentIds, COMPLETE_KEY_LIST);
      builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(buildIds(fragmentIds)).build());
    }

    return builder.build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, L1 consumer) {
    try {
      final ValueProtos.L1 l1 = ValueProtos.L1.parseFrom(value);
      setBase(consumer, l1.getBase());
      consumer
          .commitMetadataId(Id.of(l1.getMetadataId()))
          .ancestors(l1.getAncestorsList().stream().map(Id::of))
          .children(l1.getTreeList().stream().map(Id::of))
          .keyMutations(l1.getKeyMutationsList().stream().map(RocksBaseValue::createKeyMutation));
      if (l1.hasIncrementalList()) {
        final ValueProtos.IncrementalList incList = l1.getIncrementalList();
        consumer.incrementalKeyList(Id.of(incList.getCheckpointId()), incList.getDistanceFromCheckpointId());
      } else {
        consumer.completeKeyList(l1.getCompleteList().getFragmentIdsList().stream().map(Id::of));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt L1 value encountered when deserializing.", e);
    }
  }
}
