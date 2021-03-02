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
package org.projectnessie.versioned.inmem;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key.Mutation;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.L1;

final class L1Obj extends BaseObj<L1> {
  static final String COMMIT_METADATA = "metadataId";
  static final String ANCESTORS = "ancestors";
  static final String CHILDREN = "children";
  static final String KEY_LIST = "keylist";
  static final String INCREMENTAL_KEY_LIST = "incrementalKeyList";
  static final String COMPLETE_KEY_LIST = "completeKeyList";
  static final String CHECKPOINT_ID = "checkpointId";
  static final String DISTANCE_FROM_CHECKPOINT = "distanceFromCheckpoint";

  private final Id commitMetadataId;
  private final List<Id> ancestors;
  private final List<Id> children;
  private final List<Mutation> keyMutations;
  private final int distanceFromCheckpoint;
  private final Id checkpointId;
  private final List<Id> completeKeyList;

  L1Obj(Id id, long dt, Id commitMetadataId,
      List<Id> ancestors, List<Id> children,
      List<Mutation> keyMutations, int distanceFromCheckpoint,
      Id checkpointId, List<Id> completeKeyList) {
    super(id, dt);
    this.commitMetadataId = commitMetadataId;
    this.ancestors = ancestors;
    this.children = children;
    this.keyMutations = keyMutations;
    this.distanceFromCheckpoint = distanceFromCheckpoint;
    this.checkpointId = checkpointId;
    this.completeKeyList = completeKeyList;
  }

  @Override
  L1 consume(L1 consumer) {
    super.consume(consumer)
        .commitMetadataId(commitMetadataId)
        .ancestors(ancestors.stream())
        .children(children.stream())
        .keyMutations(keyMutations.stream());
    if (completeKeyList != null) {
      return consumer.completeKeyList(completeKeyList.stream());
    } else {
      return consumer.incrementalKeyList(checkpointId, distanceFromCheckpoint);
    }
  }

  static class L1Producer extends BaseObjProducer<L1> implements L1 {

    private Id commitMetadataId;
    private List<Id> ancestors;
    private List<Id> children;
    private List<Mutation> keyMutations;
    private int distanceFromCheckpoint;
    private Id checkpointId;
    private List<Id> completeKeyList;

    @Override
    public L1 commitMetadataId(Id id) {
      this.commitMetadataId = id;
      return this;
    }

    @Override
    public L1 ancestors(Stream<Id> ids) {
      this.ancestors = ids.collect(Collectors.toList());
      return this;
    }

    @Override
    public L1 children(Stream<Id> ids) {
      this.children = ids.collect(Collectors.toList());
      return this;
    }

    @Override
    public L1 keyMutations(Stream<Mutation> keyMutations) {
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
      this.completeKeyList = fragmentIds.collect(Collectors.toList());
      return this;
    }

    @Override
    BaseObj<L1> build() {
      return new L1Obj(getId(), getDt(),
          commitMetadataId, ancestors, children, keyMutations, distanceFromCheckpoint, checkpointId, completeKeyList);
    }
  }

  @Override
  BaseObj<L1> copy() {
    return new L1Obj(getId(), getDt(), commitMetadataId,
        ancestors != null ? new ArrayList<>(ancestors) : null,
        children != null ? new ArrayList<>(children) : null,
        keyMutations != null ? new ArrayList<>(keyMutations) : null,
        distanceFromCheckpoint, checkpointId,
        completeKeyList != null ? new ArrayList<>(completeKeyList) : null);
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
            || !commitMetadataId.toEntity().equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case ANCESTORS:
        evaluate(function, ancestors);
        break;
      case CHILDREN:
        evaluate(function, children);
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
        evaluate(function, completeKeyList);
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    L1Obj l1Obj = (L1Obj) o;

    return (distanceFromCheckpoint == l1Obj.distanceFromCheckpoint)
        && Objects.equals(commitMetadataId, l1Obj.commitMetadataId)
        && Objects.equals(ancestors, l1Obj.ancestors)
        && Objects.equals(children, l1Obj.children)
        && Objects.equals(keyMutations, l1Obj.keyMutations)
        && Objects.equals(checkpointId, l1Obj.checkpointId)
        && Objects.equals(completeKeyList, l1Obj.completeKeyList);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (commitMetadataId != null ? commitMetadataId.hashCode() : 0);
    result = 31 * result + (ancestors != null ? ancestors.hashCode() : 0);
    result = 31 * result + (children != null ? children.hashCode() : 0);
    result = 31 * result + (keyMutations != null ? keyMutations.hashCode() : 0);
    result = 31 * result + distanceFromCheckpoint;
    result = 31 * result + (checkpointId != null ? checkpointId.hashCode() : 0);
    result = 31 * result + (completeKeyList != null ? completeKeyList.hashCode() : 0);
    return result;
  }
}
