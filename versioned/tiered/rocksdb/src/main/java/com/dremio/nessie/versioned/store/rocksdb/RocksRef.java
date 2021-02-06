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

import static com.dremio.nessie.versioned.store.rocksdb.Function.EQUALS;
import static com.dremio.nessie.versioned.store.rocksdb.Function.SIZE;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link com.dremio.nessie.tiered.builder.Ref} providing
 * SerDe and Condition evaluation.
 */
class RocksRef extends RocksBaseValue<Ref> implements Ref {

  static final String TYPE = "type";
  static final String NAME = "name";
  static final String METADATA = "metadata";
  static final String COMMITS = "commits";
  static final String COMMIT = "commit";
  static final String CHILDREN = "children";

  private RefType type;
  private String name;
  private Id commit;
  private Id metadata;
  private Stream<Id> children;
  private List<ValueProtos.Commit> commits;

  RocksRef() {
    super();
  }

  @Override
  public boolean evaluateSegment(ExpressionPath.NameSegment nameSegment, Function function) {
    if (type == RefType.BRANCH) {
      return evaluateBranch(nameSegment, function);
    } else if (type == RefType.TAG) {
      return evaluateTag(nameSegment, function);
    }
    return true;
  }

  /**
   * Evaluates that this branch meets the condition.
   * @param nameSegment the segment on which the function is evaluated
   * @param function the function that is tested against the nameSegment
   * @return true if this branch meets the condition
   */
  private boolean evaluateBranch(ExpressionPath.NameSegment nameSegment, Function function) {

    final String segment = nameSegment.getName();
    switch (segment) {
      case ID:
        if (!idEvaluates(nameSegment, function)) {
          return false;
        }
        break;
      case TYPE:
        if (!nameSegmentChildlessAndEquals(nameSegment, function)
            || !type.toString().equals(function.getValue().getString())) {
          return false;
        }
        break;
      case NAME:
        if (!nameSegmentChildlessAndEquals(nameSegment, function)
            || !name.equals(function.getValue().getString())) {
          return false;
        }
        break;
      case CHILDREN:
        if (!evaluateStream(function, children)) {
          return false;
        }
        break;
      case METADATA:
        if (!nameSegmentChildlessAndEquals(nameSegment, function)
            || !metadata.toEntity().equals(function.getValue())) {
          return false;
        }
        break;
      case COMMITS:
        // TODO: refactor once jdbc-store Store changes are available.
        switch (function.getOperator()) {
          case SIZE:
            if (nameSegment.getChild().isPresent()
                || commits.size() != (int) function.getValue().getNumber()) {
              return false;
            }
            break;
          case EQUALS:
            return false;
          default:
            return false;
        }
        break;
      default:
        return false;
    }
    // The object has not failed any condition.
    return true;
  }

  /**
   * Evaluates that this tag meets the condition.
   * @param nameSegment the segment on which the function is evaluated
   * @param function the function that is tested against the nameSegment
   * @return true if this tag meets the condition
   */
  private boolean evaluateTag(ExpressionPath.NameSegment nameSegment, Function function) {
    final String segment = nameSegment.getName();
    switch (segment) {
      case ID:
        if (!idEvaluates(nameSegment, function)) {
          return false;
        }
        break;
      case TYPE:
        if (!nameSegmentChildlessAndEquals(nameSegment, function)
            || !type.toString().equals(function.getValue().getString())) {
          return false;
        }
        break;
      case NAME:
        if (!function.isEquals()
            || !name.equals(function.getValue().getString())) {
          return false;
        }
        break;
      case COMMIT:
        if (!function.isEquals()
            || !commit.toEntity().equals(function.getValue())) {
          return false;
        }
        break;
      default:
        return false;
    }
    // The object has not failed any condition.
    return true;
  }

  @Override
  public Ref type(RefType refType) {
    this.type = refType;
    return this;
  }

  @Override
  public Ref name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public Ref commit(Id commit) {
    this.commit = commit;
    return this;
  }

  @Override
  public Ref metadata(Id metadata) {
    this.metadata = metadata;
    return this;
  }

  @Override
  public Ref children(Stream<Id> children) {
    this.children = children;
    return this;
  }

  @Override
  byte[] build() {
    checkPresent(name, NAME);
    checkPresent(type, TYPE);

    final ValueProtos.Ref.Builder builder = ValueProtos.Ref.newBuilder().setBase(buildBase()).setName(name);

    if (type == RefType.TAG) {
      checkPresent(commit, COMMIT);
      checkNotPresent(commits, COMMITS);
      checkNotPresent(children, CHILDREN);
      checkNotPresent(metadata, METADATA);

      builder.setTag(ValueProtos.Tag.newBuilder().setId(commit.getValue()).build());
    } else {
      // Branch
      checkNotPresent(commit, COMMIT);
      checkPresent(commits, COMMITS);
      checkPresent(children, CHILDREN);
      checkPresent(metadata, METADATA);

      builder.setBranch(ValueProtos.Branch.newBuilder()
          .addAllCommits(commits)
          .addAllChildren(buildIds(children))
          .setMetadataId(metadata.getValue()).build());
    }

    return builder.build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, Ref consumer) {
    try {
      final ValueProtos.Ref ref = ValueProtos.Ref.parseFrom(value);
      setBase(consumer, ref.getBase());
      consumer.name(ref.getName());
      if (ref.hasTag()) {
        consumer.type(RefType.TAG).commit(Id.of(ref.getTag().getId()));
      } else {
        // Branch
        consumer
            .type(RefType.BRANCH)
            .commits(bc -> deserializeCommits(bc, ref.getBranch().getCommitsList()))
            .children(ref.getBranch().getChildrenList().stream().map(Id::of))
            .metadata(Id.of(ref.getBranch().getMetadataId()));

      }
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt Ref value encountered when deserializing.", e);
    }
  }

  private static void deserializeCommits(BranchCommitConsumer consumer, List<ValueProtos.Commit> commitsList) {
    for (ValueProtos.Commit commit : commitsList) {
      consumer
          .id(Id.of(commit.getId()))
          .commit(Id.of(commit.getCommit()));

      if (commit.getParent().isEmpty()) {
        commit.getDeltaList().forEach(d -> consumer.delta(d.getPosition(), Id.of(d.getOldId()), Id.of(d.getNewId())));
        commit.getKeyMutationList().forEach(km -> consumer.keyMutation(createKeyMutation(km)));
      } else {
        consumer.parent(Id.of(commit.getParent()));
      }
      consumer.done();
    }
  }

  @Override
  public Ref commits(Consumer<BranchCommitConsumer> commitsConsumer) {
    if (null == this.commits) {
      this.commits = new ArrayList<>();
    }

    commitsConsumer.accept(new BranchCommitConsumer() {
      final ValueProtos.Commit.Builder builder = ValueProtos.Commit.newBuilder();

      @Override
      public BranchCommitConsumer id(Id id) {
        builder.setId(id.getValue());
        return this;
      }

      @Override
      public BranchCommitConsumer commit(Id commit) {
        builder.setCommit(commit.getValue());
        return this;
      }

      @Override
      public BranchCommitConsumer parent(Id parent) {
        builder.setParent(parent.getValue());
        return this;
      }

      @Override
      public BranchCommitConsumer delta(int position, Id oldId, Id newId) {
        builder.addDelta(ValueProtos.Delta.newBuilder()
            .setPosition(position)
            .setOldId(oldId.getValue())
            .setNewId(newId.getValue())
            .build());
        return this;
      }

      @Override
      public BranchCommitConsumer keyMutation(Key.Mutation keyMutation) {
        builder.addKeyMutation(buildKeyMutation(keyMutation));
        return this;
      }

      @Override
      public BranchCommitConsumer done() {
        commits.add(builder.build());
        builder.clear();
        return this;
      }
    });
    return this;
  }
}
