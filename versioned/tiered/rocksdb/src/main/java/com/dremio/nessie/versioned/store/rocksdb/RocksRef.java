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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.InvalidProtocolBufferException;

class RocksRef extends RocksBaseValue<Ref> implements Ref, Evaluator {

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
  public boolean evaluate(Condition condition) {
    boolean result = true;

    // Retrieve entity at function.path
    if (type == RefType.BRANCH) {
      // TODO: do we need to subtype?
      for (Function function: condition.functionList) {
        // Branch evaluation
        final List<String> path = Evaluator.splitPath(function.getPath());
        final String segment = path.get(0);
        if (segment.equals(ID)) {
          result &= ((path.size() == 1)
            && (function.getOperator().equals(Function.EQUALS))
            && (getId().toEntity().equals(function.getValue())));
        } else if (segment.equals(TYPE)) {
          result &= ((path.size() == 1)
            && (function.getOperator().equals(Function.EQUALS))
            && (type.toString().equals(function.getValue().getString())));
        } else if (segment.equals(NAME)) {
          result &= ((path.size() == 1)
            && (function.getOperator().equals(Function.EQUALS)
            && (name.equals(function.getValue().getString()))));
        } else if (segment.startsWith(CHILDREN)) {
          evaluateStream(function, children);
        } else if (segment.equals(METADATA)) {
          result &= ((path.size() == 1)
                && (function.getOperator().equals(Function.EQUALS)))
                && (metadata.toEntity().equals(function.getValue()));
        } else if (segment.equals(COMMITS)) {
          // TODO: refactor once jdbc-store Store changes are available.
          if (function.getOperator().equals(Function.SIZE)) {
            result &= ((path.size() == 1)
                && (function.getOperator().equals(Function.SIZE)
                && (commits.size() == (int)function.getValue().getNumber())));
          } else if (function.getOperator().equals(Function.EQUALS)) {
            return false;
          } else {
            return false;
          }
        } else {
          return false;
        }
      }
    } else if (this.type == RefType.TAG) {
      for (Function function: condition.functionList) {
        // Tag evaluation
        final List<String> path = Evaluator.splitPath(function.getPath());
        final String segment = path.get(0);
        switch (segment) {
          case ID:
            result &= ((path.size() == 1)
              && (function.getOperator().equals(Function.EQUALS))
              && (getId().toEntity().equals(function.getValue())));
            break;
          case TYPE:
            result &= ((path.size() == 1)
              && (function.getOperator().equals(Function.EQUALS))
              && (type.toString().equals(function.getValue().getString())));
            break;
          case NAME:
            result &= (function.getOperator().equals(Function.EQUALS)
              && (name.equals(function.getValue().getString())));
            break;
          case COMMIT:
            result &= (function.getOperator().equals(Function.EQUALS)
              && (commit.toEntity().equals(function.getValue())));
            break;
          default:
            return false;
        }
      }
    }
    return result;
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
