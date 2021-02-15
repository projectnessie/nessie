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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.Ref;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.Ref} providing
 * SerDe and Condition evaluation.
 */
class RocksRef extends RocksBaseValue<Ref> implements Ref {

  static final String TYPE = "type";
  static final String NAME = "name";
  static final String METADATA = "metadata";
  static final String COMMITS = "commits";
  static final String COMMIT = "commit";
  static final String CHILDREN = "children";

  private Id commit;
  private String name;
  private Id metadata;
  private Stream<Id> children;
  private List<ValueProtos.Commit> commits;

  RocksRef() {
    super();
  }

  private enum Type {
    INIT(null),
    TAG("t"),
    BRANCH("b");

    private String identifier;
    Type(String identifier) {
      this.identifier = identifier;
    }

    /**
     * Get the type associated with this type tag.
     * @param identifier The type tag to classify.
     * @return The type classified.
     */
    public static Type getType(String identifier) {
      if (identifier.equals("b")) {
        return BRANCH;
      } else if (identifier.equals("t")) {
        return TAG;
      } else {
        throw new IllegalArgumentException(String.format("Unknown type name [%s].", identifier));
      }
    }
  }

  private Type type = Type.INIT;

  @Override
  public boolean evaluate(Function function) {
    if (type == Type.BRANCH) {
      return evaluateBranch(function);
    } else if (type == Type.TAG) {
      return evaluateTag(function);
    }
    return false;
  }

  /**
   * Evaluates that this branch meets the condition.
   *
   * @param function the function that is tested against the nameSegment
   * @return true if this branch meets the condition
   */
  private boolean evaluateBranch(Function function) {
    final String segment = function.getRootPathAsNameSegment().getName();

    switch (segment) {
      case ID:
        return evaluatesId(function);
      case TYPE:
        return (function.isRootNameSegmentChildlessAndEquals()
          && type.equals(Type.getType(function.getValue().getString())));
      case NAME:
        return (function.isRootNameSegmentChildlessAndEquals()
          && name.equals(function.getValue().getString()));
      case CHILDREN:
        return evaluateStream(function, children);
      case METADATA:
        return (function.isRootNameSegmentChildlessAndEquals()
          && metadata.toEntity().equals(function.getValue()));
      case COMMITS:
        // TODO: refactor once jdbc-store Store changes are available.
        switch (function.getOperator()) {
          case SIZE:
            return (!function.getRootPathAsNameSegment().getChild().isPresent()
              && commits.size() == function.getValue().getNumber());
          default:
            return false;
        }
      default:
        return false;
    }
  }

  /**
   * Evaluates that this tag meets the condition.
   *
   * @param function the function that is tested against the nameSegment
   * @return true if this tag meets the condition
   */
  private boolean evaluateTag(Function function) {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        return evaluatesId(function);
      case TYPE:
        return (function.isRootNameSegmentChildlessAndEquals()
          && type.equals(Type.getType(function.getValue().getString())));
      case NAME:
        return (function.getOperator().equals(Function.Operator.EQUALS)
          && name.equals(function.getValue().getString()));
      case COMMIT:
        return (function.getOperator().equals(Function.Operator.EQUALS)
          && commit.toEntity().equals(function.getValue()));
      default:
        return false;
    }
  }

  @Override
  public Ref name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public Tag tag() {
    if (type != Type.INIT) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }
    type = Type.TAG;
    return new RocksTag();
  }

  @Override
  public Branch branch() {
    if (type != Type.INIT) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }
    type = Type.BRANCH;
    return new RocksBranch();
  }


  class RocksTag implements Tag {
    @Override
    public Tag commit(Id commit) {
      RocksRef.this.commit = commit;
      return this;
    }

    @Override
    public Ref backToRef() {
      return RocksRef.this;
    }
  }

  class RocksBranch implements Branch {
    @Override
    public Branch metadata(Id metadata) {
      RocksRef.this.metadata = metadata;
      return this;
    }

    @Override
    public Branch children(Stream<Id> children) {
      RocksRef.this.children = children;
      return this;
    }

    @Override
    public Branch commits(Consumer<BranchCommit> commitsConsumer) {
      if (null == RocksRef.this.commits) {
        RocksRef.this.commits = new ArrayList<>();
      }

      commitsConsumer.accept(new RocksBranchCommit());
      return this;
    }

    @Override
    public Ref backToRef() {
      return RocksRef.this;
    }
  }

  private class RocksBranchCommit implements BranchCommit, SavedCommit, UnsavedCommitDelta, UnsavedCommitMutations {
    final ValueProtos.Commit.Builder builder = ValueProtos.Commit.newBuilder();

    // BranchCommit implementations
    @Override
    public BranchCommit id(Id id) {
      builder.setId(id.getValue());
      return this;
    }

    @Override
    public BranchCommit commit(Id commit) {
      builder.setCommit(commit.getValue());
      return this;
    }

    @Override
    public SavedCommit saved() {
      return this;
    }

    @Override
    public UnsavedCommitDelta unsaved() {
      return this;
    }

    // SavedCommit implementations
    @Override
    public SavedCommit parent(Id parent) {
      builder.setParent(parent.getValue());
      return this;
    }

    @Override
    public BranchCommit done() {
      commits.add(builder.build());
      builder.clear();
      return this;
    }

    // UnsavedCommitDelta implementations
    @Override
    public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
      builder.addDelta(ValueProtos.Delta.newBuilder()
          .setPosition(position)
          .setOldId(oldId.getValue())
          .setNewId(newId.getValue())
          .build());
      return this;
    }

    @Override
    public UnsavedCommitMutations mutations() {
      return this;
    }

    // UnsavedCommitMutations implementations
    @Override
    public UnsavedCommitMutations keyMutation(Key.Mutation keyMutation) {
      builder.addKeyMutation(buildKeyMutation(keyMutation));
      return this;
    }
  }

  @Override
  byte[] build() {
    checkPresent(name, NAME);

    final ValueProtos.Ref.Builder builder = ValueProtos.Ref.newBuilder().setBase(buildBase()).setName(name);

    if (type == Type.TAG) {
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
        consumer.tag().commit(Id.of(ref.getTag().getId()));
      } else {
        // Branch
        consumer
            .branch()
            .commits(bc -> deserializeCommits(bc, ref.getBranch().getCommitsList()))
            .children(ref.getBranch().getChildrenList().stream().map(Id::of))
            .metadata(Id.of(ref.getBranch().getMetadataId()));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt Ref value encountered when deserializing.", e);
    }
  }

  private static void deserializeCommits(BranchCommit consumer, List<ValueProtos.Commit> commitsList) {
    for (ValueProtos.Commit commit : commitsList) {
      consumer
          .id(Id.of(commit.getId()))
          .commit(Id.of(commit.getCommit()));

      if (commit.getParent().isEmpty()) {
        final UnsavedCommitDelta unsaved = consumer.unsaved();
        commit.getDeltaList().forEach(d -> unsaved.delta(d.getPosition(), Id.of(d.getOldId()), Id.of(d.getNewId())));
        final UnsavedCommitMutations mutations = unsaved.mutations();
        commit.getKeyMutationList().forEach(km -> mutations.keyMutation(createKeyMutation(km)));
        mutations.done();
      } else {
        consumer.saved().parent(Id.of(commit.getParent())).done();
      }
    }
  }
}
