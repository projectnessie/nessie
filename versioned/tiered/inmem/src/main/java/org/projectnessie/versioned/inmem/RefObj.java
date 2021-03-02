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
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath.NameSegment;
import org.projectnessie.versioned.impl.condition.ExpressionPath.PathSegment;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.Mutation;
import org.projectnessie.versioned.tiered.Ref;
import org.projectnessie.versioned.tiered.Ref.BranchCommit;
import org.projectnessie.versioned.tiered.Ref.UnsavedCommitDelta;
import org.projectnessie.versioned.tiered.Ref.UnsavedCommitMutations;

import com.google.common.primitives.Ints;

public class RefObj extends BaseObj<Ref> {
  static final String TYPE = "type";
  static final String NAME = "name";
  static final String METADATA = "metadata";
  static final String COMMITS = "commits";
  static final String COMMIT = "commit";
  static final String CHILDREN = "children";
  static final String TREE = "tree";
  static final String PARENT = "parent";
  static final String DELTAS = "deltas";
  static final String KEYS = "keys";

  private String name;
  // tag
  private final Id commit;
  // branch
  private final Id metadata;
  private final List<Id> children;
  private final List<Commit> commits;

  RefObj(Id id, long dt, String name, Id commit, Id metadata, List<Id> children, List<Commit> commits) {
    super(id, dt);
    this.name = name;
    this.commit = commit;
    this.metadata = metadata;
    this.children = children;
    this.commits = commits;
  }

  @Override
  Ref consume(Ref consumer) {
    super.consume(consumer)
        .name(name);
    if (commit != null) {
      consumer.tag().commit(commit);
    } else {
      consumer.branch().metadata(metadata)
          .children(children.stream())
          .commits(bc -> commits.forEach(c -> c.consume(bc)));
    }
    return consumer;
  }

  @Override
  BaseObj<Ref> copy() {
    return new RefObj(getId(), getDt(), name, commit, metadata,
        children != null ? new ArrayList<>(children) : null,
        commits != null ? commits.stream().map(Commit::copy).collect(Collectors.toList()) : null);
  }

  @Override
  void applyRemove(RemoveClause removeClause, Consumer<DeferredRemove> deferredRemoveConsumer) {
    NameSegment root = removeClause.getPath().getRoot();
    switch (root.getName()) {
      case COMMITS:
        PathSegment position = root.getChild().orElseThrow(() -> new IllegalStateException("Position required"));
        int pos = position.asPosition().getPosition();
        if (position.getChild().isPresent()) {
          commits.get(pos).applyRemove(position.getChild().get().asName().getName());
        } else {
          deferredRemoveConsumer.accept(new DeferredRemove(pos, commits::remove));
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  void applySet(SetClause setClause) {
    NameSegment root = setClause.getPath().getRoot();
    switch (root.getName()) {
      case NAME:
        switch (setClause.getValue().getType()) {
          case VALUE:
            this.name = setClause.getValue().getValue().getString();
            break;
          case FUNCTION:
          case PATH:
          default:
            throw new UnsupportedOperationException();
        }
        break;
      case TREE:
        switch (setClause.getValue().getType()) {
          case VALUE:
            int position = root.getChild().orElseThrow(() -> new IllegalStateException("Position required")).asPosition().getPosition();
            children.set(position, Id.fromEntity(setClause.getValue().getValue()));
            break;
          case FUNCTION:
          case PATH:
          default:
            throw new UnsupportedOperationException();
        }
        break;
      case COMMITS:
        switch (setClause.getValue().getType()) {
          case FUNCTION:
            ExpressionFunction expressionFunction = (ExpressionFunction) setClause.getValue();
            switch (expressionFunction.getName()) {
              case LIST_APPEND:
                List<Entity> valueList = expressionFunction.getArguments().get(1).getValue().getList();
                valueList.stream().map(Commit::fromEntity).forEach(commits::add);
                break;
              case EQUALS:
              case SIZE:
              case ATTRIBUTE_NOT_EXISTS:
              default:
                throw new UnsupportedOperationException();
            }
            break;
          case VALUE:
            PathSegment position = root.getChild().orElseThrow(() -> new IllegalStateException("Position required"));
            String valueName = position.getChild().orElseThrow(() -> new IllegalStateException("Name required")).asName().getName();
            commits.get(position.asPosition().getPosition()).setValue(valueName, setClause.getValue().getValue());
            break;
          case PATH:
          default:
            throw new UnsupportedOperationException();
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();

    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case TYPE:
        String typeFromFunction;
        if (function.getValue().getString().equals("b")) {
          typeFromFunction = "b";
        } else if (function.getValue().getString().equals("t")) {
          typeFromFunction = "t";
        } else {
          throw new IllegalArgumentException(String.format("Unknown type name [%s].", function.getValue().getString()));
        }

        String type = commit != null ? "t" : "b";

        if (!function.isRootNameSegmentChildlessAndEquals() || !type.equals(typeFromFunction)) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case NAME:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !name.equals(function.getValue().getString())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case TREE:
      case CHILDREN:
        if (commit != null) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        if (!evaluate(function, children)) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case METADATA:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || commit != null
            || !metadata.toEntity().equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case COMMIT:
        evaluateTagCommit(function);
        break;
      case COMMITS:
        evaluateBranchCommits(function);
        break;
      default:
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  /**
   * Evaluates that this branch meets the condition.
   *
   * @param function the function that is tested against the nameSegment
   * @throws ConditionFailedException thrown if the condition expression is invalid or the condition is not met.
   */
  private void evaluateBranchCommits(Function function) {
    switch (function.getOperator()) {
      case EQUALS:
        if (function.getRootPathAsNameSegment().getChild().isPresent()
            || commit != null) {
          PathSegment position = function.getRootPathAsNameSegment().getChild().get();
          Commit c = commits.get(position.asPosition().getPosition());
          PathSegment name = position.getChild().orElseThrow(() -> new IllegalStateException("Name required"));
          if (!c.evaluate(name.asName().getName(), function.getValue())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case SIZE:
        if (function.getRootPathAsNameSegment().getChild().isPresent()
            || commit != null
            || commits.size() != function.getValue().getNumber()) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      default:
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  /**
   * Evaluates that this tag meets the condition.
   *
   * @param function the function that is tested against the nameSegment
   * @throws ConditionFailedException thrown if the condition expression is invalid or the condition is not met.
   */
  private void evaluateTagCommit(Function function) {
    if (!function.getOperator().equals(Function.Operator.EQUALS)
        || commit == null
        || !commit.toEntity().equals(function.getValue())) {
      throw new ConditionFailedException(conditionNotMatchedMessage(function));
    }
  }

  static class RefProducer extends BaseObjProducer<Ref> implements Ref {

    private String name;
    private Id commit;
    private Id metadata;
    private List<Id> children;
    private final List<Commit> commits = new ArrayList<>();

    @Override
    public Ref name(String name) {
      this.name = name;
      return this;
    }

    @Override
    public Tag tag() {
      return new Tag() {
        @Override
        public Tag commit(Id commit) {
          RefProducer.this.commit = commit;
          return this;
        }

        @Override
        public Ref backToRef() {
          return RefProducer.this;
        }
      };
    }

    @Override
    public Branch branch() {
      return new Branch() {
        @Override
        public Branch metadata(Id metadata) {
          RefProducer.this.metadata = metadata;
          return this;
        }

        @Override
        public Branch children(Stream<Id> children) {
          RefProducer.this.children = children.collect(Collectors.toList());
          return this;
        }

        @Override
        public Branch commits(Consumer<BranchCommit> commits) {
          commits.accept(new BranchCommitObj());
          return this;
        }

        @Override
        public Ref backToRef() {
          return RefProducer.this;
        }
      };
    }

    private final class BranchCommitObj implements BranchCommit {
      private Id id;
      private Id commit;

      @Override
      public BranchCommit id(Id id) {
        this.id = id;
        return this;
      }

      @Override
      public BranchCommit commit(Id commit) {
        this.commit = commit;
        return this;
      }

      @Override
      public SavedCommit saved() {
        return new SavedCommit() {
          private Id parent;

          @Override
          public SavedCommit parent(Id parent) {
            this.parent = parent;
            return this;
          }

          @Override
          public BranchCommit done() {
            commits.add(new Commit(id, commit, parent));
            return BranchCommitObj.this;
          }
        };
      }

      @Override
      public UnsavedCommitDelta unsaved() {
        return new UnsavedCommitDelta() {
          private final List<UnsavedDelta> deltas = new ArrayList<>();

          @Override
          public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
            this.deltas.add(new UnsavedDelta(position, oldId, newId));
            return this;
          }

          @Override
          public UnsavedCommitMutations mutations() {
            return new UnsavedCommitMutations() {
              private final List<Mutation> keyMutations = new ArrayList<>();

              @Override
              public UnsavedCommitMutations keyMutation(Mutation keyMutation) {
                this.keyMutations.add(keyMutation);
                return this;
              }

              @Override
              public BranchCommit done() {
                commits.add(new Commit(id, commit, deltas, keyMutations));
                return BranchCommitObj.this;
              }
            };
          }
        };
      }
    }

    @Override
    BaseObj<Ref> build() {
      return new RefObj(getId(), getDt(), name, commit, metadata, children, commits);
    }
  }

  private static final class Commit {
    private Id id;
    private Id commit;

    private Id parent;

    private final List<UnsavedDelta> deltas;
    private final List<Mutation> keyMutations;

    private Commit(Id id, Id commit, Id parent, List<UnsavedDelta> deltas, List<Mutation> keyMutations) {
      this.id = id;
      this.commit = commit;

      this.parent = parent;

      this.deltas = deltas;
      this.keyMutations = keyMutations;
    }

    Commit(Id id, Id commit, Id parent) {
      this(id, commit, parent, new ArrayList<>(), new ArrayList<>());
    }

    Commit(Id id, Id commit, List<UnsavedDelta> deltas, List<Mutation> keyMutations) {
      this(id, commit, null, deltas, keyMutations);
    }

    Id getId() {
      return id;
    }

    Id getCommit() {
      return commit;
    }

    void consume(BranchCommit bc) {
      bc.id(id).commit(commit);

      if (parent != null) {
        // saved
        bc.saved().parent(parent).done();
      } else {
        // unsaved
        UnsavedCommitDelta unsaved = bc.unsaved();
        deltas.forEach(d -> unsaved.delta(d.getPosition(), d.getOldId(), d.getNewId()));
        UnsavedCommitMutations mutations = unsaved.mutations();
        keyMutations.forEach(mutations::keyMutation);
        mutations.done();
      }
    }

    Commit copy() {
      return new Commit(getId(), getCommit(), parent, new ArrayList<>(deltas), new ArrayList<>(keyMutations));
    }

    static Commit fromEntity(Entity entity) {
      Map<String, Entity> map = entity.getMap();
      if (map.containsKey(PARENT)) {
        return new Commit(
            Id.fromEntity(map.get(ID)),
            Id.fromEntity(map.get(COMMIT)),
            Id.fromEntity(map.get(PARENT)));
      } else {
        return new Commit(
            Id.fromEntity(map.get(ID)),
            Id.fromEntity(map.get(COMMIT)),
            map.get(DELTAS).getList().stream().map(UnsavedDelta::fromEntity).collect(Collectors.toList()),
            map.get(KEYS).getList().stream().map(Commit::keyMutationFromEntity).collect(Collectors.toList())
        );
      }
    }

    private static Mutation keyMutationFromEntity(Entity entity) {
      Map<String, Entity> map = entity.getMap();
      if (map.containsKey("a")) {
        List<Entity> list = map.get("a").getList();
        String payloadString = list.get(0).getString();
        Byte payload = payloadString != null && payloadString.length() > 0 && payloadString.charAt(0) != 0
            ? Byte.parseByte(payloadString) : null;
        return Mutation.Addition.of(keyFromList(list), payload);
      }
      if (map.containsKey("d")) {
        List<Entity> list = map.get("d").getList();
        return Mutation.Removal.of(keyFromList(list));
      }
      throw new UnsupportedOperationException();
    }

    private static Key keyFromList(List<Entity> list) {
      return Key.of(list.stream().skip(1).map(Entity::getString).toArray(String[]::new));
    }

    boolean evaluate(String name, Entity value) {
      switch (name) {
        case ID:
          return Id.fromEntity(value).equals(id);
        case COMMIT:
          return Id.fromEntity(value).equals(commit);
        case PARENT:
          return Id.fromEntity(value).equals(parent);
        default:
          return false;
      }
    }

    void setValue(String name, Entity value) {
      switch (name) {
        case ID:
          this.id = Id.fromEntity(value);
          break;
        case COMMIT:
          this.commit = Id.fromEntity(value);
          break;
        case PARENT:
          this.parent = Id.fromEntity(value);
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    public void applyRemove(String name) {
      switch (name) {
        case DELTAS:
          deltas.clear();
          break;
        case KEYS:
          keyMutations.clear();
          break;
        case PARENT:
          parent = null;
          break;
        default:
          throw new UnsupportedOperationException();
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

      Commit commit1 = (Commit) o;

      return Objects.equals(id, commit1.id)
          && Objects.equals(commit, commit1.commit)
          && Objects.equals(parent, commit1.parent)
          && Objects.equals(deltas, commit1.deltas)
          && Objects.equals(keyMutations, commit1.keyMutations);
    }

    @Override
    public int hashCode() {
      int result = id != null ? id.hashCode() : 0;
      result = 31 * result + (commit != null ? commit.hashCode() : 0);
      result = 31 * result + (parent != null ? parent.hashCode() : 0);
      result = 31 * result + (deltas != null ? deltas.hashCode() : 0);
      result = 31 * result + (keyMutations != null ? keyMutations.hashCode() : 0);
      return result;
    }
  }

  private static final class UnsavedDelta {
    private static final String POSITION = "position";
    private static final String OLD = "old";
    private static final String NEW = "new";

    private final int position;
    private final Id oldId;
    private final Id newId;

    UnsavedDelta(int position, Id oldId, Id newId) {
      this.position = position;
      this.oldId = oldId;
      this.newId = newId;
    }

    int getPosition() {
      return position;
    }

    Id getOldId() {
      return oldId;
    }

    Id getNewId() {
      return newId;
    }

    static UnsavedDelta fromEntity(Entity entity) {
      Map<String, Entity> map = entity.getMap();
      return new UnsavedDelta(Ints.saturatedCast(map.get(POSITION).getNumber()),
        Id.fromEntity(map.get(OLD)),
        Id.fromEntity(map.get(NEW)));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UnsavedDelta that = (UnsavedDelta) o;
      return position == that.position && Objects.equals(oldId, that.oldId)
          && Objects.equals(newId, that.newId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(position, oldId, newId);
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

    RefObj refObj = (RefObj) o;

    return Objects.equals(name, refObj.name)
        && Objects.equals(commit, refObj.commit)
        && Objects.equals(metadata, refObj.metadata)
        && Objects.equals(children, refObj.children)
        && Objects.equals(commits, refObj.commits);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (commit != null ? commit.hashCode() : 0);
    result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
    result = 31 * result + (children != null ? children.hashCode() : 0);
    result = 31 * result + (commits != null ? commits.hashCode() : 0);
    return result;
  }
}
