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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.IdMap;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RocksRef extends RocksBaseValue<Ref> implements Ref, Evaluator {

  public static final String TYPE = "type";
  public static final String NAME = "name";
  public static final String METADATA = "metadata";
  public static final String COMMITS = "commits";
  public static final String COMMIT = "commit";
  public static final String CHILDREN = "children";

  // TODO: Create correct constuctor with all attributes.
  static RocksRef EMPTY =
      new RocksRef();

  static Id EMPTY_ID = EMPTY.getId();

  RefType type;
  String name;
  Id commit;
  Id metadata;
  Stream<Id> children;
  private List<Commit> commits;

  public RocksRef() {
    super(EMPTY_ID, 0);
  }

  @Override
  public boolean evaluate(Condition condition) {
    boolean result = true;

    // Retrieve entity at function.path
    if (type == RefType.BRANCH) {
      // TODO: do we need to subtype?
      for (Function function: condition.functionList) {
        // Branch evaluation
        List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));
        String segment = path.get(0);
        if (segment.equals(TYPE)) {
          result &= ((path.size() == 1)
                && (function.getOperator().equals(Function.EQUALS))
                && (!type.toString().equals(function.getValue().getString())));
        } else if (segment.equals(CHILDREN)) {
          evaluateStream(function, children);
        } else if (segment.equals(METADATA)) {
          result &= ((path.size() == 1)
                && (function.getOperator().equals(Function.EQUALS)))
                && (metadata.toEntity().equals(function.getValue()));
        } else if (segment.equals(COMMITS)) {
          if (function.getOperator().equals(Function.SIZE)) {
            // Is a List
            // TODO: We require a getCommits() accessor in InternalBranch
            result &= false;
          } else if (function.getOperator().equals(Function.EQUALS)) {
            // TODO: We require a getCommits() accessor in InternalBranch
          } else {
            return false;
          }
        }
      }
    } else if (this.type == RefType.TAG) {
      for (Function function: condition.functionList) {
        // Tag evaluation
        List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));
        String segment = path.get(0);
        if (segment.equals(TYPE)) {
          result &= ((path.size() == 1)
              && (function.getOperator().equals(Function.EQUALS))
              && (type.toString().equals(function.getValue().getString())));
        } else if (segment.equals(NAME)) {
          result &= (function.getOperator().equals(Function.EQUALS)
              && (name.equals(function.getValue().getString())));
        } else if (segment.equals(COMMIT)) {
          result &= (function.getOperator().equals(Function.EQUALS)
            && (commit.toEntity().equals(function.getValue())));
        } else {
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
  public Ref commits(Consumer<BranchCommitConsumer> commits) {
    return this;
  }

  // Adapted from InternalBranch.Commit
  public static final class Commit {

    static final String ID = "id";
    static final String COMMIT = "commit";
    static final String DELTAS = "deltas";
    static final String PARENT = "parent";
    static final String KEY_MUTATIONS = "keys";

    private final Boolean saved;
    private final Id id;
    private final Id commit;
    private final Id parent;
    private final List<UnsavedDelta> deltas;
    private final List<Key.Mutation> keyMutationList;

    /**
     * Construct a commit without mutations.
     * @param id the commit id
     * @param commit the commit
     * @param parent the parent of this commit
     */
    public Commit(Id id, Id commit, Id parent) {
      this.id = id;
      this.parent = parent;
      this.commit = commit;
      this.saved = true;
      this.deltas = Collections.emptyList();
      this.keyMutationList = null;
    }

    // TODO: Look at use of Key.Mutation instead of KeyMutationList. Taken from InternalRef

    /**
     * Construct a commit before it has been saved to the backing store. At this stage it is not know if
     * it will succeed if another commit saves first.
     * @param unsavedId id of the commit
     * @param commit the commit
     * @param deltas changes this commit will cause
     * @param keyMutationList changes to the list of keys
     */
    public Commit(Id unsavedId, Id commit, List<UnsavedDelta> deltas, List<Key.Mutation> keyMutationList) {
      super();
      this.saved = false;
      this.deltas = ImmutableList.copyOf(Preconditions.checkNotNull(deltas));
      this.commit = Preconditions.checkNotNull(commit);
      this.parent = null;
      this.keyMutationList = Preconditions.checkNotNull(keyMutationList);
      this.id = Preconditions.checkNotNull(unsavedId);
    }

    Id getId() {
      return id;
    }

    public boolean isSaved() {
      return saved;
    }

    public Entity toEntity() {
      return Entity.ofMap(itemToMap(this));
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
      return Objects.equal(saved, commit1.saved)
        && Objects.equal(id, commit1.id)
        && Objects.equal(commit, commit1.commit)
        && Objects.equal(parent, commit1.parent)
        && Objects.equal(deltas, commit1.deltas);
      // TODO: need to create local implementation of KeyMutationList
      // && KeyMutationList.equalsIgnoreOrder(keyMutationList, commit1.keyMutationList);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(saved, id, commit, parent, deltas, keyMutationList);
    }

    Map<String, Entity> itemToMap(Commit item) {
      ImmutableMap.Builder<String, Entity> builder = ImmutableMap.builder();
      builder
          .put(ID, item.getId().toEntity())
          .put(COMMIT, item.commit.toEntity());
      if (item.saved) {
        builder.put(PARENT, item.parent.toEntity());
      } else {
        Entity deltas = Entity.ofList(
            item.deltas.stream()
            .map(UnsavedDelta::itemToMap)
            .map(Entity::ofMap)
            .collect(Collectors.toList()));
        builder.put(DELTAS, deltas);
        // TODO: need to create local implementation of KeyMutationList
        // builder.put(KEY_MUTATIONS, item.keyMutationList.toEntity());
      }
      return builder.build();
    }
  }

  // Adapted from InternalBranch.UnsavedDelta
  public static class UnsavedDelta {

    private static final String POSITION = "position";
    private static final String NEW_ID = "new";
    private static final String OLD_ID = "old";

    private final int position;
    private final Id oldId;
    private final Id newId;


    /**
     * Representation of a change.
     * @param position  the position where the change occurs
     * @param oldId last id
     * @param newId new id
     */
    public UnsavedDelta(int position, Id oldId, Id newId) {
      this.position = position;
      this.oldId = oldId;
      this.newId = newId;
    }

    public IdMap apply(IdMap tree) {
      return tree.withId(position, newId);
    }

    public IdMap reverse(IdMap tree) {
      return tree.withId(position,  oldId);
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
      return position == that.position && Objects.equal(oldId, that.oldId)
        && Objects.equal(newId, that.newId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(position, oldId, newId);
    }

    Map<String, Entity> itemToMap() {
      return ImmutableMap.<String, Entity>builder()
        .put(POSITION, Entity.ofNumber(position))
        .put(OLD_ID, oldId.toEntity())
        .put(NEW_ID, newId.toEntity())
        .build();
    }
  }

}
