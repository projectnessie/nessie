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
package com.dremio.nessie.versioned.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.InternalBranch.Commit;
import com.dremio.nessie.versioned.impl.InternalBranch.UnsavedDelta;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;

/**
 * Generic class for reading a reference.
 */
abstract class InternalRef extends PersistentBase<Ref> {

  static final String TYPE = "type";

  InternalRef(Id id, Long dt) {
    super(id, dt);
  }

  public enum Type {
    BRANCH("b"),
    TAG("t"),
    HASH(null),
    UNKNOWN(null);

    private final Entity value;

    Type(String identifier) {
      this.value = Entity.ofString(identifier);
    }

    public ExpressionFunction typeVerification() {
      return ExpressionFunction.equals(ExpressionPath.builder(TYPE).build(), toEntity());
    }

    /**
     * Convert the type to it's entity type tag.
     * @return A Entity holding the type tag.
     */
    public Entity toEntity() {
      if (this == HASH) {
        throw new IllegalStateException("You should not try to retrieve the identifier for a hash "
            + "type since they are not saveable as searchable refs.");
      }
      return value;
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
        throw new IllegalArgumentException(String.format("Unknown identifier name [%s].", identifier));
      }
    }
  }

  abstract Type getType();

  InternalBranch getBranch() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a branch.", this.getClass().getName()));
  }

  InternalTag getTag() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a tag.", this.getClass().getName()));
  }

  /**
   * Implement {@link Ref} to build an {@link InternalRef} object.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static final class Builder extends EntityBuilder<InternalRef, Ref> implements Ref {

    private RefType refType;
    private String name;

    // tag only
    private Id commit;

    // branch only
    private Id metadata;
    private Stream<Id> children;
    private List<Commit> commits;

    @Override
    public Builder type(RefType refType) {
      checkCalled(this.refType, "refType");
      this.refType = refType;
      return this;
    }

    @Override
    public Builder name(String name) {
      checkCalled(this.name, "name");
      this.name = name;
      return this;
    }

    @Override
    public Builder commit(Id commit) {
      checkCalled(this.commit, "commit");
      this.commit = commit;
      return this;
    }

    @Override
    public Builder metadata(Id metadata) {
      checkCalled(this.metadata, "metadata");
      this.metadata = metadata;
      return this;
    }

    @Override
    public Builder children(Stream<Id> children) {
      checkCalled(this.children, "children");
      this.children = children;
      return this;
    }

    @Override
    public Ref commits(Consumer<BranchCommitConsumer> commits) {
      checkCalled(this.commits, "commits");
      this.commits = new ArrayList<>();
      commits.accept(new BranchCommitConsumer() {
        private Id id;
        private Id commit;
        private Id parent;
        private List<UnsavedDelta> unsavedDeltas = new ArrayList<>();
        private List<KeyMutation> keyMutations = new ArrayList<>();

        @Override
        public BranchCommitConsumer done() {
          if (parent != null) {
            Builder.this.commits.add(new Commit(id, commit, parent));
          } else {
            Builder.this.commits.add(new Commit(id, commit, unsavedDeltas, KeyMutationList.of(keyMutations)));
          }

          id = null;
          commit = null;
          parent = null;
          unsavedDeltas = new ArrayList<>();
          keyMutations = new ArrayList<>();

          return this;
        }

        @Override
        public BranchCommitConsumer id(Id id) {
          this.id = id;
          return this;
        }

        @Override
        public BranchCommitConsumer commit(Id commit) {
          this.commit = commit;
          return this;
        }

        @Override
        public BranchCommitConsumer parent(Id parent) {
          this.parent = parent;
          return this;
        }

        @Override
        public BranchCommitConsumer delta(int position, Id oldId, Id newId) {
          unsavedDeltas.add(new UnsavedDelta(position, oldId, newId));
          return this;
        }

        @Override
        public BranchCommitConsumer keyMutation(Key.Mutation keyMutation) {
          keyMutations.add(KeyMutation.fromMutation(keyMutation));
          return this;
        }
      });
      return this;
    }

    @Override
    InternalRef build() {
      // null-id is allowed (will be generated)
      checkSet(refType, "refType");
      checkSet(name, "name");

      switch (refType) {
        case TAG:
          checkSet(commit, "commit");
          return new InternalTag(id, name, commit, dt);
        case BRANCH:
          checkSet(metadata, "metadata");
          checkSet(children, "children");
          checkSet(commits, "commits");
          return new InternalBranch(
              id,
              name,
              IdMap.of(children, InternalL1.SIZE),
              metadata,
              commits,
              dt);
        default:
          throw new UnsupportedOperationException("Unknown ref-type " + refType);
      }
    }
  }

}
