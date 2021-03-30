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
package org.projectnessie.versioned.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.InternalBranch.Commit;
import org.projectnessie.versioned.impl.InternalBranch.UnsavedDelta;
import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.Mutation;
import org.projectnessie.versioned.tiered.Ref;

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
  static class Builder<REF extends InternalRef> extends EntityBuilder<InternalRef, Ref> implements Ref {

    private Id id;
    private String name;

    private Builder<REF> typed;

    @Override
    public Builder<REF> id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tag tag() {
      if (typed != null) {
        throw new IllegalStateException("Must only call tag() or branch() once.");
      }
      TagBuilder b = new TagBuilder();
      typed = (Builder<REF>) b;
      return b;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Branch branch() {
      if (typed != null) {
        throw new IllegalStateException("Must only call tag() or branch() once.");
      }
      BranchBuilder b = new BranchBuilder();
      typed = (Builder<REF>) b;
      return b;
    }

    @Override
    public Builder<REF> name(String name) {
      checkCalled(this.name, "name");
      this.name = name;
      return this;
    }

    @Override

    REF build() {
      if (typed == null) {
        throw new IllegalStateException("Must call tag() or branch() before build().");
      }
      return typed.build();
    }

    private class TagBuilder extends Builder<InternalTag> implements Tag {
      private Id commit;

      @Override
      public Tag commit(Id commit) {
        checkCalled(this.commit, "commit");
        this.commit = commit;
        return this;
      }

      @Override
      InternalTag build() {
        // null-id is allowed (will be generated)
        checkSet(name, "name");
        checkSet(commit, "commit");
        return new InternalTag(id, name, commit, dt);
      }

      @Override
      public Ref backToRef() {
        return Builder.this;
      }
    }

    private class BranchBuilder extends Builder<InternalBranch> implements Branch {

      private Id metadata;
      private Stream<Id> children;
      private List<Commit> commits;

      @Override
      public Branch metadata(Id metadata) {
        checkCalled(this.metadata, "metadata");
        this.metadata = metadata;
        return this;
      }

      @Override
      public Branch children(Stream<Id> children) {
        checkCalled(this.children, "children");
        this.children = children;
        return this;
      }

      @Override
      public Branch commits(Consumer<BranchCommit> commits) {
        checkCalled(this.commits, "commits");
        this.commits = new ArrayList<>();
        commits.accept(new InternalBranchCommit());
        return this;
      }

      @Override
      public Ref backToRef() {
        return Builder.this;
      }

      @Override
      InternalBranch build() {
        // null-id is allowed (will be generated)
        checkSet(name, "name");
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
      }

      private class InternalBranchCommit implements BranchCommit, SavedCommit,
          UnsavedCommitDelta, UnsavedCommitMutations {

        private Id id;
        private Id commit;
        private Id parent;
        private List<UnsavedDelta> unsavedDeltas = new ArrayList<>();
        private List<InternalMutation> keyMutations = new ArrayList<>();

        @Override
        public BranchCommit done() {
          if (parent != null) {
            BranchBuilder.this.commits.add(new Commit(id, commit, parent));
          } else {
            BranchBuilder.this.commits
                .add(new Commit(id, commit, unsavedDeltas, KeyMutationList.of(keyMutations)));
          }

          id = null;
          commit = null;
          parent = null;
          unsavedDeltas = new ArrayList<>();
          keyMutations = new ArrayList<>();

          return this;
        }

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
          return this;
        }

        @Override
        public UnsavedCommitDelta unsaved() {
          return this;
        }

        @Override
        public SavedCommit parent(Id parent) {
          this.parent = parent;
          return this;
        }

        @Override
        public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
          unsavedDeltas.add(new UnsavedDelta(position, oldId, newId));
          return this;
        }

        @Override
        public UnsavedCommitMutations mutations() {
          return this;
        }

        @Override
        public UnsavedCommitMutations keyMutation(Mutation keyMutation) {
          keyMutations.add(InternalMutation.fromMutation(keyMutation));
          return this;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  <E extends PersistentBase<Ref>> EntityType<Ref, E, ?> getEntityType() {
    return (EntityType<Ref, E, ?>) EntityType.REF;
  }
}
