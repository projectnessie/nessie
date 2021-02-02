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

package com.dremio.nessie.tiered.builder.base;

import java.util.function.Consumer;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key.Mutation;
import com.dremio.nessie.versioned.store.Id;

/**
 * Abstract implementation of {@link Ref}, all methods return {@code this}.
 * <p>All {@code Abstract*} classes in this package are meant to ease consumption of values loaded
 * via {@link com.dremio.nessie.versioned.VersionStore}, so users do not have to implement every
 * method.</p>
 * <p>{@link Stream}s passed into the default method implementations are fully consumed when invoked.</p>
 */
public abstract class AbstractRef implements Ref {
  @Override
  public Ref id(Id id) {
    return this;
  }

  @Override
  public Ref dt(long dt) {
    return this;
  }

  @Override
  public Ref name(String name) {
    return this;
  }

  @Override
  public Tag tag() {
    return new AbstractTag(this) {
    };
  }

  @Override
  public Branch branch() {
    return new AbstractBranch(this) {
    };
  }

  /**
   * Abstract implementation of {@link Tag}, all methods return {@code this}.
   */
  public abstract static class AbstractTag implements Tag {
    private final Ref ref;

    protected AbstractTag(Ref ref) {
      this.ref = ref;
    }

    @Override
    public Tag commit(Id commit) {
      return this;
    }

    @Override
    public Ref backToRef() {
      return ref;
    }
  }

  /**
   * Abstract implementation of {@link Branch}, all methods return {@code this}.
   * <p>{@link Stream}s passed into the default method implementations are fully consumed when invoked.</p>
   */
  public abstract static class AbstractBranch implements Branch {
    private final Ref ref;

    protected AbstractBranch(Ref ref) {
      this.ref = ref;
    }

    @Override
    public Branch metadata(Id metadata) {
      return this;
    }

    @Override
    public Branch children(Stream<Id> children) {
      children.forEach(ignored -> {});
      return this;
    }

    @Override
    public Branch commits(Consumer<BranchCommit> commits) {
      return this;
    }

    @Override
    public Ref backToRef() {
      return ref;
    }
  }

  /**
   * Abstract implementation of {@link UnsavedCommitDelta}, all methods return {@code this},
   * {@link #saved()} and {@link #unsaved()} return "empty" implementations of
   * {@link Ref.SavedCommit} and {@link Ref.UnsavedCommitDelta}.
   */
  public abstract static class AbstractBranchCommit implements BranchCommit {

    @Override
    public BranchCommit id(Id id) {
      return this;
    }

    @Override
    public BranchCommit commit(Id commit) {
      return this;
    }

    @Override
    public SavedCommit saved() {
      return new AbstractSavedCommit(this) {
      };
    }

    @Override
    public UnsavedCommitDelta unsaved() {
      return new AbstractUnsavedCommitDelta(this) {
      };
    }
  }

  /**
   * Abstract implementation of {@link UnsavedCommitDelta}, all methods return {@code this},
   * {@link #mutations()} returns an "empty" implementation of {@link Ref.UnsavedCommitDelta},
   * keeps a reference to {@link Ref.BranchCommit}.
   */
  public abstract static class AbstractUnsavedCommitDelta implements UnsavedCommitDelta {
    protected final BranchCommit branchCommit;

    protected AbstractUnsavedCommitDelta(BranchCommit branchCommit) {
      this.branchCommit = branchCommit;
    }

    @Override
    public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
      return this;
    }

    @Override
    public UnsavedCommitMutations mutations() {
      return new AbstractUnsavedCommitMutations(branchCommit) {
      };
    }
  }

  /**
   * Abstract implementation of {@link SavedCommit}, all methods return {@code this},
   * keeps a reference to {@link Ref.BranchCommit}.
   */
  public abstract static class AbstractSavedCommit implements SavedCommit {
    protected final BranchCommit branchCommit;

    protected AbstractSavedCommit(BranchCommit branchCommit) {
      this.branchCommit = branchCommit;
    }

    @Override
    public SavedCommit parent(Id parent) {
      return this;
    }

    @Override
    public BranchCommit done() {
      return branchCommit;
    }
  }

  /**
   * Abstract implementation of {@link UnsavedCommitMutations}, all methods return {@code this},
   * keeps a reference to {@link com.dremio.nessie.tiered.builder.Ref.BranchCommit}.
   */
  public abstract static class AbstractUnsavedCommitMutations implements UnsavedCommitMutations {
    protected final BranchCommit branchCommit;

    protected AbstractUnsavedCommitMutations(BranchCommit branchCommit) {
      this.branchCommit = branchCommit;
    }

    @Override
    public UnsavedCommitMutations keyMutation(Mutation keyMutation) {
      return this;
    }

    @Override
    public BranchCommit done() {
      return branchCommit;
    }
  }
}
