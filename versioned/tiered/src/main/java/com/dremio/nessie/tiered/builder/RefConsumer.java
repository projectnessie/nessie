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
package com.dremio.nessie.tiered.builder;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public interface RefConsumer extends HasIdConsumer<RefConsumer> {
  enum RefType {
    TAG,
    BRANCH
  }

  /**
   * TODO javadoc.
   */
  RefConsumer id(Id id);

  /**
   * TODO javadoc.
   */
  RefConsumer type(RefType refType);

  /**
   * TODO javadoc.
   */
  RefConsumer name(String name);

  /**
   * TODO javadoc.
   */
  RefConsumer commit(Id commit);

  /**
   * TODO javadoc.
   */
  RefConsumer metadata(Id metadata);

  /**
   * TODO javadoc.
   */
  RefConsumer children(Stream<Id> children);

  /**
   * TODO javadoc.
   */
  RefConsumer commits(Stream<BranchCommit> commits);

  class BranchCommit {

    private final boolean saved;
    private final Id id;
    private final Id commit;
    private final Id parent;
    private final List<BranchUnsavedDelta> deltas;
    private final List<Key> keyAdditions;
    private final List<Key> keyRemovals;

    public BranchCommit(Id id, Id commit, Id parent) {
      this.id = id;
      this.parent = parent;
      this.commit = commit;
      this.saved = true;
      this.deltas = Collections.emptyList();
      this.keyAdditions = null;
      this.keyRemovals = null;
    }

    public BranchCommit(Id unsavedId, Id commit, List<BranchUnsavedDelta> deltas, List<Key> keyAdditions, List<Key> keyRemovals) {
      super();
      this.saved = false;
      this.deltas = ImmutableList.copyOf(Preconditions.checkNotNull(deltas));
      this.commit = Preconditions.checkNotNull(commit);
      this.parent = null;
      this.keyAdditions = Preconditions.checkNotNull(keyAdditions);
      this.keyRemovals = Preconditions.checkNotNull(keyRemovals);
      this.id = Preconditions.checkNotNull(unsavedId);
    }

    public boolean isSaved() {
      return saved;
    }

    public Id getId() {
      return id;
    }

    public Id getCommit() {
      return commit;
    }

    public Id getParent() {
      return parent;
    }

    public List<BranchUnsavedDelta> getDeltas() {
      return deltas;
    }

    public List<Key> getKeyAdditions() {
      return keyAdditions;
    }

    public List<Key> getKeyRemovals() {
      return keyRemovals;
    }
  }

  class BranchUnsavedDelta {
    private final int position;
    private final Id oldId;
    private final Id newId;

    public BranchUnsavedDelta(int position, Id oldId, Id newId) {
      this.position = position;
      this.oldId = oldId;
      this.newId = newId;
    }

    public int getPosition() {
      return position;
    }

    public Id getOldId() {
      return oldId;
    }

    public Id getNewId() {
      return newId;
    }
  }
}
