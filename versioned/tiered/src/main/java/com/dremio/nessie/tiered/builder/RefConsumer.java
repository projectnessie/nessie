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

/**
 * Reference-consumer for branches + tags.
 * <p>
 * Tags must have the following attributes, other attributes are not allowed:
 * </p>
 * <ul>
 * <li>{@link #id(Id)}</li>
 * <li>{@link #name(String)}</li>
 * <li>{@link #commit(Id)}</li>
 * </ul>
 * <p>
 * Branches must have the following attributes, other attributes are not allowed:
 * </p>
 * <ul>
 * <li>{@link #id(Id)}</li>
 * <li>{@link #name(String)}</li>
 * <li>{@link #commits(Stream)} containing {@link BranchCommit} elements, that represents
 * the commit-log for the branch</li>
 * <li>{@link #metadata(Id)}</li>
 * <li>{@link #children(Stream)} containing {@link Id} elements</li>
 * </ul>
 *
 * <em>Branches</em>
 *
 * <p>The branch state is a current snapshot of the L1 state. It may also include some changes which define how we arrived here.
 *
 * <p>This is basically an L1 but the id of the data and the parent is unknown until we finalize all commits.
 *
 * <p>This contains a commit log of non-finalized commits including deltas. These can be used to build up history of recent
 * commits just in case the L1's associated with each were not saved.
 *
 * <p>The initial structure of the commit log is:</p>
 *
 * <pre>[{id: &lt;EMPTY_COMMIT_ID&gt;, parent: &lt;EMPTY_COMMIT_ID&gt;}]</pre>
 *
 * <p>If a lot of commits come in at the same moment, the log may temporarily be represented like this:
 * <pre>
 * [
 *   {id: &lt;l1Id&gt;, parent: &lt;parent_l1id&gt;}
 *   {id: &lt;randomid&gt;, saved:false, deltas: [
 *       {position:&lt;pos&gt;, oldId: &lt;oldL2Id&gt;, newId: &lt;newL2Id&gt;},
 *       {position:&lt;pos&gt;, oldId: &lt;oldL2Id&gt;, newId: &lt;newL2Id&gt;}
 *       ]
 *   }
 *   {id: &lt;randomid&gt;, saved:false, deltas: [{position:&lt;pos&gt;, oldId: &lt;oldL2Id&gt;, newId: &lt;newL2Id&gt;}]}
 *   {id: &lt;randomid&gt;, saved:false, deltas: [
 *       {position:&lt;pos&gt;, oldId: &lt;oldL2Id&gt;, newId: &lt;newL2Id&gt;},
 *       {position:&lt;pos&gt;, oldId: &lt;oldL2Id&gt;, newId: &lt;newL2Id&gt;}
 *       ]
 *   }
 *   {id: &lt;randomid&gt;, saved:false, deltas: [{position:&lt;pos&gt;, oldId: &lt;oldL2Id&gt;, newId: &lt;newL2Id&gt;}]}
 * ]
 * </pre>
 *
 * <p>In the state shown above, several concurrent commmiters have written their branch commits to the branch
 * but have yet to clean up. In those cases, any one of the committers may clean up some or all of the
 * non-finalized commits (including commits that potentially happened after their own).
 *
 * <p>Each time a commit happens, the committer will do the following:
 * <ol>
 * <li>The mutates the branch tree AND adds their change to the list of commits in the log.
 * <li>Save the tree state (and any other pending saves) based on the result of their mutations in step 1. (*)
 * <li>Remove all finalized commits from the log except the last one. Finalize the last one within the log. (*)
 * </ol>
 *
 * <p>A commit is complete once step (1) above is completed. While steps 2 and 3 are typically also done by the same actor as step 1,
 * they may not be. In situations where that actor dies or is slow, other actors may "finalize" that commit. The commit <b>must</b>
 * be finalized before being exposed to outside consumers of the VersionStore.
 *
 * <p>The following things are always true about the commit log.
 * <ol>
 * <li>There must always be at least one finalized entry.
 * <li>The order of commits will always be &lt;finalized&gt;+&lt;unsaved&gt;*
 * (one or more saved followed by zero or more unsaved commits).
 * <li>The ids for all saved commits will exist in the L1 table.
 * </ol>
 */
public interface RefConsumer extends BaseConsumer<RefConsumer> {
  enum RefType {
    TAG,
    BRANCH
  }

  /**
   * Set the type of the reference.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param refType The type of the reference.
   * @return This consumer.
   */
  RefConsumer type(RefType refType);

  /**
   * Set the name of the reference.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param name The name of the reference.
   * @return This consumer.
   */
  RefConsumer name(String name);

  /**
   * Set the commit of the reference.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param commit The commit of the reference.
   * @return This consumer.
   */
  RefConsumer commit(Id commit);

  /**
   * Set the metadata of the reference.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param metadata The metadata of the reference.
   * @return This consumer.
   */
  RefConsumer metadata(Id metadata);

  /**
   * Set the children of the reference.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param children The children of the reference.
   * @return This consumer.
   */
  RefConsumer children(Stream<Id> children);

  /**
   * Set the commits of the reference.
   *
   * <p>Can be called multiple times. The order of calling is unimportant and could change.
   * @param commits The commits of the reference.
   * @return This consumer.
   */
  RefConsumer commits(Stream<BranchCommit> commits);

  /**
   * Represents a commit in a branch.
   */
  class BranchCommit {

    private final boolean saved;
    private final Id id;
    private final Id commit;
    private final Id parent;
    private final List<BranchUnsavedDelta> deltas;
    private final List<Key> keyAdditions;
    private final List<Key> keyRemovals;

    /**
     * Constructor for a "saved" commit.
     *
     * @param id the ID
     * @param commit commit ID
     * @param parent parent's ID
     */
    public BranchCommit(Id id, Id commit, Id parent) {
      this.id = id;
      this.parent = parent;
      this.commit = commit;
      this.saved = true;
      this.deltas = Collections.emptyList();
      this.keyAdditions = null;
      this.keyRemovals = null;
    }

    /**
     * Constructor for an "unsaved" commit.
     *
     * @param unsavedId the ID
     * @param commit commit ID
     * @param deltas unsaved deltas
     * @param keyAdditions added keys
     * @param keyRemovals removed keys
     */
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

    /**
     * Saved-flag of this commit.
     *
     * @return saved-flag
     */
    public boolean isSaved() {
      return saved;
    }

    /**
     * ID of this commit.
     *
     * @return ID
     */
    public Id getId() {
      return id;
    }

    /**
     * Commit-ID of this commit.
     *
     * @return Commit-ID
     */
    public Id getCommit() {
      return commit;
    }

    /**
     * Parent of this commit.
     *
     * @return parent-ID
     */
    public Id getParent() {
      return parent;
    }

    /**
     * Deltas of this commit.
     *
     * @return unsaved deltas
     */
    public List<BranchUnsavedDelta> getDeltas() {
      return deltas;
    }

    /**
     * Key-additions of this commit.
     *
     * @return added keys
     */
    public List<Key> getKeyAdditions() {
      return keyAdditions;
    }

    /**
     * Key-removals of this commit.
     *
     * @return removed keys
     */
    public List<Key> getKeyRemovals() {
      return keyRemovals;
    }
  }

  /**
   * Represent the "unsaved delta" of a single commit in a branch's commit-log.
   */
  class BranchUnsavedDelta {
    private final int position;
    private final Id oldId;
    private final Id newId;

    /**
     * Constructor.
     *
     * @param position position
     * @param oldId old-ID
     * @param newId new-ID
     */
    public BranchUnsavedDelta(int position, Id oldId, Id newId) {
      this.position = position;
      this.oldId = oldId;
      this.newId = newId;
    }

    /**
     * Position of this delta.
     *
     * @return position
     */
    public int getPosition() {
      return position;
    }

    /**
     * Old-ID of this delta.
     *
     * @return old-ID
     *
     */
    public Id getOldId() {
      return oldId;
    }

    /**
     * New-ID of this delta.
     *
     * @return new-ID
     */
    public Id getNewId() {
      return newId;
    }
  }
}
