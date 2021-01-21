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

import java.util.function.Consumer;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;

/**
 * Reference-consumer for branches + tags.
 * <p>
 * Tags are handled by calling {@link #tag()} and setting the following attributes:
 * </p>
 * <ul>
 * <li>{@link #tag()}</li>
 * <li>{@link #id(Id)}</li>
 * <li>{@link #name(String)}</li>
 * <li>{@link Tag#commit(Id)}</li>
 * </ul>
 * <p>
 * Branches must have the following attributes, other attributes are not allowed:
 * </p>
 * <ul>
 * <li>{@link #branch()}</li>
 * <li>{@link #id(Id)}</li>
 * <li>{@link #name(String)}</li>
 * <li>{@link Branch#commits(Consumer)} receives a {@link BranchCommit} that must be used to
 * immediately execute the callbacks that represent the commit-log for the branch</li>
 * <li>{@link Branch#metadata(Id)}</li>
 * <li>{@link Branch#children(Stream)} containing {@link Id} elements</li>
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
public interface Ref extends BaseValue<Ref> {
  /**
   * Set the name of the reference.
   * <p>Must be called exactly once.</p>
   *
   * @param name The name of the reference.
   * @return This consumer.
   */
  Ref name(String name);

  /**
   * Specifies that this reference is a tag, not a branch.
   * <p>Either this function or {@link #branch()} must be called.</p>
   * @return consumer instance for tags.
   */
  Tag tag();

  /**
   * Specifies that this reference is a branch, not a tag.
   * <p>Either this function or {@link #tag()} must be called.</p>
   * @return consumer instance for branches.
   */
  Branch branch();

  interface Tag {
    /**
     * Set the commit of the reference.
     * <p>Must be called exactly once.</p>
     *
     * @param commit The commit of the reference.
     * @return This consumer.
     */
    Tag commit(Id commit);
  }

  interface Branch {

    /**
     * Set the metadata of the reference.
     * <p>Must be called exactly once.</p>
     *
     * @param metadata The metadata of the reference.
     * @return This consumer.
     */
    Branch metadata(Id metadata);

    /**
     * Set the children of the reference.
     * <p>Must be called exactly once.</p>
     *
     * @param children The children of the reference.
     * @return This consumer.
     */
    Branch children(Stream<Id> children);

    /**
     * Set the commits of the reference.
     * <p>Must be called exactly once.</p>
     * <p>Implementations must immediately execute the callbacks in the {@link
     * BranchCommit}
     * to construct the commit-log of the branch.</p>
     *
     * @param commits The branch's commit-log receiver.
     * @return This consumer.
     */
    Branch commits(Consumer<BranchCommit> commits);
  }

  /**
   * Users of this consumers must call the methods in the following order.
   * <ol>
   *   <li>{@link #id(Id)} (mandatory), {@link #commit(Id)} (mandatory)</li>
   *   <li>Either {@link #saved()} or {@link #unsaved()}</li>
   *   <li>Each saved and unsaved branch-commit must be terminated with its {@code done()} method</li>
   * </ol>
   */
  interface BranchCommit {

    /**
     * ID of the branch's commit.
     * <p>Must be called exactly once for each branch-commit.</p>
     * @param id ID of the branch's commit
     * @return this consumer
     */
    BranchCommit id(Id id);

    /**
     * Commit of the branch's commit.
     * <p>Must be called exactly once for each branch-commit.</p>
     * @param commit Commit of the branch's commit
     * @return this consumer
     */
    BranchCommit commit(Id commit);

    /**
     * Continue with this branch-commit as a saved commit.
     * @return consumer that takes the parent for the saved-commit.
     */
    SavedCommit saved();

    /**
     * Continue with this branch-commit as an unsaved commit.
     * @return consumer that takes the deltas and later the mutations for the unsaved-commit.
     */
    UnsavedCommitDelta unsaved();
  }

  interface SavedCommit {
    /**
     * Parent of the branch's commit.
     * <p>Must be called exactly once for each saved branch-commit.</p>
     * @param parent Parent of the branch's commit
     * @return this consumer
     */
    SavedCommit parent(Id parent);

    /**
     * End the current commit.
     * @return this consumer
     */
    BranchCommit done();
  }

  interface UnsavedCommitDelta {

    /**
     * Add a delta.
     * <p>Can be called multiple times, but must be called after {@link #id(Id)} and {@link
     * BranchCommit#commit(Id)}.</p>
     *
     * @param position delta-position
     * @param oldId    delta-old-id
     * @param newId    delta-new-id
     * @return this consumer
     */
    UnsavedCommitDelta delta(int position, Id oldId, Id newId);

    /**
     * Continue with the mutations for this unsaved-commit.
     * @return consumer that takes the key-mutations for this unsaved-commit.
     */
    UnsavedCommitMutations mutations();
  }

  interface UnsavedCommitMutations {
    /**
     * Add a key-mutation.
     * <p>Can be called multiple times.</p>
     * @param keyMutation key-mutation
     * @return this consumer
     */
    UnsavedCommitMutations keyMutation(Key.Mutation keyMutation);

    /**
     * End the current commit.
     * @return this consumer
     */
    BranchCommit done();
  }
}
