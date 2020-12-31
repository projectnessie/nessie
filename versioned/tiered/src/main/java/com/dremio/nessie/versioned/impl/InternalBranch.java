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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

/**
 * Stores the current state of branch.
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
 * they may not be. In situations where that actor dies or is slow, other actors are may "finalize" that commit. The commit <b>must</b>
 * be finalized before being exposed to outside consumers of the VersionStore.
 *
 * <p>The following things are always true about the commit log.
 * <ol>
 * <li>There must always be at least one finalized entry.
 * <li>There order of commits will always be &lt;finalized&gt;+&lt;unsaved&gt;*
 * (one or more saved followed by zero or more unsaved commits).
 * <li>The ids for all saved commits will exist in the L1 table.
 * </ol>
 */
class InternalBranch extends MemoizedId implements InternalRef, Persistent<RefConsumer<?>> {

  static final String ID = "id";
  static final String NAME = "name";
  static final String METADATA = "metadata";
  static final String TREE = "tree";
  static final String COMMITS = "commits";

  private static final List<Commit> SINGLE_EMPTY_COMMIT = ImmutableList.of(new Commit(L1.EMPTY_ID, Id.EMPTY, Id.EMPTY));

  private static final Logger LOGGER = LoggerFactory.getLogger(InternalBranch.class);

  private final String name;
  private final IdMap tree;
  private final Id metadata;
  private final List<Commit> commits;

  /**
   * Create an empty branch.
   * @param name name of the branch.
   */
  public InternalBranch(String name) {
    this(InternalRefId.ofBranch(name).getId(), name, L1.EMPTY.getMap(), Id.EMPTY, SINGLE_EMPTY_COMMIT);
  }

  /**
   * Create a new branch targeting an L1.
   * @param name name of the branch
   * @param target the L1 to target (should already be persisted)
   */
  public InternalBranch(String name, L1 target) {
    this(InternalRefId.ofBranch(name).getId(), name, target.getMap(), Id.EMPTY,
        ImmutableList.of(new Commit(target.getId(), target.getMetadataId(), target.getParentId())));
  }

  InternalBranch(Id id, String name, IdMap tree, Id metadata, List<Commit> commits) {
    super(id);
    this.metadata = metadata;
    this.name = name;
    this.tree = tree;
    this.commits = commits;
    assert tree.size() == L1.SIZE;
    ensureConsistentId();
  }

  public String getName() {
    return name;
  }

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
    private final KeyMutationList keyMutationList;

    public Commit(Id id, Id commit, Id parent) {
      this.id = id;
      this.parent = parent;
      this.commit = commit;
      this.saved = true;
      this.deltas = Collections.emptyList();
      this.keyMutationList = null;
    }

    public Commit(Id unsavedId, Id commit, List<UnsavedDelta> deltas, KeyMutationList keyMutationList) {
      super();
      this.saved = false;
      this.deltas = ImmutableList.copyOf(Preconditions.checkNotNull(deltas));
      this.commit = Preconditions.checkNotNull(commit);
      this.parent = null;
      this.keyMutationList = Preconditions.checkNotNull(keyMutationList);
      this.id = Preconditions.checkNotNull(unsavedId);
    }

    Id getParent() {
      Preconditions.checkArgument(saved, "Can only retrieve parent on saved commits.");
      return parent;
    }

    Id getId() {
      return id;
    }

    public boolean isSaved() {
      return saved;
    }

    public Entity toEntity() {
      return Entity.ofMap(SCHEMA.itemToMap(this, true));
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
          && Objects.equal(deltas, commit1.deltas)
          && KeyMutationList.equalsIgnoreOrder(keyMutationList, commit1.keyMutationList);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(saved, id, commit, parent, deltas, keyMutationList);
    }

    static final SimpleSchema<Commit> SCHEMA = new SimpleSchema<Commit>(Commit.class) {

      @Override
      public Commit deserialize(Map<String, Entity> map) {
        if (!map.containsKey(DELTAS)) {
          return new Commit(Id.fromEntity(map.get(ID)), Id.fromEntity(map.get(COMMIT)),
              Id.fromEntity(map.get(PARENT)));
        }

        List<UnsavedDelta> deltas = map.get(DELTAS)
            .getList()
            .stream()
            .map(av -> UnsavedDelta.SCHEMA.mapToItem(av.getMap()))
            .collect(Collectors.toList());
        return new Commit(
            Id.fromEntity(map.get(ID)),
            Id.fromEntity(map.get(COMMIT)),
            deltas,
            KeyMutationList.fromEntity(map.get(KEY_MUTATIONS))
            );

      }

      @Override
      public Map<String, Entity> itemToMap(Commit item, boolean ignoreNulls) {
        ImmutableMap.Builder<String, Entity> builder = ImmutableMap.builder();
        builder
          .put(ID, item.getId().toEntity())
            .put(COMMIT, item.commit.toEntity());
        if (item.saved) {
          builder.put(PARENT, item.parent.toEntity());
        } else {
          Entity deltas = Entity.ofList(
              item.deltas.stream().map(
                  d -> Entity.ofMap(
                      UnsavedDelta.SCHEMA.itemToMap(d, true)
                      )
                  ).collect(Collectors.toList()));
          builder.put(DELTAS, deltas);
          builder.put(KEY_MUTATIONS, item.keyMutationList.toEntity());
        }
        return builder.build();
      }
    };
  }

  /**
   * Identify the list of intended commits that need to be completed.
   * @return
   */
  public UpdateState getUpdateState(Store store)  {
    // generate sublist of important commits.
    List<Commit> unsavedCommits = new ArrayList<>();
    Commit lastSavedCommit = null;
    boolean inUnsaved = false;
    assert !commits.isEmpty();
    int unsavedStartOffset = 0;
    Commit lastUnsaved = null;
    for (Commit c : commits) {
      if (c.saved) {
        lastSavedCommit = c;
        unsavedStartOffset++;
        assert !inUnsaved;
      } else {
        assert lastSavedCommit != null;
        inUnsaved = true;
        unsavedCommits.add(c);
        lastUnsaved = c;
      }
    }

    final List<Delete> deletes = new ArrayList<>();

    { // delete excess deletes.
      // delete all but last item.
      for (int i = 0; i < commits.size() - 1; i++) {
        deletes.add(new Delete(i, commits.get(i).id));
      }
    }

    IdMap tree = this.tree;

    L1 lastSavedL1 = lastSavedCommit.id.isEmpty() ? L1.EMPTY : store.loadSingle(ValueType.L1, lastSavedCommit.id);

    if (unsavedCommits.isEmpty()) {
      return new UpdateState(Collections.emptyList(), deletes, lastSavedL1, 0, lastSavedL1.getId(), this);
    }

    // first we rewind the tree to the original state
    for (Commit c : Lists.reverse(unsavedCommits)) {
      for (UnsavedDelta delta : c.deltas) {
        tree = delta.reverse(tree);
      }
    }

    L1 lastL1 = lastSavedL1;
    int lastPos = unsavedStartOffset;
    Id lastId = null;
    final List<SaveOp<?>> toSave = new ArrayList<>();

    for (Commit c : unsavedCommits) {
      for (UnsavedDelta delta : c.deltas) {
        tree = delta.apply(tree);
      }
      lastL1 = lastL1.getChildWithTree(c.commit, tree, c.keyMutationList)
          .withCheckpointAsNecessary(store);
      toSave.add(new SaveOp<L1>(ValueType.L1, lastL1));
      lastId = c.id;
      if (lastUnsaved != c) {
        // update for next loop.
        lastPos++;
      }
    }

    // now we should have the same tree as we originally did.
    assert tree.equals(this.tree);
    return new UpdateState(toSave, deletes, lastL1, lastPos, lastId, this);
  }

  static final class UpdateState {
    private volatile boolean saved = false;
    private final List<SaveOp<?>> saves;
    private final List<Delete> deletes;
    private final L1 finalL1;
    private final int finalL1position;
    private final Id finalL1RandomId;
    private final InternalBranch initialBranch;

    private UpdateState(
        List<SaveOp<?>> saves,
        List<Delete> deletes,
        L1 finalL1,
        int finalL1position,
        Id finalL1RandomId,
        InternalBranch initialBranch) {
      super();
      this.saves = Preconditions.checkNotNull(saves);
      this.deletes = Preconditions.checkNotNull(deletes);
      this.finalL1 = Preconditions.checkNotNull(finalL1);
      this.finalL1position = finalL1position;
      this.finalL1RandomId = Preconditions.checkNotNull(finalL1RandomId);
      this.initialBranch = Preconditions.checkNotNull(initialBranch);
      if (finalL1position == 0 && !deletes.isEmpty()) {
        throw new IllegalStateException("We should never have deletes if the final position is zero.");
      }
    }

    /**
     * Ensure that all l1s to save are available. Returns once the L1s reference in this object are available for
     * reference. Before returning, will also submit a separate thread to the provided executor that will attempt to
     * clean up the existing commit log. Once the log is cleaned up, the returned CompletableFuture will return a cleaned InternalBranch.
     *
     * @param store The store to save to.
     * @param executor The executor to do any necessary clean up of the commit log.
     * @param attempts The number of times we'll attempt to clean up the commit log.
     * @param waitOnCollapse Whether or not the operation should wait on the final operation of collapsing the commit log succesfully
     *        before returning/failing. If false, the final collapse will be done in a separate thread.
     * @return
     */
    @SuppressWarnings("unchecked")
    CompletableFuture<InternalBranch> ensureAvailable(Store store, Executor executor, int attempts, boolean waitOnCollapse) {
      if (saves.isEmpty()) {
        saved = true;
        return CompletableFuture.completedFuture(initialBranch);
      }

      store.save(saves);
      saved = true;

      CompletableFuture<InternalBranch> future = CompletableFuture.supplyAsync(() -> {
        try {
          return collapseIntentionLog(this, store, initialBranch, attempts);
        } catch (ReferenceNotFoundException | ReferenceConflictException e) {
          throw new CompletionException(e);
        }
      }, executor);

      if (!waitOnCollapse) {
        return future;
      }

      try {
        future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        Throwables.throwIfUnchecked(e.getCause());
        throw new IllegalStateException(e.getCause());
      }
      return future;
    }

    /**
     * Collapses the intention log within a branch, reattempting multiple times.
     *
     * <p>After completing an operation, we should attempt to collapse the intention log. There are two steps associated with this:
     *
     * <ul>
     * <li>Save all the unsaved items in the intention log.
     * <li>Removing all the unsaved items in the intention log except the last one, which will be
     * converted to an id pointer of the previous commit.
     * </ul>
     *
     * @param branch The branch that potentially has items to collapse.
     * @param attempts Number of attempts to make before giving up on collapsing. (This is an optimistic locking scheme.)
     * @return The updated branch object after the intention log was collapsed.
     * @throws ReferenceNotFoundException when branch does not exist.
     * @throws ReferenceConflictException If attempts are depleted and operation cannot be applied due to heavy concurrency
     */
    private static InternalBranch collapseIntentionLog(UpdateState initialState, Store store, InternalBranch branch, int attempts)
        throws ReferenceNotFoundException, ReferenceConflictException {
      try {
        for (int attempt = 0; attempt < attempts; attempt++) {

          // cleanup pending updates.
          UpdateState updateState = attempts == 0 ? initialState : branch.getUpdateState(store);

          // now we need to take the current list and turn it into a list of 1 item that is saved.
          final ExpressionPath commits = ExpressionPath.builder("commits").build();
          final ExpressionPath last = commits.toBuilder().position(updateState.finalL1position).build();

          UpdateExpression update = UpdateExpression.initial();
          ConditionExpression condition = ConditionExpression.initial();

          for (Delete d : updateState.deletes) {
            ExpressionPath path = commits.toBuilder().position(d.position).build();
            condition = condition.and(ExpressionFunction.equals(path.toBuilder().name(ID).build(), d.id.toEntity()));
            update = update.and(RemoveClause.of(path));
          }

          condition = condition.and(ExpressionFunction.equals(last.toBuilder().name(ID).build(),
              updateState.finalL1RandomId.toEntity()));

          // remove extra commits field for last commit.
          update = update
              .and(RemoveClause.of(last.toBuilder().name(Commit.DELTAS).build()))
              .and(RemoveClause.of(last.toBuilder().name(Commit.KEY_MUTATIONS).build()))
              .and(SetClause.equals(last.toBuilder().name(Commit.PARENT).build(), updateState.finalL1.getParentId().toEntity()))
              .and(SetClause.equals(last.toBuilder().name(Commit.ID).build(), updateState.finalL1.getId().toEntity()));

          Optional<InternalRef> updated = store.update(ValueType.REF, branch.getId(), update, Optional.of(condition));
          if (updated.isPresent()) {
            LOGGER.debug("Completed collapse update on attempt {}.", attempt);
            return updated.get().getBranch();
          }

          LOGGER.debug("Failed to collapse update on attempt {}.", attempt);
          // something must have changed, reload the branch.
          final InternalRef ref = store.loadSingle(ValueType.REF, branch.getId());
          if (ref.getType() != Type.BRANCH) {
            throw new ReferenceNotFoundException("Failure while collapsing log. Former branch is now a " + ref.getType());
          }
          branch = ref.getBranch();
        }

      } catch (Exception ex) {
        LOGGER.debug("Exception when trying to update item.", ex);
      }
      throw new ReferenceConflictException(String.format("Unable to collapse intention log after %d attempts, giving up.", attempts));
    }

    public L1 getL1() {
      Preconditions.checkArgument(saved,
          "You must call UpdateState.ensureAvailable() before attempting to retrieve the L1 state of this branch.");
      return finalL1;
    }
  }

  static class Delete {
    int position;
    Id id;

    public Delete(int position, Id id) {
      super();
      this.position = position;
      this.id = id;
    }
  }

  public static class UnsavedDelta {

    private static final String POSITION = "position";
    private static final String NEW_ID = "new";
    private static final String OLD_ID = "old";

    private final int position;
    private final Id oldId;
    private final Id newId;


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

    static final SimpleSchema<UnsavedDelta> SCHEMA = new SimpleSchema<UnsavedDelta>(UnsavedDelta.class) {

      @Override
      public UnsavedDelta deserialize(Map<String, Entity> map) {
        return new UnsavedDelta(
            Ints.saturatedCast(map.get(POSITION).getNumber()),
            Id.of(map.get(OLD_ID).getBinary()),
            Id.of(map.get(NEW_ID).getBinary())
            );
      }

      @Override
      public Map<String, Entity> itemToMap(UnsavedDelta item, boolean ignoreNulls) {
        return ImmutableMap.<String, Entity>builder()
            .put(POSITION, Entity.ofNumber(item.position))
            .put(OLD_ID, item.oldId.toEntity())
            .put(NEW_ID, item.newId.toEntity())
            .build();
      }
    };
  }

  @Override
  Id generateId() {
    return Id.build(name);
  }

  static final SimpleSchema<InternalBranch> SCHEMA = new SimpleSchema<InternalBranch>(InternalBranch.class) {

    @Override
    public InternalBranch deserialize(Map<String, Entity> attributeMap) {
      return new InternalBranch(
          Id.fromEntity(attributeMap.get(ID)),
          attributeMap.get(NAME).getString(),
          IdMap.fromEntity(attributeMap.get(TREE), L1.SIZE),
          Id.fromEntity(attributeMap.get(METADATA)),
          attributeMap.get(COMMITS).getList().stream().map(av -> Commit.SCHEMA.mapToItem(av.getMap())).collect(Collectors.toList())
      );
    }

    @Override
    public Map<String, Entity> itemToMap(InternalBranch item, boolean ignoreNulls) {
      return ImmutableMap.<String, Entity>builder()
          .put(ID, item.getId().toEntity())
          .put(NAME, Entity.ofString(item.name))
          .put(METADATA, item.metadata.toEntity())
          .put(COMMITS, Entity.ofList(item.commits.stream().map(Commit::toEntity).collect(Collectors.toList())))
          .put(TREE, item.tree.toEntity())
          .build();
    }

  };

  @Override
  public Type getType() {
    return Type.BRANCH;
  }

  @Override
  public InternalBranch getBranch() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InternalBranch that = (InternalBranch) o;
    return Objects.equal(name, that.name) && Objects
        .equal(tree, that.tree) && Objects.equal(metadata, that.metadata)
        && Objects.equal(commits, that.commits);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, tree, metadata, commits);
  }

  @Override
  public ValueType type() {
    return ValueType.REF;
  }

  @Override
  public RefConsumer<?> applyToConsumer(RefConsumer<?> consumer) {
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

}
