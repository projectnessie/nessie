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

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.tiered.builder.Ref.UnsavedCommitDelta;
import com.dremio.nessie.tiered.builder.Ref.UnsavedCommitMutations;
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
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Stores the current state of branch.
 *
 * @see Ref for a detailed description
 */
class InternalBranch extends InternalRef {

  static final String ID = "id";
  static final String TREE = "tree";
  static final String COMMITS = "commits";

  private static final List<Commit> SINGLE_EMPTY_COMMIT = ImmutableList.of(new Commit(InternalL1.EMPTY_ID, Id.EMPTY, Id.EMPTY));

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
    this(InternalRefId.ofBranch(name).getId(), name, InternalL1.EMPTY.getMap(), Id.EMPTY, SINGLE_EMPTY_COMMIT, DT.now());
  }

  /**
   * Create a new branch targeting an L1.
   * @param name name of the branch
   * @param target the L1 to target (should already be persisted)
   */
  public InternalBranch(String name, InternalL1 target) {
    this(InternalRefId.ofBranch(name).getId(), name, target.getMap(), Id.EMPTY,
        ImmutableList.of(new Commit(target.getId(), target.getMetadataId(), target.getParentId())), DT.now());
  }

  InternalBranch(Id id, String name, IdMap tree, Id metadata, List<Commit> commits, Long dt) {
    super(id, dt);
    this.metadata = metadata;
    this.name = name;
    this.tree = tree;
    this.commits = commits;
    assert tree.size() == InternalL1.SIZE;
    ensureConsistentId();
  }


  /**
   * Get the most recent saved parent within the list of commits.
   *
   * <p>This is useful for doing garbage collection reference tracking.
   *
   * @return The Id of the most recent saved parent. Could be L1.EMPTY.
   */
  Id getLastDefinedParent() {
    for (Commit c : Lists.reverse(commits)) {
      if (c.isSaved()) {
        return c.id;
      }
    }

    throw new IllegalStateException("Unable to determine last defined parent.");
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
          && Objects.equal(deltas, commit1.deltas)
          && KeyMutationList.equalsIgnoreOrder(keyMutationList, commit1.keyMutationList);
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
        builder.put(KEY_MUTATIONS, item.keyMutationList.toEntity());
      }
      return builder.build();
    }
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

    InternalL1 lastSavedL1 = lastSavedCommit.id.isEmpty() ? InternalL1.EMPTY : EntityType.L1.loadSingle(store, lastSavedCommit.id);

    if (unsavedCommits.isEmpty()) {
      return new UpdateState(Collections.emptyList(), deletes, lastSavedL1, 0, lastSavedL1.getId(), this);
    }

    // first we rewind the tree to the original state
    for (Commit c : Lists.reverse(unsavedCommits)) {
      for (UnsavedDelta delta : c.deltas) {
        tree = delta.reverse(tree);
      }
    }

    InternalL1 lastL1 = lastSavedL1;
    int lastPos = unsavedStartOffset;
    Id lastId = null;
    final List<SaveOp<?>> toSave = new ArrayList<>();

    for (Commit c : unsavedCommits) {
      for (UnsavedDelta delta : c.deltas) {
        tree = delta.apply(tree);
      }
      lastL1 = lastL1.getChildWithTree(c.commit, tree, c.keyMutationList)
          .withCheckpointAsNecessary(store);
      toSave.add(EntityType.L1.createSaveOpForEntity(lastL1));
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
    private final InternalL1 finalL1;
    private final int finalL1position;
    private final Id finalL1RandomId;
    private final InternalBranch initialBranch;

    private UpdateState(
        List<SaveOp<?>> saves,
        List<Delete> deletes,
        InternalL1 finalL1,
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

          InternalRef.Builder producer = EntityType.REF.newEntityProducer();
          boolean updated = store.update(ValueType.REF, branch.getId(), update, Optional.of(condition), Optional.of(producer));
          if (updated) {
            LOGGER.debug("Completed collapse update on attempt {}.", attempt);
            return producer.build().getBranch();
          }

          LOGGER.debug("Failed to collapse update on attempt {}.", attempt);
          // something must have changed, reload the branch.
          final InternalRef ref = EntityType.REF.loadSingle(store, branch.getId());
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

    public InternalL1 getL1() {
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

    Map<String, Entity> itemToMap() {
      return ImmutableMap.<String, Entity>builder()
          .put(POSITION, Entity.ofNumber(position))
          .put(OLD_ID, oldId.toEntity())
          .put(NEW_ID, newId.toEntity())
          .build();
    }
  }

  @Override
  Id generateId() {
    return Id.build(name);
  }

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
  Ref applyToConsumer(Ref consumer) {
    return super.applyToConsumer(consumer)
        .name(name)
        .branch()
        .metadata(metadata)
        .children(this.tree.stream())
        .commits(cc -> {
          for (Commit c : commits) {
            cc.id(c.id)
                .commit(c.commit);
            if (c.saved) {
              cc.saved()
                  .parent(c.parent)
                  .done();
            } else {
              UnsavedCommitDelta deltas = cc.unsaved();
              c.deltas.forEach(d -> deltas.delta(
                      d.position,
                      d.oldId,
                      d.newId
                  ));

              UnsavedCommitMutations mutations = deltas.mutations();
              c.keyMutationList.getMutations().forEach(km -> mutations.keyMutation(km.toMutation()));

              mutations.done();
            }
          }
        });
  }

}
