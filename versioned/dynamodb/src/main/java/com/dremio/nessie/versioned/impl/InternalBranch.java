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
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.InternalRef.Type;
import com.dremio.nessie.versioned.impl.Store.ValueType;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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
 * <p>The initial structure of the commit log is:
 * <pre>[{id: <EMPTY_COMMIT_ID>, parent: <EMPTY_COMMIT_ID>}]</pre>
 *
 * <p>If a lot of commits come in at the same moment, the log may temporarily be represented like this:
 * <pre>
 * [
 *   {id: <l1Id>, parent: <parent_l1id>}
 *   {id: <randomid>, saved:false, deltas: [{position:<pos>, oldId: <oldL2Id>, newId: <newL2Id>},{position:<pos>, oldId: <oldL2Id>, newId: <newL2Id>}]}
 *   {id: <randomid>, saved:false, deltas: [{position:<pos>, oldId: <oldL2Id>, newId: <newL2Id>}]}
 *   {id: <randomid>, saved:false, deltas: [{position:<pos>, oldId: <oldL2Id>, newId: <newL2Id>},{position:<pos>, oldId: <oldL2Id>, newId: <newL2Id>}]}
 *   {id: <randomid>, saved:false, deltas: [{position:<pos>, oldId: <oldL2Id>, newId: <newL2Id>}]}
 * ]
 * </pre>
 * <p>In the state shown above, several concurrent commmiters have written their branch commits to the branch but have yet to clean up. In those cases,
 * any one of the committers may clean up some or all of the non-finalized commits (including commits that potentially happened after their own).
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
 * <li>There order of commits will always be <finalized>+<unsaved>* (one or more saved followed by zero or more unsaved commits).
 * <li>The ids for all saved commits will exist in the L1 table.
 * <ul>
 */
class InternalBranch extends MemoizedId {

  static final String ID = "id";
  static final String NAME = "name";
  static final String METADATA = "metadata";
  static final String TREE = "tree";
  static final String COMMITS = "commits";

  private static final Logger LOGGER = LoggerFactory.getLogger(InternalBranch.class);

  private final String name;
  private final IdMap tree;
  private final Id metadata;
  private final List<Commit> commits;

  /**
   * Create an empty branch.
   * @param name
   */
  public InternalBranch(String name) {
    super(InternalRefId.ofBranch(name).getId());
    this.name = name;
    this.metadata = Id.EMPTY;
    this.tree = L1.EMPTY.getMap();
    this.commits = ImmutableList.of(new Commit(L1.EMPTY.getId(), Id.EMPTY, L1.EMPTY.getId()));
  }

  /**
   * Create a new branch targeting an L1.
   * @param name
   * @param target
   */
  public InternalBranch(String name, L1 target) {
    super(InternalRefId.ofBranch(name).getId());
    this.name = name;
    this.metadata = Id.EMPTY;
    this.tree = target.getMap();
    this.commits = ImmutableList.of(new Commit(target.getId(), target.getMetadataId(), target.getParentId()));
  }

  private InternalBranch(Id id, String name, IdMap tree, Id metadata, List<Commit> commits) {
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

  public static class Commit {

    static final String ID = "id";
    static final String COMMIT = "commit";
    static final String DELTAS = "deltas";
    static final String PARENT = "parent";

    private final Boolean saved;
    private final Id id;
    private final Id commit;
    private final Id parent;
    private final List<UnsavedDelta> deltas;

    public Commit(Id id, Id commit, Id parent) {
      this.id = id;
      this.parent = parent;
      this.commit = commit;
      this.saved = true;
      this.deltas = Collections.emptyList();
    }

    public Commit(Id unsavedId, Id commit, List<UnsavedDelta> deltas) {
      super();
      this.saved = false;
      this.deltas = deltas;
      this.commit = commit;
      this.parent = null;
      this.id = unsavedId == null ? generateUnsavedHash() : unsavedId;
    }

    private Id generateUnsavedHash() {
      //TODO: reevaluate, we could should probably make this entirely random.
      return Id.build(h -> {
        commit.addToHash(h);
        deltas.forEach(d -> {
          h.putInt(d.position);
          d.newId.addToHash(h);
        });
      });
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

    public AttributeValue toAttributeValue() {
      return AttributeValue.builder().m(SCHEMA.itemToMap(this, true)).build();
    }

    static final TableSchema<Commit> SCHEMA = new SimpleSchema<Commit>(Commit.class) {

      @Override
      public Commit deserialize(Map<String, AttributeValue> map) {
        if(!map.containsKey(DELTAS)) {
          System.out.println("[BRANCH]: " + map);
          return new Commit(Id.fromAttributeValue(map.get(ID)), Id.fromAttributeValue(map.get(COMMIT)), Id.fromAttributeValue(map.get(PARENT)));
        } else {
          List<UnsavedDelta> deltas = map.get(DELTAS)
              .l()
              .stream()
              .map(av -> UnsavedDelta.SCHEMA.mapToItem(av.m()))
              .collect(Collectors.toList());
          return new Commit(
              Id.fromAttributeValue(map.get(ID)),
              Id.fromAttributeValue(map.get(COMMIT)),
              deltas
              );
        }
      }

      @Override
      public Map<String, AttributeValue> itemToMap(Commit item, boolean ignoreNulls) {
        ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
        builder
          .put(ID, item.getId().toAttributeValue())
          .put(COMMIT, item.commit.toAttributeValue());
        if(item.saved) {
          builder.put(PARENT, item.parent.toAttributeValue());
        } else {
          AttributeValue deltas = AttributeValue.builder().l(
              item.deltas.stream().map(
                  d -> AttributeValue.builder().m(
                      UnsavedDelta.SCHEMA.itemToMap(d, true)
                      ).build()
                  ).collect(Collectors.toList()))
          .build();
          builder.put(DELTAS, deltas);
        }
        return builder.build();
      }
    };
  }

  /**
   * Identify the list of intended commits that need to be completed.
   * @return
   */
  public UpdateState getUpdateState()  {
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
      for(int i =0; i < commits.size() - 1; i++) {
        deletes.add(new Delete(i, commits.get(i).id));
      }
    }

    IdMap tree = this.tree;

    if (unsavedCommits.isEmpty()) {
      L1 l1 = new L1(lastSavedCommit.commit, lastSavedCommit.parent, tree);
      return new UpdateState(Collections.emptyList(), deletes, l1, 0, l1.getId(), this);
    }
    // first we rewind the tree to the original state
    for (Commit c : Lists.reverse(unsavedCommits)) {
      for(UnsavedDelta delta : c.deltas) {
        tree = delta.reverse(tree);
      }
    }

    L1 lastL1 = null;
    int lastPos = unsavedStartOffset;
    Id lastId = null;
    final List<L1> toSave = new ArrayList<>();

    Id parentId = lastSavedCommit.id;
    for (Commit c : unsavedCommits) {
      for(UnsavedDelta delta : c.deltas) {
        tree = delta.apply(tree);
      }
      L1 l1 = new L1(c.commit, parentId, tree);
      toSave.add(l1);

      if (lastUnsaved == c) {
        lastL1 = l1;
        lastId = c.id;
      } else {
        // update for next loop.
        lastPos++;
      }
      parentId = l1.getId();

    }

    // now we should have the same tree as we originally did.
    assert tree.equals(this.tree);
    return new UpdateState(toSave, deletes, lastL1, lastPos, lastId, this);
  }


  static class UpdateState {
    private volatile boolean saved = false;
    private final List<L1> l1sToSave;
    private final List<Delete> deletes;
    private final L1 finalL1;
    private final int finalL1position;
    private final Id finalL1RandomId;
    private final InternalBranch initialBranch;

    private UpdateState(List<L1> l1sToSave, List<Delete> deletes, L1 finalL1, int finalL1position, Id finalL1RandomId, InternalBranch initialBranch) {
      super();
      if(finalL1position == 0 && !deletes.isEmpty()) {
        throw new IllegalStateException("We should never have deletes if the final position is zero.");
      }
      this.l1sToSave = Preconditions.checkNotNull(l1sToSave);
      this.deletes = Preconditions.checkNotNull(deletes);
      this.finalL1 = Preconditions.checkNotNull(finalL1);
      this.finalL1position = Preconditions.checkNotNull(finalL1position);
      this.finalL1RandomId = Preconditions.checkNotNull(finalL1RandomId);
      this.initialBranch = Preconditions.checkNotNull(initialBranch);
    }

    /**
     * Ensure that all l1s to save are available. Returns once the L1s reference in this object are available for
     * reference. Before returning, will also submit a separate thread to the provided executor that will attempt to
     * clean up the existing commit log. Once the log is cleaned up, the returned CompletableFuture will return a cleaned InternalBranch.
     *
     * @param store The store to save to.
     * @param executor The executor to do any necessary clean up of the commit log.
     * @param int attempts The number of times we'll attempt to clean up the commit log.
     * @return
     */
    @SuppressWarnings("unchecked")
    CompletableFuture<InternalBranch> ensureAvailable(Store store, Executor executor, int attempts) {
      if(l1sToSave.isEmpty()) {
        saved = true;
        return CompletableFuture.completedFuture(initialBranch);
      }

      List<SaveOp<L1>> l1Saves = l1sToSave.stream().map(l1 -> new SaveOp<L1>(ValueType.L1, l1)).collect(Collectors.toList());
      store.save((List<SaveOp<?>>) (Object) l1Saves);
      saved = true;
      return CompletableFuture.supplyAsync(() -> {
        try {
          return collapseIntentionLog(this, store, initialBranch, attempts);
        } catch (ReferenceNotFoundException | ReferenceConflictException e) {
          throw new CompletionException(e);
        }
      }, executor);
    }

    /**
     * Collapses the intention log within a branch, reattempting multiple times.
     *
     * After completing an operation, we should attempt to collapse the intention log. There are two steps associated with this:
     * <ul>
     * <li>Save all the unsaved items in the intention log.
     * <li>Removing all the unsaved items in the intention log except the last one, which will be converted to an id pointer of the previous commit.
     *
     * @param branch The branch that potentially has items to collapse.
     * @param attempts Number of attempts to make before giving up on collapsing. (This is an optimistic locking scheme.)
     * @return The updated branch object after the intention log was collapsed.
     * @throws ReferenceNotFoundException
     * @throws ReferenceConflictException
     */
    private static InternalBranch collapseIntentionLog(UpdateState initialState, Store store, InternalBranch branch, int attempts) throws ReferenceNotFoundException, ReferenceConflictException {
      try {
      for (int attempt = 0; attempt < attempts; attempt++) {

        // cleanup pending updates.
        UpdateState updateState = attempts == 0 ? initialState : branch.getUpdateState();

        // now we need to take the current list and turn it into a list of 1 item that is saved.
        final ExpressionPath commits = ExpressionPath.builder("commits").build();
        final ExpressionPath last = commits.toBuilder().position(updateState.finalL1position).build();

        UpdateExpression update = UpdateExpression.initial();
        ConditionExpression condition = ConditionExpression.initial();

        for(Delete d : updateState.deletes) {
          ExpressionPath path = commits.toBuilder().position(d.position).build();
          condition = condition.and(ExpressionFunction.equals(path.toBuilder().name(ID).build(), d.id.toAttributeValue()));
          update = update.and(RemoveClause.of(path));
        }

        condition = condition.and(ExpressionFunction.equals(last.toBuilder().name(ID).build(), updateState.finalL1RandomId.toAttributeValue()));

        // remove extra commits field for last commit.
        update = update
            .and(RemoveClause.of(last.toBuilder().name(Commit.DELTAS).build()))
            .and(SetClause.equals(last.toBuilder().name(Commit.PARENT).build(), updateState.finalL1.getParentId().toAttributeValue()))
            .and(SetClause.equals(last.toBuilder().name(Commit.ID).build(), updateState.finalL1.getId().toAttributeValue()));

        Optional<InternalRef> updated = store.update(ValueType.REF, branch.getId(), update, Optional.of(condition));
        if (updated.isPresent()) {
          LOGGER.debug("Completed collapse update on attempt {}.", attempt);
          return updated.get().getBranch();
        }

        LOGGER.debug("Failed to collapse update on attempt {}.", attempt);
        // something must have changed, reload the branch.
        final InternalRef ref = store.loadSingle(ValueType.REF, branch.getId());
        if(ref.getType() != Type.BRANCH) {
          throw new ReferenceNotFoundException("Failure while collapsing log. Former branch is now a " + ref.getType());
        }
        branch = ref.getBranch();
      }

      } catch (Exception ex) {
        ex.printStackTrace();
      }
      throw new ReferenceConflictException(String.format("Unable to collapse intention log after %d attempts, giving up.", attempts));
    }

    public L1 getL1() {
      Preconditions.checkArgument(saved, "You must call UpdateState.ensureAvailable() before attempting to retrieve the L1 state of this branch.");
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


  public static class UpdateItem {
    private final boolean unsaved;
    private final int ordinal;
    private final Id id;
    private final L1 l1;

    static UpdateItem unsaved(int ordinal, Id unsavedId, L1 l1) {
      return new UpdateItem(false, unsavedId, l1, ordinal);
    }

    static UpdateItem saved(int ordinal, Id l1Id) {
      return new UpdateItem(false, l1Id, null, ordinal);
    }

    private UpdateItem(boolean unsaved, Id id, L1 l1, int ordinal) {
      super();
      this.unsaved = unsaved;
      this.id = id;
      this.l1 = l1;
      this.ordinal = ordinal;

    }

    public Id getId() {
      return id;
    }

    public L1 getL1() {
      Preconditions.checkArgument(unsaved);
      return l1;
    }

    public int getOrdinal() {
      return ordinal;
    }

    public boolean isUnsaved() {
      return unsaved;
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
      return tree.setId(position, newId);
    }

    public IdMap reverse(IdMap tree) {
      return tree.setId(position,  oldId);
    }

    static final TableSchema<UnsavedDelta> SCHEMA = new SimpleSchema<UnsavedDelta>(UnsavedDelta.class) {

      @Override
      public UnsavedDelta deserialize(Map<String, AttributeValue> map) {
        return new UnsavedDelta(
            Integer.parseInt(map.get(POSITION).n()),
            Id.of(map.get(OLD_ID).b().asByteArray()),
            Id.of(map.get(NEW_ID).b().asByteArray())
            );
      }

      @Override
      public Map<String, AttributeValue> itemToMap(UnsavedDelta item, boolean ignoreNulls) {
        return ImmutableMap.<String, AttributeValue>builder()
            .put(POSITION, AttributeValue.builder().n(Integer.toString(item.position)).build())
            .put(OLD_ID, item.oldId.toAttributeValue())
            .put(NEW_ID, item.newId.toAttributeValue())
            .build();
      }
    };
  }

  public InternalRef asRef() {
    return InternalRef.of(this);
  }

  @Override
  Id generateId() {
    return Id.build(name);
  }

  static final TableSchema<InternalBranch> SCHEMA = new SimpleSchema<InternalBranch>(InternalBranch.class) {


    @Override
    public InternalBranch deserialize(Map<String, AttributeValue> attributeMap) {
      return new InternalBranch(
          Id.fromAttributeValue(attributeMap.get(ID)),
          attributeMap.get(NAME).s(),
          IdMap.fromAttributeValue(attributeMap.get(TREE), L1.SIZE),
          Id.fromAttributeValue(attributeMap.get(METADATA)),
          attributeMap.get(COMMITS).l().stream().map(av -> Commit.SCHEMA.mapToItem(av.m())).collect(Collectors.toList())
      );
    }

    @Override
    public Map<String, AttributeValue> itemToMap(InternalBranch item, boolean ignoreNulls) {
      return ImmutableMap.<String, AttributeValue>builder()
          .put(ID, item.getId().toAttributeValue())
          .put(NAME, AttributeValue.builder().s(item.name).build())
          .put(METADATA, item.metadata.toAttributeValue())
          .put(COMMITS, AttributeValue.builder().l(item.commits.stream().map(Commit::toAttributeValue).collect(Collectors.toList())).build())
          .put(TREE, item.tree.toAttributeValue())
          .build();
    }

  };

}
