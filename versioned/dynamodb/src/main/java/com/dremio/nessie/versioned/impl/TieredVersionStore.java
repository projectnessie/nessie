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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Diff;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.ImmutableBranchName;
import com.dremio.nessie.versioned.ImmutableTagName;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import com.dremio.nessie.versioned.impl.DiffFinder.KeyDiff;
import com.dremio.nessie.versioned.impl.HistoryRetriever.HistoryItem;
import com.dremio.nessie.versioned.impl.InternalBranch.Commit;
import com.dremio.nessie.versioned.impl.InternalBranch.UpdateState;
import com.dremio.nessie.versioned.impl.InternalRef.Type;
import com.dremio.nessie.versioned.impl.PartialTree.CommitOp;
import com.dremio.nessie.versioned.impl.PartialTree.LoadType;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/**
 * A version store that uses a tree of levels to store version information.
 */
public class TieredVersionStore<DATA, METADATA> implements VersionStore<DATA, METADATA> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TieredVersionStore.class);

  private static final int MAX_MERGE_DEPTH = 200;

  private final Serializer<DATA> serializer;
  private final Serializer<METADATA> metadataSerializer;
  private final StoreWorker<DATA,METADATA> storeWorker;
  private final ExecutorService executor;
  private Store store;
  private final int commitRetryCount = 5;
  private final int p2commitRetry = 5;
  private final boolean waitOnCollapse;

  /**
   * Construct a Dynamo VersionStore.
   */
  public TieredVersionStore(StoreWorker<DATA,METADATA> storeWorker, Store store, boolean waitOnCollapse) {
    this.serializer = storeWorker.getValueSerializer();
    this.metadataSerializer = storeWorker.getMetadataSerializer();
    this.store = store;
    this.storeWorker = storeWorker;
    this.executor = Executors.newCachedThreadPool();
    this.waitOnCollapse = waitOnCollapse;
  }

  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash) throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    if (!targetHash.isPresent()) {
      if (ref instanceof TagName) {
        throw new IllegalArgumentException("You must provide a target hash to create a tag.");
      }

      InternalBranch branch = new InternalBranch(ref.getName());
      if (!store.putIfAbsent(ValueType.REF, branch)) {
        throw new ReferenceAlreadyExistsException("A branch or tag already exists with that name.");
      }

      return;
    }

    // with a hash.
    final L1 l1;
    try {
      l1 = store.loadSingle(ValueType.L1, Id.of(targetHash.get()));
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("Unable to find target hash.", ex);
    }

    InternalRef newRef = ref instanceof TagName ? new InternalTag(null, ref.getName(), l1.getId()) : new InternalBranch(ref.getName(), l1);
    if (!store.putIfAbsent(ValueType.REF, newRef)) {
      throw new ReferenceAlreadyExistsException("A branch or tag already exists with that name.");
    }
  }

  @Override
  public WithHash<Ref> toRef(String refOfUnknownType) throws ReferenceNotFoundException {
    try {
      InternalRef ref = store.loadSingle(ValueType.REF, Id.build(refOfUnknownType));
      if (ref.getType() == Type.TAG) {
        return WithHash.of(ref.getTag().getCommit().toHash(), TagName.of(ref.getTag().getName()));
      }

      Id id = ensureValidL1(ref.getBranch()).getId();
      return WithHash.of(id.toHash(), BranchName.of(ref.getBranch().getName()));
    } catch (ResourceNotFoundException ex) {
      // ignore. could be a hash.
    }

    try {
      Hash hash = Hash.of(refOfUnknownType);
      L1 l1 = store.loadSingle(ValueType.L1, Id.of(hash));
      return WithHash.of(l1.getId().toHash(), l1.getId().toHash());
    } catch (RuntimeException ex) {
      // ignore.
    }

    throw new ReferenceNotFoundException(String.format("Unable to find the provided ref %s.", refOfUnknownType));
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    InternalRefId id = InternalRefId.of(ref);

    // load ref so we can figure out how to apply condition, and do first condition check.
    final InternalRef iref;
    try {
      iref = store.loadSingle(ValueType.REF, id.getId());
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException(String.format("Unable to find '%s'.", ref.getName()), ex);
    }

    if (iref.getType() != id.getType()) {
      String t1 = iref.getType() == Type.BRANCH ? "tag" : "branch";
      String t2 = iref.getType() == Type.BRANCH ? "branch" : "tag";
      throw new ReferenceConflictException(String.format("You attempted to delete a %s using a %s invocation.", t1, t2));
    }

    ConditionExpression c = ConditionExpression.of(id.getType().typeVerification());
    if (iref.getType() == Type.TAG) {
      if (hash.isPresent()) {
        c = c.and(ExpressionFunction.equals(ExpressionPath.builder(InternalTag.COMMIT).build(), Id.of(hash.get()).toEntity()));
      }

      if (!store.delete(ValueType.REF, iref.getTag().getId(), Optional.of(c))) {
        String message = "Unable to delete tag. " + (hash.isPresent() ? "The tag does not point to the hash that was referenced."
            : "The tag was changed to a branch while the delete was occurring.");
        throw new ReferenceConflictException(message);
      }
    } else {

      // set the condition that the commit log is in a clean state, with a single saved commit and that commit is pointing
      // to the desired hash.
      if (hash.isPresent()) {
        c = c.and(ExpressionFunction.equals(
            ExpressionPath.builder(InternalBranch.COMMITS).position(0).name(Commit.ID).build(), Id.of(hash.get()).toEntity()));
        c = c.and(ExpressionFunction.equals(
            ExpressionFunction.size(ExpressionPath.builder(InternalBranch.COMMITS).build()), Entity.ofNumber("1")));
      }

      if (!store.delete(ValueType.REF,  iref.getBranch().getId(), Optional.of(c))) {
        String message = "Unable to delete branch. " + (hash.isPresent() ? "The branch does not point to the hash that was referenced."
            : "The branch was changed to a tag while the delete was occurring.");
        throw new ReferenceConflictException(message);
      }
    }

  }

  @Override
  public void commit(BranchName branchName, Optional<Hash> expectedHash, METADATA incomingCommit, List<Operation<DATA>> ops)
      throws ReferenceConflictException, ReferenceNotFoundException {
    final InternalCommitMetadata metadata = InternalCommitMetadata.of(metadataSerializer.toBytes(incomingCommit));
    final List<InternalKey> keys = ops.stream().map(op -> new InternalKey(op.getKey())).collect(Collectors.toList());
    int loop = 0;
    InternalRefId ref = InternalRefId.ofBranch(branchName.getName());
    InternalBranch updatedBranch = null;
    while (true) {

      final PartialTree<DATA> current = PartialTree.of(serializer, ref, keys);
      final PartialTree<DATA> expected = expectedHash.isPresent()
          ? PartialTree.of(serializer, InternalRefId.ofHash(expectedHash.get()), keys) : current;

      // load both trees (excluding values)
      store.load(current.getLoadChain(this::ensureValidL1, LoadType.NO_VALUES)
          .combine(expected.getLoadChain(this::ensureValidL1, LoadType.NO_VALUES)));

      List<OperationHolder> holders = ops.stream().map(o -> new OperationHolder(current, expected, o)).collect(Collectors.toList());
      List<InconsistentValue> mismatches = holders.stream()
          .map(OperationHolder::verify)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());
      if (!mismatches.isEmpty()) {
        throw new InconsistentValue.InconsistentValueException(mismatches);
      }

      // do updates.
      holders.forEach(o -> o.apply());

      // save all but l1 and branch.
      store.save(
          Streams.concat(
              current.getMostSaveOps(),
              Stream.of(new SaveOp<WrappedValueBean>(ValueType.COMMIT_METADATA, metadata))
          ).collect(Collectors.toList()));

      CommitOp commitOp = current.getCommitOp(
          metadata.getId(),
          holders.stream().filter(OperationHolder::isUnchangedOperation).map(OperationHolder::getKey).collect(Collectors.toList()),
          true,
          true);

      Optional<InternalRef> updated = store.update(ValueType.REF, ref.getId(),
          commitOp.getUpdateWithCommit(), Optional.of(commitOp.getTreeCondition()));
      if (!updated.isPresent()) {
        if (loop++ < commitRetryCount) {
          continue;
        }
        throw new ReferenceConflictException(
            String.format("Unable to complete commit due to conflicting events. Retried %d times before failing.", commitRetryCount));
      }

      updatedBranch = updated.get().getBranch();
      break;
    }

    // Now we'll try to collapse the intention log. Note that this is done post official commit so we need to return
    // successfully even if this fails.
    try {
      updatedBranch.getUpdateState(store).ensureAvailable(store, executor, p2commitRetry, waitOnCollapse);
    } catch (Exception ex) {
      LOGGER.info("Failure while collapsing intention log after commit.", ex);
    }
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    try {
      InternalRefId id = InternalRefId.of(ref);
      final L1 startingL1;
      if (id.getType() == Type.HASH) {
        // points to L1.
        startingL1 = store.loadSingle(ValueType.L1, id.getId());
      } else {
        InternalRef iref = store.loadSingle(ValueType.REF, id.getId());
        if (iref.getType() == Type.TAG) {
          startingL1 = store.loadSingle(ValueType.L1, iref.getTag().getCommit());
        } else {
          startingL1 = ensureValidL1(iref.getBranch());
        }
      }

      HistoryRetriever hr = new HistoryRetriever(store, startingL1, Id.EMPTY, false, true, false);
      return hr.getStream().map(hi -> WithHash.of(hi.getId().toHash(), metadataSerializer.fromBytes(hi.getMetadata().getBytes())));

    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("Unable to find request reference.", ex);
    }
  }


  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return store.getRefs()
        .map(ir -> {
          if (ir.getType() == Type.TAG) {
            return WithHash.<NamedRef>of(ir.getTag().getCommit().toHash(), ImmutableTagName.builder().name(ir.getTag().getName()).build());
          }

          InternalBranch branch = ir.getBranch();
          L1 l1 = ensureValidL1(branch);
          return WithHash.<NamedRef>of(l1.getId().toHash(), ImmutableBranchName.builder().name(ir.getBranch().getName()).build());
        });
  }

  /**
   * Ensures that the internal branch object has a valid saved L1 in storage.
   * @param branch The branch that may have unsaved deltas.
   * @return The L1 that is guaranteed to be addressable.
   */
  private L1 ensureValidL1(InternalBranch branch) {
    UpdateState updateState = branch.getUpdateState(store);
    updateState.ensureAvailable(store, executor, p2commitRetry, waitOnCollapse);
    return updateState.getL1();
  }

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    try {
      InternalRef iref = store.loadSingle(ValueType.REF, InternalRefId.ofUnknownName(ref.getName()).getId());
      if (iref.getType() == Type.BRANCH) {
        return ensureValidL1(iref.getBranch()).getId().toHash();
      } else {
        return iref.getTag().getCommit().toHash();
      }
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException(String.format("Unable to find ref %s", ref.getName()), ex);
    }
  }

  @Override
  public void assign(NamedRef namedRef, Optional<Hash> currentTarget, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    final String name = namedRef.getName();
    final Id refId = InternalRefId.of(namedRef).getId();
    // you can't assign to an empty branch.
    Preconditions.checkArgument(!refId.isEmpty(), "Invalid target hash.");
    final Id newId = Id.of(targetHash);

    final Id expectedId;
    if (currentTarget.isPresent()) {
      expectedId = Id.of(currentTarget.get());
    } else {
      expectedId = null;
    }

    final L1 l1;
    try {
      l1 = store.loadSingle(ValueType.L1, newId);
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("Unable to find target hash.");
    }

    final boolean isTag = namedRef instanceof TagName;
    final InternalRef.Type type = isTag ? InternalRef.Type.TAG : InternalRef.Type.BRANCH;
    final String expectedType = isTag ? "Tag" : "Branch";
    final String unexpectedType = isTag ? "Branch" : "Tag";

    ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(
            ExpressionPath.builder(InternalRef.TYPE).build(),
            type.toEntity())
        );

    final InternalRef toSave;

    if (isTag) {
      if (currentTarget.isPresent()) {
        condition = condition.and(ExpressionFunction.equals(ExpressionPath.builder(InternalTag.COMMIT).build(),
            expectedId.toEntity()));
      }
      toSave = new InternalTag(refId, namedRef.getName(), newId);
    } else {
      if (currentTarget.isPresent()) {
        condition = condition.and(
            ExpressionFunction.equals(
                ExpressionPath.builder(InternalBranch.COMMITS).position(0).name(Commit.ID).build(),
                expectedId.toEntity()));
      }
      toSave = new InternalBranch(name, l1);
    }

    try {
      store.put(ValueType.REF, toSave, Optional.of(condition));
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("The current tag", ex);
    } catch (ConditionalCheckFailedException ex) {
      if (currentTarget.isPresent()) {
        throw new ReferenceConflictException(
            String.format("Unable to assign ref %s. The reference has changed, doesn't "
                + "exist or you are trying to overwrite a %s with a %s.",
                name, unexpectedType, expectedType), ex);
      } else {
        throw new ReferenceNotFoundException(
            String.format("Unable to assign ref %s. The reference doesn't exist or you are "
                + "trying to overwrite a %s with a %s.", name, unexpectedType, expectedType), ex);
      }
    }

  }

  @Override
  public Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException {
    // naive implementation.
    InternalRefId refId = InternalRefId.of(ref);
    final L1 start;

    switch (refId.getType()) {
      case BRANCH:
        InternalRef branchRef = store.loadSingle(ValueType.REF, refId.getId());
        start = ensureValidL1(branchRef.getBranch());
        break;
      case TAG:
        InternalRef tagRef = store.loadSingle(ValueType.REF, refId.getId());
        start = store.loadSingle(ValueType.L1, tagRef.getTag().getCommit());
        break;
      case HASH:
        start = store.loadSingle(ValueType.L1, refId.getId());
        break;
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException();
    }

    return start.getKeys(store).map(InternalKey::toKey);
  }

  @Override
  public DATA getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    InternalKey ikey = new InternalKey(key);
    PartialTree<DATA> tree = PartialTree.of(serializer, InternalRefId.of(ref), Collections.singletonList(ikey));
    store.load(tree.getLoadChain(this::ensureValidL1, LoadType.SELECT_VALUES));
    return tree.getValueForKey(ikey).orElse(null);
  }

  @Override
  public List<Optional<DATA>> getValues(Ref ref, List<Key> key) throws ReferenceNotFoundException {
    List<InternalKey> keys = key.stream().map(InternalKey::new).collect(Collectors.toList());
    PartialTree<DATA> tree = PartialTree.of(serializer, InternalRefId.of(ref), keys);
    store.load(tree.getLoadChain(this::ensureValidL1, LoadType.SELECT_VALUES));
    return keys.stream().map(tree::getValueForKey).collect(Collectors.toList());
  }

  @Override
  public Collector collectGarbage() {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> currentBranchHash, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {

    Id endTarget = Id.of(sequenceToTransplant.get(0));
    internalTransplant(sequenceToTransplant.get(sequenceToTransplant.size() - 1), targetBranch, currentBranchHash,
        true,
        (from, commonParent) -> {
          // first we need to validate that the actual history matches the provided sequence.
          Stream<L1> historyStream = new HistoryRetriever(store, from, null, true, false, true).getStream().map(HistoryItem::getL1);
          List<L1> l1s = Lists.reverse(takeUntilNext(historyStream, endTarget).collect(ImmutableList.toImmutableList()));
          List<Hash> hashes = l1s.stream().map(L1::getId).map(Id::toHash).skip(1).collect(Collectors.toList());
          if (!hashes.equals(sequenceToTransplant)) {
            throw new IllegalArgumentException("Provided are not sequential and consistent with history.");
          }

          return l1s;
        });
  }

  private static Stream<L1> takeUntilNext(Stream<L1> stream, Id endTarget) {
    Spliterator<L1> iter = stream.spliterator();

    return StreamSupport.stream(new AbstractSpliterator<L1>(iter.estimateSize(), 0) {
      boolean found = false;
      boolean delivered = false;
      @Override
      public boolean tryAdvance(Consumer<? super L1> consumer) {
        boolean hasNext = iter.tryAdvance(l1 -> {
          delivered = found;
          found = l1.getId().equals(endTarget);
          consumer.accept(l1);
        });
        return !delivered && hasNext;
      }
    }, false);
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedBranchHash)
      throws ReferenceNotFoundException, ReferenceConflictException {

    internalTransplant(fromHash, toBranch, expectedBranchHash, false, (from, commonParent) -> {
      return Lists.reverse(new HistoryRetriever(store, from, commonParent, true, false, true)
          .getStream().map(HistoryItem::getL1).collect(ImmutableList.toImmutableList()));
    });
  }

  private interface HistoryHelper {
    List<L1> getFromL1s(L1 headL1, Id commonParent);
  }

  private void internalTransplant(
      Hash fromHash,
      BranchName toBranch,
      Optional<Hash> expectedBranchHash,
      boolean cherryPick,
      HistoryHelper historyHelper)
      throws ReferenceNotFoundException, ReferenceConflictException {

    final InternalRefId branchId = InternalRefId.ofBranch(toBranch.getName());
    Pointer<L1> fromPtr = new Pointer<>();
    Pointer<L1> toPtr = new Pointer<>();
    Pointer<InternalRef> branch = new Pointer<>();

    {
      // load the related assets.
      List<LoadOp<?>> loadOps = new ArrayList<>();

      // always load the l1 we're merging from.
      loadOps.add(new LoadOp<L1>(ValueType.L1, Id.of(fromHash), l -> fromPtr.set(l)));
      if (expectedBranchHash.isPresent()) {
        // if an expected branch hash is provided, use that l1 as the basic. Still load the branch to make sure it exists.
        loadOps.add(new LoadOp<L1>(ValueType.L1, Id.of(expectedBranchHash.get()), l1 -> toPtr.set(l1)));
        loadOps.add(new LoadOp<InternalRef>(ValueType.REF, branchId.getId(), r -> branch.set(r)));
      } else {

        // if no expected branch hash is provided, use the head of the branch as the basis for the rebase.
        loadOps.add(new LoadOp<InternalRef>(ValueType.REF, branchId.getId(), r -> toPtr.set(ensureValidL1(r.getBranch()))));
      }
      store.load(new LoadStep(loadOps));

      if (expectedBranchHash.isPresent() && branch.get().getType() != Type.BRANCH) {
        // since we're doing a InternalRef load, we could get a tag value back (instead of branch). Throw if this happens.
        throw new ReferenceConflictException("The requested branch is now a tag.");
      }
    }

    final L1 from = fromPtr.get();
    final L1 to = toPtr.get();

    // let's find a common parent.
    Id commonParent = HistoryRetriever.findCommonParent(store, from, to, MAX_MERGE_DEPTH);

    List<L1> fromL1s = historyHelper.getFromL1s(from, commonParent);
    if (fromL1s.size() == 1) {
      Preconditions.checkArgument(fromL1s.get(0).getId().equals(L1.EMPTY_ID));
      // the from hash is the empty hash, no operations to merge.
      return;
    }

    List<DiffFinder> fromDiffs = DiffFinder.getFinders(fromL1s, store);
    LoadStep load = fromDiffs.stream().map(DiffFinder::getLoad).collect(LoadStep.toLoadStep());

    final List<InternalKey> fromKeyChanges;

    if (!cherryPick) {

      // if the common parent has no children, simple fast-forward merge.
      if (to.getId().equals(commonParent)) {
        assign(toBranch, expectedBranchHash, fromHash);
        return;
      }

      // in the merge scenario we need to confirm that there were not changes to the master branch against the same keys
      // separately from the "from" items. This is more restrictive than a simple head to head comparison as it is possible
      // that target branch had a mutation applied and then reverted. In that situation, the merge operation will fail. This
      // more accurately represents a "rebase" operation where the new commits have to be replayed individually across the
      // new target branch as opposed to only the head of that branch.

      List<L1> toL1s =  Lists.reverse(new HistoryRetriever(store, to, commonParent, true, false, true)
          .getStream().map(HistoryItem::getL1).collect(ImmutableList.toImmutableList()));

      if (toL1s.size() == 1) {
        Preconditions.checkArgument(toL1s.get(0).getId().equals(L1.EMPTY_ID));
        // merging to an empty branch. Just do reassign.
        assign(toBranch, expectedBranchHash, fromHash);
        return;
      }

      List<DiffFinder> toDiffs = DiffFinder.getFinders(toL1s, store);

      // combine the 'from' load with the 'to' LoadSteps so we can minimize db requests.
      load = load.combine(toDiffs.stream().map(DiffFinder::getLoad).collect(LoadStep.toLoadStep()));
      store.load(load);

      // TODO: add unchanged operations.
      fromKeyChanges = fromDiffs.stream()
          .flatMap(DiffFinder::getKeyDiffs)
          .map(KeyDiff::getKey)
          .collect(ImmutableList.toImmutableList());
      Set<InternalKey> toKeyChanges = toDiffs.stream().flatMap(DiffFinder::getKeyDiffs).map(KeyDiff::getKey).collect(Collectors.toSet());

      List<InternalKey> conflictKeys = fromKeyChanges.stream().filter(toKeyChanges::contains).collect(ImmutableList.toImmutableList());

      if (!conflictKeys.isEmpty()) {
        throw new ReferenceConflictException(
            String.format(
                "The following keys have been changed in conflict: %s.",
                conflictKeys.stream().map(InternalKey::toString).collect(Collectors.joining(", "))));
      }

    } else {
      store.load(load);
      fromKeyChanges = fromDiffs.stream()
          .flatMap(DiffFinder::getKeyDiffs)
          .map(KeyDiff::getKey)
          .collect(ImmutableList.toImmutableList());
    }

    // now that we've validated the operation, we need to build up two sets of changes. One is the composite changes
    // which will be applied to the Branch IdMap. The second is distinct operations that will be added to the the commit
    // intention log of the Branch object.
    PartialTree<DATA> headToRebaseOn = PartialTree.of(serializer, InternalRef.Type.BRANCH, to, fromKeyChanges);
    List<DiffManager> creators = fromDiffs.stream().map(DiffManager::new).collect(Collectors.toList());
    store.load(creators.stream().map(DiffManager::getLoad)
        .collect(LoadStep.toLoadStep())
        .combine(headToRebaseOn.getLoadChain(this::ensureValidL1, LoadType.NO_VALUES)));

    // Now that we have all the items loaded, let's apply the changeset to both the sequential DiffManagers and the composite PartialTree.
    creators.forEach(pt -> pt.apply(headToRebaseOn));

    // Save L2s and L3s. Note we don't need to do any value saves here as we know that the values are already stored.
    store.save(
        Stream.concat(
            creators.stream().flatMap(c -> c.tree.getMostSaveOps()),
            headToRebaseOn.getMostSaveOps())
        .distinct()
        .collect(Collectors.toList()));

    // get a list of all the intentions as a SetClause
    List<Commit> intentions = creators.stream().map(pt -> pt.getCommit()).collect(Collectors.toList());
    SetClause commitUpdate = CommitOp.getCommitSet(intentions);

    // Get the composite commit operation, but exclude any Commit intentions.
    CommitOp headCommit = headToRebaseOn.getCommitOp(to.getMetadataId(), Collections.emptyList(), true, false);

    // Do a conditional update that combines the commit intentions with the composite tree updates,
    // based on the composite tree conditions.
    Optional<InternalRef> updated = store.update(ValueType.REF, branchId.getId(),
        headCommit.getTreeUpdate().and(commitUpdate),
        Optional.of(headCommit.getTreeCondition())
        );

    if (!updated.isPresent()) {
      throw new ReferenceConflictException("Unable to complete commit.");
    }
  }

  /**
   * Class used to manage the tree mutations required to move between two L1s.
   */
  private class DiffManager {
    private PartialTree<DATA> tree;
    private Id metadataId;
    private DiffFinder finder;

    DiffManager(DiffFinder finder) {
      this.finder = finder;
      metadataId = finder.getTo().getMetadataId();
      tree = PartialTree.of(serializer, InternalRef.Type.BRANCH, finder.getFrom(),
          finder.getKeyDiffs().map(KeyDiff::getKey).collect(Collectors.toList()));
    }

    /**
     * Generate the commit intention operation associated with this set of diffs.
     *
     * @return The Commit Intention record.
     */
    public Commit getCommit() {
      return tree.getCommitOp(metadataId, Collections.emptyList(), false, true).getCommitIntention();
    }

    public LoadStep getLoad() {
      return tree.getLoadChain(TieredVersionStore.this::ensureValidL1, LoadType.NO_VALUES);
    }

    /**
     * Apply the diff associated with the set of ops this tree is managing.
     * @param secondTree The compound tree that will receive all diffs.
     */
    public void apply(PartialTree<DATA> secondTree) {
      finder.getKeyDiffs().forEach(kd -> {
        Optional<Id> valueToSet = Optional.ofNullable(kd.getTo()).filter(i -> !i.isEmpty());
        tree.setValueIdForKey(kd.getKey(), valueToSet);
        secondTree.setValueIdForKey(kd.getKey(), valueToSet);
      });
    }


  }

  @Override
  public Stream<Diff<DATA>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    PartialTree<DATA> fromTree = PartialTree.of(serializer, InternalRefId.of(from), Collections.emptyList());
    PartialTree<DATA> toTree = PartialTree.of(serializer, InternalRefId.of(to), Collections.emptyList());
    store.load(fromTree.getLoadChain(this::ensureValidL1, LoadType.NO_VALUES)
        .combine(toTree.getLoadChain(this::ensureValidL1, LoadType.NO_VALUES)));

    DiffFinder finder = new DiffFinder(fromTree.getCurrentL1(), toTree.getCurrentL1());
    store.load(finder.getLoad());

    // For now, we'll load all the values at once. In the future, we should paginate diffs.
    Map<Id, InternalValue> values = new HashMap<>();
    List<LoadOp<InternalValue>> loads = finder.getKeyDiffs()
        .flatMap(k -> Stream.of(k.getFrom(), k.getTo()))
        .distinct()
        .filter(id -> !id.isEmpty())
        .map(id -> new LoadOp<InternalValue>(ValueType.VALUE, id, val -> values.put(id, val)))
        .collect(Collectors.toList());
    store.load(LoadStep.of(loads.toArray(new LoadOp[loads.size()])));

    return finder.getKeyDiffs().map(kd -> Diff.of(
        kd.getKey().toKey(),
        Optional.ofNullable(kd.getFrom()).map(id -> values.get(id)).map(v -> serializer.fromBytes(v.getBytes())),
        Optional.ofNullable(kd.getTo()).map(id -> values.get(id)).map(v -> serializer.fromBytes(v.getBytes()))
      )
    );
  }

  class OperationHolder {
    private final PartialTree<DATA> current;
    private final PartialTree<DATA> expected;
    private final Operation<DATA> operation;
    private final InternalKey key;

    public OperationHolder(PartialTree<DATA> current, PartialTree<DATA> expected, Operation<DATA> operation) {
      this.current = Preconditions.checkNotNull(current);
      this.expected = Preconditions.checkNotNull(expected);
      this.operation = Preconditions.checkNotNull(operation);
      this.key = new InternalKey(operation.getKey());
    }

    public Optional<InconsistentValue> verify() {
      if (!operation.shouldMatchHash()) {
        return Optional.empty();
      }

      Optional<Id> currentValueId = current.getValueIdForKey(key);
      Optional<Id> expectedValueId = expected.getValueIdForKey(key);
      if (!currentValueId.equals(expectedValueId)) {
        return Optional.of(new InconsistentValue(operation.getKey(), expectedValueId, currentValueId));
      }

      return Optional.empty();
    }

    public InternalKey getKey() {
      return key;
    }

    public void apply() {
      if (operation instanceof Put) {
        current.setValueForKey(key, Optional.of(((Put<DATA>) operation).getValue()));
      } else if (operation instanceof Delete) {
        current.setValueForKey(key, Optional.empty());
      } else if (operation instanceof Unchanged) {
        // no mutations required as the check was done on the current.
      } else {
        throw new IllegalStateException("Unknown operation type.");
      }
    }

    public boolean isUnchangedOperation() {
      return operation instanceof Unchanged;
    }
  }

}
