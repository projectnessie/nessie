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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.ImmutableBranchName;
import com.dremio.nessie.versioned.ImmutableTagName;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import com.dremio.nessie.versioned.impl.InternalBranch.Commit;
import com.dremio.nessie.versioned.impl.InternalBranch.UnsavedDelta;
import com.dremio.nessie.versioned.impl.InternalBranch.UpdateState;
import com.dremio.nessie.versioned.impl.InternalRef.Type;
import com.dremio.nessie.versioned.impl.DynamoStore.ValueType;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

public class DynamoVersionStore<DATA, METADATA> implements VersionStore<DATA, METADATA> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoVersionStore.class);

  private final Serializer<DATA> serializer;
  private final Serializer<METADATA> metadataSerializer;
  private final StoreWorker<DATA,METADATA> storeWorker;
  private final ExecutorService executor;
  private DynamoStore store;
  private final int commitRetryCount = 5;
  private final int p2commitRetry = 5;

  public DynamoVersionStore(StoreWorker<DATA,METADATA> storeWorker, DynamoStore store) {
    this.serializer = storeWorker.getValueSerializer();
    this.metadataSerializer = storeWorker.getMetadataSerializer();
    this.store = store;
    this.storeWorker = storeWorker;
    this.executor = Executors.newCachedThreadPool();
  }

  @Override
  public void commit(BranchName branchName, Optional<Hash> expectedHash, METADATA incomingCommit, List<Operation<DATA>> ops) throws ReferenceConflictException, ReferenceNotFoundException {
    final InternalCommitMetadata metadata = InternalCommitMetadata.of(metadataSerializer.toBytes(incomingCommit));
    final List<InternalKey> keys = ops.stream().map(op -> new InternalKey(op.getKey())).collect(Collectors.toList());
    int loop = 0;
    InternalRefId ref = InternalRefId.ofBranch(branchName.getName());
    InternalBranch updatedBranch = null;
    while (true) {

      final PartialTree<DATA> current = PartialTree.of(serializer, ref, keys);
      final PartialTree<DATA> expected = expectedHash.isPresent() ? PartialTree.of(serializer, InternalRefId.ofHash(expectedHash.get()), keys) : current;

      // load both trees (excluding values)
      store.load(current.getLoadChain(this::ensureValidL1, false).combine(expected.getLoadChain(this::ensureValidL1, false)));

      List<OperationHolder> holders = ops.stream().map(o -> new OperationHolder(current, expected, o)).collect(Collectors.toList());
      List<InconsistentValue> mismatches = holders.stream().map(o -> o.verify()).filter(Optional::isPresent).map(o -> o.get()).collect(Collectors.toList());
      if (!mismatches.isEmpty()) {
        throw new InconsistentValue.InconsistentValueException(mismatches);
      }

      // do updates.
      holders.forEach(o -> o.apply());

      // save all but l1 and branch.
      store.save(
          Streams.concat(
              current.getMostSaveOps().stream(),
              Collections.singleton(new SaveOp<WrappedValueBean>(ValueType.COMMIT_METADATA, metadata)).stream()
          ).collect(Collectors.toList()));

      // now do the branch conditional update.
      final List<PositionMutation> mutations = current.getL1Mutations();

      // positions that have a condition
      final Set<Integer> conditionPositions = new HashSet<>();

      List<UnsavedDelta> deltas = new ArrayList<>();

      UpdateExpression update = UpdateExpression.initial();
      ConditionExpression condition = ConditionExpression.of(ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), InternalRef.Type.BRANCH.toAttributeValue()));

      for(PositionMutation pm : mutations) {
        if(!pm.isDirty()) {
          continue;
        }

        boolean added = conditionPositions.add(pm.getPosition());
        assert added;
        ExpressionPath p = ExpressionPath.builder(InternalBranch.TREE).position(pm.getPosition()).build();
        update = update.and(SetClause.equals(p, pm.getNewId().toAttributeValue()));
        condition = condition.and(ExpressionFunction.equals(p, pm.getOldId().toAttributeValue()));
        deltas.add(pm.toUnsavedDelta());
      }

      for(OperationHolder oh : (Iterable<OperationHolder>) holders.stream().filter(OperationHolder::isUnchangedOperation)::iterator) {
        int position = oh.key.getL1Position();
        if (conditionPositions.add(position)) {
          // this doesn't already have a condition. Add one.
          ExpressionPath p = ExpressionPath.builder(InternalBranch.TREE).position(position).build();
          condition = condition.and(ExpressionFunction.equals(p, current.getCurrentL1().getId(position).toAttributeValue()));
        }
      }

      // Add the new commit
      Commit commitIntention = new Commit(Id.generateRandom(), metadata.getId(), deltas);
      update = update.and(SetClause.appendToList(ExpressionPath.builder(InternalBranch.COMMITS).build(), AttributeValue.builder().l(commitIntention.toAttributeValue()).build()));

      Optional<InternalRef> updated = store.update(ValueType.REF, ref.getId(), update, Optional.of(condition));
      if(!updated.isPresent()) {
        if(loop++ < commitRetryCount) {
          continue;
        }
        throw new ReferenceConflictException(String.format("Unable to complete commit due to conflicting events. Retried %d times before failing.", commitRetryCount));
      }

      updatedBranch = updated.get().getBranch();
      break;
    }

    // Now we'll try to collapse the intention log. Note that this is done post official commit so we need to return successfully even if this fails.
    try {
      updatedBranch.getUpdateState().ensureAvailable(store, executor, p2commitRetry);
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
        if(iref.getType() == Type.TAG) {
          startingL1 = store.loadSingle(ValueType.L1, iref.getTag().getCommit());
        } else {
          startingL1 = ensureValidL1(iref.getBranch());
        }
      }

      final Iterator<WithHash<METADATA>> iterator = new Iterator<WithHash<METADATA>>() {

        private L1 currentL1;

        @Override
        public boolean hasNext() {
          if(currentL1 == null) {
            return true;
          }
          return !currentL1.getParentId().equals(L1.EMPTY_ID);
        }

        @Override
        public WithHash<METADATA> next() {
          if(currentL1 == null) {
            currentL1 = startingL1;
          } else {
            currentL1 = store.loadSingle(ValueType.L1, currentL1.getParentId());
          }

          WrappedValueBean bean = store.loadSingle(ValueType.COMMIT_METADATA, currentL1.getMetadataId());
          return WithHash.of(currentL1.getId().toHash(), metadataSerializer.fromBytes(bean.getBytes()));
        }};
      return StreamSupport.stream(Spliterators.spliterator(iterator, 10, 0), false);
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("Unable to find request reference.", ex);
    }
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    InternalRefId id = InternalRefId.of(ref);

    // load ref so we can figure out how to apply condition, and do first condition check.
    final InternalRef iref;
    try {
      iref = store.loadSingle(ValueType.REF, id.getId());
    } catch(ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException(String.format("Unable to find '%s'.", ref.getName()), ex);
    }
    ConditionExpression c = ConditionExpression.of(iref.getType().typeVerification());
    if (iref.getType() == Type.TAG) {
      if (hash.isPresent()) {
        c = c.and(ExpressionFunction.equals(ExpressionPath.builder(InternalTag.COMMIT).build(), Id.of(hash.get()).toAttributeValue()));
      }

      if (!store.delete(ValueType.REF, iref.getTag().getId(), Optional.of(c))) {
        throw new ReferenceConflictException("Unable to complete tag. " + (hash.isPresent() ? "The tag does not point to the hash that was referenced." : "The tag was changed to a branch while the delete was occurring."));
      }
    } else {
      if (hash.isPresent()) {
        c = c.and(ExpressionFunction.equals(ExpressionPath.builder(InternalBranch.COMMITS).position(0).name(Commit.ID).build(), Id.of(hash.get()).toAttributeValue()));
        c = c.and(ExpressionFunction.equals(ExpressionFunction.size(ExpressionPath.builder(InternalBranch.COMMITS).build()), AttributeValue.builder().n("1").build()));
      }

      if (!store.delete(ValueType.REF,  iref.getBranch().getId(), Optional.of(c))) {
        throw new ReferenceConflictException("Unable to complete tag. " + (hash.isPresent() ? "The branch does not point to the hash that was referenced." : "The branch was changed to a tag while the delete was occurring."));
      }
    }

  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return store.getRefs()
        .map(ir -> {
          if(ir.getType() == Type.TAG) {
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
    UpdateState updateState = branch.getUpdateState();
    updateState.ensureAvailable(store, executor, p2commitRetry);
    return updateState.getL1();
  }

  @Override
  public DATA getValue(Ref ref, Key key) {
    InternalKey ikey = new InternalKey(key);
    PartialTree<DATA> tree = PartialTree.of(serializer, InternalRefId.of(ref), Collections.singletonList(ikey));
    store.load(tree.getLoadChain(this::ensureValidL1, true));
    return tree.getValueForKey(ikey).orElse(null);
  }

  private class OperationHolder {
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
      if(!operation.shouldMatchHash()) {
        return Optional.empty();
      }

      Optional<Id> currentValueId = current.getValueIdForKey(key);
      Optional<Id> expectedValueId = expected.getValueIdForKey(key);
      if(!currentValueId.equals(expectedValueId)) {
        return Optional.of(new InconsistentValue(operation.getKey(), expectedValueId, currentValueId));
      }

      return Optional.empty();
    }

    public void apply() {
      if(operation instanceof Put) {
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

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    try {
      InternalRef iref = store.loadSingle(ValueType.REF, InternalRefId.ofUnknownName(ref.getName()).getId());
      if(iref.getType() == Type.BRANCH) {
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
    final Id newId = Id.of(targetHash.asBytes());
    final Id expectedId = currentTarget.map(Id::of).orElse(null);

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
            AttributeValue.builder().s(type.identifier()).build())
        );

    final InternalRef toSave;

    if(isTag) {
      if(currentTarget.isPresent()) {
        condition = condition.and(ExpressionFunction.equals(ExpressionPath.builder(InternalTag.COMMIT).build(), expectedId.toAttributeValue()));
      }
      toSave = new InternalTag(refId, namedRef.getName(), newId).asRef();
    } else {
      if(currentTarget.isPresent()) {
        condition = condition.and(ExpressionFunction.equals(ExpressionPath.builder(InternalBranch.COMMITS).position(0).name(Commit.ID).build(), expectedId.toAttributeValue()));
      }
      toSave = new InternalBranch(name, l1).asRef();
    }

    try {
      store.put(ValueType.REF, toSave, Optional.of(condition));
    } catch(ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("The current tag", ex);
    } catch(ConditionalCheckFailedException ex) {
      if(currentTarget.isPresent()) {
        throw new ReferenceNotFoundException(String.format("Unable to assign ref %s. Either the reference has changed or you are trying to overwrite a %s with a %s.", name, unexpectedType, expectedType), ex);
      } else {
        throw new ReferenceNotFoundException(String.format("Unable to assign ref %s. You cannot overwrite a %s with a %s.", name, unexpectedType, expectedType), ex);
      }
    }

  }


  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash) throws ReferenceNotFoundException, ReferenceConflictException {
    if (!targetHash.isPresent()) {
      if (ref instanceof TagName) {
        InternalTag tag = new InternalTag(null, ref.getName(), L1.EMPTY_ID);
        if(!store.putIfAbsent(ValueType.REF, InternalRef.of(tag))) {
          throw new ReferenceConflictException("A branch or tag already exists with that name.");
        }
      } else {
        InternalBranch branch = new InternalBranch(ref.getName());
        if(!store.putIfAbsent(ValueType.REF, InternalRef.of(branch))) {
          throw new ReferenceConflictException("A branch or tag already exists with that name.");
        }
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

    if (ref instanceof TagName) {
      InternalTag tag = new InternalTag(null, ref.getName(), l1.getId());
      if(!store.putIfAbsent(ValueType.REF, InternalRef.of(tag))) {
        throw new ReferenceConflictException("A branch or tag already exists with that name.");
      }
    } else {
      InternalBranch branch = new InternalBranch(ref.getName(), l1);
      if(!store.putIfAbsent(ValueType.REF, InternalRef.of(branch))) {
        throw new ReferenceConflictException("A branch or tag already exists with that name.");
      }
    }
  }

  @Override
  public Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public List<Optional<DATA>> getValue(Ref ref, List<Key> key) throws ReferenceNotFoundException {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public Collector collectGarbage() {
    throw new IllegalStateException("Not yet implemented.");
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> currentBranchHash, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedBranchHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }



}
