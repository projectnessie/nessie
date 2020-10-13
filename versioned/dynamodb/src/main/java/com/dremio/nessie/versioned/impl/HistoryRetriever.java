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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.DynamoStore.ValueType;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

/**
 * Enables retrieval of L1 history.
 */
class HistoryRetriever {

  private final boolean retrieveL1;
  private final boolean retrieveCommit;
  private final boolean includeEndEmpty;
  private final DynamoStore store;
  private final L1 start;
  private final Id end;

  public HistoryRetriever(DynamoStore store, L1 start, Id end, boolean retrieveL1, boolean retrieveCommit, boolean includeEndEmpty) {
    super();
    this.store = store;
    this.start = start;
    this.end = end;
    this.retrieveL1 = retrieveL1;
    this.retrieveCommit = retrieveCommit;
    this.includeEndEmpty = includeEndEmpty;
  }

  class HistoryItem {

    private Id id;
    private L1 l1;
    private InternalCommitMetadata commitMetadata;

    public HistoryItem(Id id) {
      this.id = id;
    }

    public L1 getL1() {
      return l1;
    }

    public Id getId() {
      return id;
    }

    public InternalCommitMetadata getMetadata() {
      return commitMetadata;
    }

  }

  Stream<HistoryItem> getStream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new HistoryIterator(), 0), false);
  }

  private class HistoryIterator extends AbstractIterator<HistoryItem> {

    private Iterator<HistoryItem> currentIterator;
    private boolean isLast = false;
    private HistoryItem previous;

    public HistoryIterator() {
      this.previous = null;

      if (start.getId().equals(L1.EMPTY_ID) && !includeEndEmpty) {
        this.currentIterator = Collections.emptyIterator();
        this.isLast = true;
      } else {
        HistoryItem item = new HistoryItem(start.getId());
        item.l1 = start;
        if (retrieveCommit && !start.getMetadataId().isEmpty()) {
          item.commitMetadata = store.loadSingle(ValueType.COMMIT_METADATA, start.getMetadataId());
        }
        this.currentIterator = Collections.singleton(item).iterator();
      }
    }

    @Override
    protected HistoryItem computeNext() {
      while (!currentIterator.hasNext() && !isLast) {
        try {
          calculateNextList(previous.getL1().getParentList());
        } catch (ReferenceNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      if (!currentIterator.hasNext()) {
        endOfData();
        return null;
      }

      previous = currentIterator.next();
      return previous;
    }

    private void calculateNextList(ParentList list) throws ReferenceNotFoundException {
      int max = list.getParents().size();
      final List<HistoryItem> items = new ArrayList<>();
      final List<LoadOp<?>> loadOps = new ArrayList<>();
      final List<Supplier<LoadOp<?>>> secondOps = new ArrayList<>();
      final List<Id> ids = list.getParents();
      for (int i = 0; i < max; i++) {
        final boolean lastInList = i == max - 1;
        Id parent = ids.get(i);

        if (parent.isEmpty()) {
          break;
        }

        if (!includeEndEmpty && parent.equals(L1.EMPTY_ID)) {
          isLast = true;
          break;
        }

        final HistoryItem item = new HistoryItem(parent);
        items.add(item);
        if (retrieveL1 || retrieveCommit || lastInList) {
          loadOps.add(new LoadOp<L1>(ValueType.L1, parent, l1 -> item.l1 = l1));
        }

        if (retrieveCommit && !parent.equals(L1.EMPTY_ID)) {
          secondOps.add(() -> new LoadOp<InternalCommitMetadata>(
              ValueType.COMMIT_METADATA, item.l1.getMetadataId(), cmd -> item.commitMetadata = cmd));
        }

        if (parent.equals(end)) {
          isLast = true;
          break;
        }
      }

      if (loadOps.isEmpty()) {
        currentIterator = Collections.emptyIterator();
        isLast = true;
        return;
      }

      store.load(new LoadStep(loadOps, () -> {
        if (secondOps.isEmpty()) {
          return Optional.empty();
        }

        return Optional.of(new LoadStep(secondOps.stream().map(Supplier::get).collect(ImmutableList.toImmutableList())));
      }));
      currentIterator = items.iterator();
    }

  }

}
