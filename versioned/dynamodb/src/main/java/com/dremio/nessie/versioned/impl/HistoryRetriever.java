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
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.DynamoStore.ValueType;
import com.google.common.collect.AbstractIterator;

/**
 * Enables retrieval of L1 history.
 */
class HistoryRetriever {

  private final DynamoStore store;
  private final L1 start;
  private final Id end;

  public HistoryRetriever(DynamoStore store, L1 start, Id end) {
    super();
    this.store = store;
    this.start = start;
    this.end = end;
  }

  class HistoryItem {

    private final L1 l1;

    public HistoryItem(L1 l1) {
      this.l1 = l1;
    }

    public L1 getL1() {
      return l1;
    }

  }

  Stream<HistoryItem> getStream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new HistoryIterator(), 0), false);
  }

  private class HistoryIterator extends AbstractIterator<HistoryItem> {

    private Iterator<L1> currentIterator;
    private boolean isLast = false;
    private L1 previous;

    public HistoryIterator() {
      this.previous = null;
      this.currentIterator = Collections.singleton(start).iterator();
    }

    @Override
    protected HistoryItem computeNext() {
      while (!currentIterator.hasNext() && !isLast) {
        try {
          calculateNextList(previous.getParentList());
        } catch (ReferenceNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      if (!currentIterator.hasNext()) {
        endOfData();
        return null;
      }

      previous = currentIterator.next();
      return new HistoryItem(previous);
    }

    private void calculateNextList(ParentList list) throws ReferenceNotFoundException {
      int max = list.getParents().size();
      final L1[] l1s = new L1[max];
      List<LoadOp<?>> loadOps = new ArrayList<>();

      for (int i = 0; i < max; i++) {
        Id parent = list.getParents().get(i);
        if (parent.isEmpty()) {
          break;
        }
        final int pos = i;
        loadOps.add(new LoadOp<L1>(ValueType.L1, parent, l1 -> l1s[pos] = l1));

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

      store.load(new LoadStep(loadOps));
      List<L1> l1List = Stream.of(l1s).filter(l -> l != null).collect(Collectors.toList());
      currentIterator = l1List.iterator();
    }

  }

}
