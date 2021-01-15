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
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadStep;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference.ValueDifference;

/**
 * Given two L1s, determine the value differences between them.
 */
class DiffFinder {

  private final List<L3Diff> l3Diffs = new ArrayList<>();
  private final L1Diff l1Diff;

  public DiffFinder(L1 first, L1 second) {
    this.l1Diff = new L1Diff(first, second);
  }

  public LoadStep getLoad() {
    return l1Diff.getLoad(l3Diffs);
  }

  public L1 getFrom() {
    return l1Diff.from;
  }

  public L1 getTo() {
    return l1Diff.to;
  }

  public Stream<KeyDiff> getKeyDiffs() {
    return l3Diffs.stream().flatMap(L3Diff::getKeyDiffs);
  }

  private static class L1Diff {
    private final L1 from;
    private final L1 to;

    public L1Diff(L1 from, L1 to) {
      super();
      this.from = from;
      this.to = to;
    }

    public LoadStep getLoad(List<L3Diff> l3DiffsOutput) {
      List<L2Diff> l2Diffs = new ArrayList<>();
      EntityLoadOps loadOps = new EntityLoadOps();
      for (int i = 0; i < L1.SIZE; i++) {
        Id a = from.getId(i);
        Id b = to.getId(i);
        if (!a.equals(b)) {
          L2Diff d = new L2Diff();
          l2Diffs.add(d);
          loadOps.load(EntityType.L2, a, d::from);
          loadOps.load(EntityType.L2, b, d::to);
        }
      }

      return loadOps.build(() -> L2Diff.loadStep(l2Diffs, l3DiffsOutput));
    }

  }

  private static class L2Diff {
    private L2 from;
    private L2 to;

    void from(L2 from) {
      this.from = from;
    }

    void to(L2 to) {
      this.to = to;
    }

    public static Optional<LoadStep> loadStep(Collection<L2Diff> diffs, List<L3Diff> l3DiffsOutput) {
      EntityLoadOps loadOps = new EntityLoadOps();
      for (L2Diff diff : diffs) {
        L2 from = diff.from;
        L2 to = diff.to;
        for (int i = 0; i < L2.SIZE; i++) {
          Id a = from.getId(i);
          Id b = to.getId(i);
          if (!a.equals(b)) {
            L3Diff d = new L3Diff();
            l3DiffsOutput.add(d);
            loadOps.load(EntityType.L3, a, d::from);
            loadOps.load(EntityType.L3, b, d::to);
          }
        }
      }
      return loadOps.buildOptional();
    }
  }

  private static class L3Diff {
    private L3 from;
    private L3 to;

    void from(L3 from) {
      this.from = from;
    }

    void to(L3 to) {
      this.to = to;
    }

    Stream<KeyDiff> getKeyDiffs() {
      return L3.compare(from, to);
    }

  }

  /**
   * Describes the state of mutated key between two versions.
   */
  static class KeyDiff {

    private final InternalKey key;
    private final Id from;
    private final Id to;

    static KeyDiff onlyOnLeft(Entry<InternalKey, Id> left) {
      return new KeyDiff(left.getKey(), left.getValue(), Id.EMPTY);
    }

    static KeyDiff onlyOnRight(Entry<InternalKey, Id> right) {
      return new KeyDiff(right.getKey(), Id.EMPTY, right.getValue());
    }

    private KeyDiff(InternalKey key, Id from, Id to) {
      super();
      this.key = key;
      this.from = from;
      this.to = to;
    }

    KeyDiff(Entry<InternalKey, ValueDifference<Id>> diff) {
      this.key = diff.getKey();
      ValueDifference<Id> id = diff.getValue();
      from = id.leftValue();
      to = id.rightValue();
    }

    /**
     * The key that this diff applies to.
     * @return The key
     */
    public InternalKey getKey() {
      return key;
    }

    /**
     * The initial value of this Key.
     * @return The Id. Will be Id.EMPTY if the key was added as part of this diff.
     */
    public Id getFrom() {
      return from;
    }

    /**
     * The final value of this Key.
     * @return The Id. Will be Id.EMPTY if the key was removed as part of this diff.
     */
    public Id getTo() {
      return to;
    }

  }

  static List<DiffFinder> getFinders(List<L1> l1Ascending) {
    Preconditions.checkArgument(l1Ascending.size() > 1);
    L1 previous = null;
    List<DiffFinder> diffs = new ArrayList<>();
    for (L1 l1 : l1Ascending) {
      if (previous != null) {
        DiffFinder finder = new DiffFinder(previous, l1);
        diffs.add(finder);
      }
      previous = l1;
    }
    return diffs;
  }
}
