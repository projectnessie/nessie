/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import static org.projectnessie.versioned.storage.common.logic.HeadsAndForkPoints.headsAndForkPoints;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import java.util.Set;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.collections.ObjectHashSet;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public class IdentifyHeadsAndForkPoints {
  // Map contains both the commit-IDs and parent-
  private final Object2IntHashMap<ObjId> commits;
  private final Set<ObjId> heads;
  private final Set<ObjId> forkPoints;
  private final long scanStartedAtInMicros;

  private static <T> Set<T> newOpenAddressingHashSet() {
    return new ObjectHashSet<>(
        ObjectHashSet.DEFAULT_INITIAL_CAPACITY, Hashing.DEFAULT_LOAD_FACTOR, false);
  }

  private static final int MASK_COMMIT_SEEN = 1;
  private static final int MASK_PARENT_SEEN = 2;

  public IdentifyHeadsAndForkPoints(int expectedCommitCount, long scanStartedAtInMicros) {
    // Using open-addressing implementation here, because it's much more space-efficient than
    // java.util.HashSet.
    this.commits =
        new Object2IntHashMap<>(expectedCommitCount * 2, Hashing.DEFAULT_LOAD_FACTOR, 0, true);
    this.heads = newOpenAddressingHashSet();
    this.forkPoints = newOpenAddressingHashSet();
    this.scanStartedAtInMicros = scanStartedAtInMicros;
  }

  public boolean handleCommit(CommitObj entry) {
    return handleCommit(entry.id(), entry.directParent());
  }

  public boolean isCommitNew(ObjId commitId) {
    int cv = commits.getValue(commitId);
    return (cv & MASK_COMMIT_SEEN) == 0;
  }

  public boolean handleCommit(ObjId commitId, ObjId parent) {

    int cv = commits.getValue(commitId);
    boolean commitNew = (cv & MASK_COMMIT_SEEN) == 0;
    if (commitNew) {
      commits.put(commitId, cv | MASK_COMMIT_SEEN);
    }
    boolean commitNotSeenAsParent = (cv & MASK_PARENT_SEEN) == 0;
    if (commitNotSeenAsParent) {
      // If the commit-ID has not been seen as a parent, it must be a HEAD
      heads.add(commitId);
    }

    // Only process the parent-ID when the commit has not been seen before.
    if (!commitNew) {
      return false;
    }

    // Do not handle 'no ancestor' as a "legit parent".
    if (EMPTY_OBJ_ID.equals(parent)) {
      return true;
    }

    int pv = commits.getValue(parent);
    boolean parentNew = (pv & MASK_PARENT_SEEN) == 0;

    if (!parentNew) {
      // If "parent" has already been seen, then it must be a fork point.
      forkPoints.add(parent);
    } else {
      commits.put(parent, pv | MASK_PARENT_SEEN);
      // Commits in "parents" that are also contained in "heads" cannot be HEADs.
      // This can happen because the commits are scanned in "random order".
      heads.remove(parent);
    }

    return true;
  }

  public HeadsAndForkPoints finish() {
    return headsAndForkPoints(heads, forkPoints, scanStartedAtInMicros);
  }
}
