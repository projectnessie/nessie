/*
 * Copyright (C) 2023 Dremio
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

import java.util.Arrays;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * For {@link MergeBase}, holds only some properties from {@link CommitObj} to reduce heap pressure.
 *
 * <p>Identifying the merge-base may require holding a lot of commits, so keeping only the needed
 * data should help reducing the heap pressure (think: {@link CommitObj#incrementalIndex()}.
 */
final class ShallowCommit {
  /** Whether a commit is reachable via 'commit A'. */
  static final int COMMIT_A = 1;

  /** Whether a commit is reachable via 'commit B'. */
  static final int COMMIT_B = 2;

  static final int CANDIDATE = 4;
  static final int RESULT = 8;

  static final int BOTH_COMMITS = COMMIT_A | COMMIT_B;
  static final int REACHABILITY_FLAGS = BOTH_COMMITS | CANDIDATE;
  static final int ALL_FLAGS = COMMIT_A | COMMIT_B | CANDIDATE | RESULT;

  private final ObjId id;
  private final ObjId[] parents;
  private final long seq;
  private int flags;

  ShallowCommit(ObjId id, ObjId[] parents, long seq) {
    this.id = id;
    this.parents = parents;
    this.seq = seq;
  }

  ObjId id() {
    return id;
  }

  ObjId[] parents() {
    return parents;
  }

  long seq() {
    return seq;
  }

  int flags() {
    return flags;
  }

  int reachabilityFlags() {
    return flags & REACHABILITY_FLAGS;
  }

  boolean isCandidate() {
    return (flags & CANDIDATE) != 0;
  }

  boolean isNotCandidate() {
    return (flags & CANDIDATE) == 0;
  }

  boolean isAnyFlagSet(int flags) {
    return (this.flags & flags) != 0;
  }

  void setCommitA() {
    this.flags |= COMMIT_A;
  }

  void setCommitB() {
    this.flags |= COMMIT_B;
  }

  void clearCandidate() {
    this.flags &= ~CANDIDATE;
  }

  boolean clearFlagsIfAnySet(int flags) {
    int ex = this.flags;
    if ((ex & flags) != 0) {
      this.flags = ex & ~flags;
      return true;
    }
    return false;
  }

  boolean setAllFlagsIfAnyMissing(int flags) {
    int ex = this.flags;
    if ((ex & flags) != flags) {
      this.flags = ex | flags;
      return true;
    }
    return false;
  }

  boolean setCandidate() {
    int ex = this.flags;
    if ((ex & CANDIDATE) == 0) {
      this.flags = ex | CANDIDATE;
      return true;
    }
    return false;
  }

  boolean setResult() {
    int ex = this.flags;
    if ((ex & RESULT) == 0) {
      this.flags = ex | RESULT;
      return true;
    }
    return false;
  }

  boolean clearResult() {
    int ex = this.flags;
    if ((ex & RESULT) != 0) {
      this.flags = ex & ~RESULT;
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder f = new StringBuilder();
    if ((flags & BOTH_COMMITS) == BOTH_COMMITS) {
      f.append("BOTH_COMMITS");
    } else {
      if ((flags & COMMIT_A) != 0) {
        f.append("COMMIT_A");
      }
      if ((flags & COMMIT_B) != 0) {
        if (f.length() > 0) {
          f.append(", ");
        }
        f.append("COMMIT_B");
      }
    }
    if ((flags & CANDIDATE) != 0) {
      if (f.length() > 0) {
        f.append(", ");
      }
      f.append("CANDIDATE");
    }
    if ((flags & RESULT) != 0) {
      if (f.length() > 0) {
        f.append(", ");
      }
      f.append("RESULT");
    }
    return "ShallowCommit{"
        + "id="
        + id
        + ", parents="
        + Arrays.toString(parents)
        + ", seq="
        + seq
        + ", flags="
        + f
        + '}';
  }
}
