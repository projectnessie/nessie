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

import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.CommitLogicImpl.NO_COMMON_ANCESTOR_IN_PARENTS_OF;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.agrona.collections.ObjectHashSet;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@Value.Immutable
public abstract class MergeBase {
  public abstract CommitLogic commitLogic();

  public abstract List<ObjId> heads();

  public abstract boolean respectMergeParents();

  public static ImmutableMergeBase.Builder builder() {
    return ImmutableMergeBase.builder();
  }

  @Value.NonAttribute
  public CommitObj identifyMergeBase() {
    List<Chain> chains = new ArrayList<>();
    for (ObjId head : heads()) {
      chains.add(addChain(head));
    }

    while (true) {
      boolean allEof = true;
      for (int i = 0; i < chains.size(); i++) {
        Chain chain = chains.get(i);
        CommitObj commit = chain.next();
        if (commit != null) {
          allEof = false;

          for (int j = 0; j < chains.size(); j++) {
            if (j != i) {
              Chain chk = chains.get(j);
              if (chk.commits.contains(commit.id())) {
                return commit;
              }
            }
          }

          for (ObjId secondaryParent : commit.secondaryParents()) {
            if (chains.stream().noneMatch(c -> c.commits.contains(secondaryParent))) {
              chains.add(addChain(secondaryParent));
            }
          }
        }
      }
      if (allEof) {
        throw noCommonAncestor();
      }
    }
  }

  Chain addChain(ObjId head) {
    Iterator<CommitObj> log = commitLogic().commitLog(commitLogQuery(head));
    return new Chain(log);
  }

  private NoSuchElementException noCommonAncestor() {
    StringBuilder sb = new StringBuilder(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
    for (int i = 0; i < heads().size(); i++) {
      if (i == heads().size() - 1) {
        sb.append(" and ");
      } else if (i > 0) {
        sb.append(", ");
      }
      sb.append(heads().get(i));
    }
    return new NoSuchElementException(sb.toString());
  }

  static final class Chain {
    final ObjectHashSet<ObjId> commits = new ObjectHashSet<>();
    final Iterator<CommitObj> commitLog;

    Chain(Iterator<CommitObj> commitLog) {
      this.commitLog = commitLog;
    }

    CommitObj next() {
      Iterator<CommitObj> log = commitLog;
      if (log.hasNext()) {
        CommitObj commit = log.next();
        commits.add(commit.id());
        return commit;
      }
      return null;
    }
  }
}
