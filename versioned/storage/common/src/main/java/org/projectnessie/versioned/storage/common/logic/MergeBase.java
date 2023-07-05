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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.CommitLogicImpl.NO_COMMON_ANCESTOR_IN_PARENTS_OF;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.BOTH_COMMITS;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.CANDIDATE;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Stream;
import org.agrona.collections.Object2ObjectHashMap;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Identifies the nearest commit that shall be used as the base commit for a merge of the given
 * {@link #fromCommitId() from-commit} onto the given {@link #targetCommitId() target commit}.
 *
 * <p>This class also supports finding the base commit for N-way merges, although N-way merges are
 * not implemented for Nessie (yet).
 */
@Value.Immutable
public abstract class MergeBase {
  public abstract Function<ObjId, CommitObj> loadCommit();

  public abstract ObjId targetCommitId();

  public abstract ObjId fromCommitId();

  /**
   * Whether merge-parents shall be respected, defaults to {@code true}. Setting this to {@code
   * false} changes the behavior to return the <em>common ancestor</em> instead of the nearest
   * merge-base.
   */
  @Value.Default
  public boolean respectMergeParents() {
    return true;
  }

  public static ImmutableMergeBase.Builder builder() {
    return ImmutableMergeBase.builder();
  }

  @Value.NonAttribute
  public ObjId identifyMergeBase() {
    List<ShallowCommit> mergeBases = identifyAllMergeBases();
    if (mergeBases == null || mergeBases.isEmpty()) {
      throw noCommonAncestor();
    }
    return mergeBases.get(0).id();
  }

  private List<ShallowCommit> identifyAllMergeBases() {
    ShallowCommit targetCommit = shallowCommit(targetCommitId());
    if (targetCommit == null) {
      // This is rather a UX hack, to raise a "not found" exception if any of the from-commit-IDs
      // does not exist
      shallowCommit(fromCommitId());
      return null;
    }

    ShallowCommit fromCommit = shallowCommit(fromCommitId());
    if (fromCommit == null) {
      return singletonList(targetCommit);
    }

    return findMergeBases(fromCommit, targetCommit);
  }

  private List<ShallowCommit> findMergeBases(ShallowCommit commitA, ShallowCommit commitB) {
    if (commitB.id().equals(commitA.id())) {
      return newArrayList(commitA);
    }

    // Theoretically it should be correct to always return `reachableCommits` here. However, it
    // might be necessary to re-incarnate `removeRedundant`, if there is a situation in which it
    // would yield the "better" result, so keep the code.
    return flagReachableCommits(commitA, commitB);

  }

  private List<ShallowCommit> flagReachableCommits(ShallowCommit commitA, ShallowCommit commitB) {
    PriorityQueue<ShallowCommit> queue =
        new PriorityQueue<>(comparing(ShallowCommit::seq).reversed());
    List<ShallowCommit> result = new ArrayList<>();

    commitA.setCommitA();
    queue.add(commitA);

    commitB.setCommitB();
    queue.add(commitB);

    while (queue.stream().anyMatch(ShallowCommit::isNotCandidate)) {
      ShallowCommit commit = requireNonNull(queue.poll());

      int reachabilityFlags = commit.reachabilityFlags();
      if (reachabilityFlags == BOTH_COMMITS) {
        if (commit.setResult()) {
          // A new result commit
          result.add(commit);
        }
        // Populate the CANDIDATE flag "down".
        reachabilityFlags |= CANDIDATE;
      }

      // Propagate the relevant COMMIT_A, COMMIT_B, CANDIDATE flags down to the parent commits,
      // enqueue those, if the relevant flags were not already set.
      int reachabilityFlagsFinal = reachabilityFlags;
      parentCommits(commit)
          .filter(parent -> parent.setAllFlagsIfAnyMissing(reachabilityFlagsFinal))
          .forEach(queue::add);
    }

    return result;
  }
  private NoSuchElementException noCommonAncestor() {
    return new NoSuchElementException(
        NO_COMMON_ANCESTOR_IN_PARENTS_OF + targetCommitId() + " and " + fromCommitId());
  }

  private Stream<ShallowCommit> parentCommits(ShallowCommit commit) {
    return Arrays.stream(commit.parents()).map(this::shallowCommit).filter(Objects::nonNull);
  }

  private ShallowCommit shallowCommit(ObjId objId) {
    if (EMPTY_OBJ_ID.equals(objId)) {
      return null;
    }
    return commits.computeIfAbsent(
        objId,
        id -> {
          CommitObj commit = loadCommit().apply(id);
          if (commit == null) {
            throw new NoSuchElementException("Commit '" + id + "' not found");
          }
          ObjId[] parents;
          if (respectMergeParents()) {
            List<ObjId> secondary = commit.secondaryParents();
            parents = new ObjId[1 + secondary.size()];
            int end = parents.length - 1;
            for (int i = 0; i < end; i++) {
              parents[i] = secondary.get(i);
            }
            parents[end] = commit.directParent();
          } else {
            parents = new ObjId[] {commit.directParent()};
          }
          return new ShallowCommit(commit.id(), parents, commit.seq());
        });
  }

  private final Object2ObjectHashMap<ObjId, ShallowCommit> commits = new Object2ObjectHashMap<>();
}
