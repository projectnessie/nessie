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
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.ALL_FLAGS;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.BOTH_COMMITS;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.CANDIDATE;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
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

    List<ShallowCommit> reachableCommits = flagReachableCommits(commitA, commitB);

    clearFlags(newArrayList(commitA, commitB), ALL_FLAGS);

    return removeRedundant(reachableCommits);
  }

  private List<ShallowCommit> flagReachableCommits(ShallowCommit commitA, ShallowCommit commitB) {
    PriorityQueue<ShallowCommit> queue = new PriorityQueue<>(comparing(ShallowCommit::seq));
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

  private List<ShallowCommit> removeRedundant(List<ShallowCommit> reachableCommits) {
    // Note: all commits in 'reachableCommits' have the RESULT flag set.

    if (reachableCommits.isEmpty()) {
      return reachableCommits;
    }

    // Sort the given commits by 'seq'. This allows us to increase the 'min seq' limit when we
    // discover the commit with the lowest 'seq' that is CANDIDATE.
    reachableCommits.sort(comparing(ShallowCommit::seq));
    // 'commitMinSeq.seq' allows us to "ignore" commits that are _not_ nearer. (Want to find the
    // "nearest" or "closest" merge-base.)
    // The index 'minSeqIndex' points to the current index of 'commitMinSeq' within
    // 'reachableCommits' that is not yet known to be CANDIDATE.
    int minSeqIndex = 0;
    ShallowCommit commitMinSeq = reachableCommits.get(minSeqIndex);

    // For all reachable commits that were not yet flagged as RESULT, flag all their direct parents
    // as CANDIDATE and return the new candidates. 'newCandidates' holds the commits, "newest" ones
    // (by 'seq') first.
    List<ShallowCommit> newCandidates =
        reachableCommits.stream()
            .peek(ShallowCommit::setResult)
            .flatMap(this::parentCommits)
            .filter(ShallowCommit::setCandidate)
            .sorted(comparing(ShallowCommit::seq).reversed())
            .collect(Collectors.toList());

    // Remove CANDIDATE flag for now to allow walking through parents.
    newCandidates.forEach(ShallowCommit::clearCandidate);

    // Iterate, start with the highest 'seq'. It should find all other commits during parents-walk,
    // allowing us to terminate early.
    int reachableCommitCount = reachableCommits.size();
    // Number of commits that are flagged as a RESULT.
    int remainingResults = reachableCommitCount;
    Deque<ShallowCommit> deque = new ArrayDeque<>();
    for (int i = 0; i < newCandidates.size() && remainingResults > 1; i++) {
      // note: 'deque' is always empty here

      ShallowCommit candidate = newCandidates.get(i);
      candidate.setCandidate();
      deque.add(candidate);

      while (!deque.isEmpty()) {
        ShallowCommit c = deque.peek();

        if (c.clearResult()) {
          // 'c' had the RESULT flag set
          if (--remainingResults == 0) {
            // All RESULTs processed - done with all candidates (will exit inner and outer loop).
            break;
          }

          // If 'c' is the commit "min-seq commit", push minSeq to the first non-CANDIDATE commit.
          if (c.id().equals(commitMinSeq.id())) {
            while (minSeqIndex < reachableCommitCount - 1 && commitMinSeq.isCandidate()) {
              minSeqIndex++;
              commitMinSeq = reachableCommits.get(minSeqIndex);
            }
          }
        }

        // Find the first "new" candidate commit with a higher 'seq', enqueue it and start over.
        if (c.seq() >= commitMinSeq.seq()) {
          // Get the first non-CANDIDATE flagged parent commit (then flagged as CANDIDATE).
          Optional<ShallowCommit> firstNonCandidate =
              parentCommits(c).filter(ShallowCommit::setCandidate).findFirst();
          if (firstNonCandidate.isPresent()) {
            // Found a commit that is "nearer" _and_ was not flagged as CANDIDATE.
            deque.addFirst(firstNonCandidate.get());
            continue;
          }
        }

        // All candidates have been visited, remove 'c' from the deque.
        deque.remove();
      }
    }

    // Clear RESULT flag and build the 'result' list ordered by seq. Need to '.collect()' here,
    // because we need the CANDIDATE flag as a filter condition, but have to clear it before
    // returning.
    List<ShallowCommit> result =
        reachableCommits.stream()
            .peek(ShallowCommit::clearResult)
            .filter(ShallowCommit::isNotCandidate)
            .sorted(comparing(ShallowCommit::seq))
            .collect(Collectors.toList());

    // Clear CANDIDATE flag
    clearFlags(newCandidates, CANDIDATE);

    return result;
  }

  /** Clears the given flags from all given commits recursively. */
  private void clearFlags(List<ShallowCommit> commits, int flags) {
    Deque<ShallowCommit> remainingParents = new ArrayDeque<>();

    for (ShallowCommit commit : commits) {
      // Note: 'remainingParents' is mutated
      clearFlagsInner(remainingParents, commit, flags);
    }

    while (!remainingParents.isEmpty()) {
      ShallowCommit commit = remainingParents.removeFirst();
      // Note: 'remainingParents' is mutated
      clearFlagsInner(remainingParents, commit, flags);
    }
  }

  private void clearFlagsInner(
      Deque<ShallowCommit> remainingParents, ShallowCommit commit, int flags) {
    // Clear commit flags of the given commit and its _direct_ parents (predecessors), repeat until
    // the first "untouched" commit is reached (matching the logic elsewhere in this class).
    while (commit.clearFlagsIfAnySet(flags)) {

      Iterator<ShallowCommit> parents = parentCommits(commit).iterator();
      if (!parents.hasNext()) {
        // no parents, at "beginning of time", nothing to do anymore
        return;
      }

      // If 'commit' or any of its parents has any flag to be cleared, add it to 'remainingParents'
      // so it is handled in the next iteration in 'clearFlags()'.
      // Start with the direct parent of the commit passed into 'clearFlagsInner()'.
      commit = parents.next();
      if (commit.isAnyFlagSet(flags)) {
        remainingParents.addLast(commit);
      }
      while (parents.hasNext()) {
        ShallowCommit parent = parents.next();
        if (parent.isAnyFlagSet(flags)) {
          remainingParents.addLast(parent);
        }
      }
    }
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
