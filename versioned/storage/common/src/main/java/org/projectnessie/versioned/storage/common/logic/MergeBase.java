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
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.DEFAULT_PARENTS_PER_COMMIT;
import static org.projectnessie.versioned.storage.common.logic.CommitLogicImpl.NO_COMMON_ANCESTOR_IN_PARENTS_OF;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.BOTH_COMMITS;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.CANDIDATE;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
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
  /**
   * Loads the given commits, returning one element per requested {@link ObjId}, in the same order,
   * {@code null} for commits that do not exist. Commits are requested in batches, so a
   * storage-backed implementation should use a bulk fetch.
   */
  public abstract Function<List<ObjId>, List<CommitObj>> loadCommits();

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

    //    List<ShallowCommit> reachableCommits = flagReachableCommits(commitA, commitB);
    //
    //    int numReachable = reachableCommits.size();
    //    if (numReachable < 2) {
    //      // Short-cut, if there is only one reachable commit that is already the result for both
    //      // commits. Also if there are no reachable commits at all.
    //      return reachableCommits;
    //    }
    //
    //    clearFlags(newArrayList(commitA, commitB), ALL_FLAGS);
    //
    //    return removeRedundant(reachableCommits);
  }

  private List<ShallowCommit> flagReachableCommits(ShallowCommit commitA, ShallowCommit commitB) {
    PriorityQueue<ShallowCommit> queue =
        new PriorityQueue<>(comparingLong(ShallowCommit::seq).reversed());
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

  //  private List<ShallowCommit> removeRedundant(List<ShallowCommit> reachableCommits) {
  //    // Note: all commits in 'reachableCommits' have the RESULT flag set.
  //
  //    // Sort the given commits by 'seq'. This allows us to increase the 'min seq' limit when we
  //    // discover the commit with the lowest 'seq' that is CANDIDATE.
  //    reachableCommits.sort(comparing(ShallowCommit::seq));
  //    // 'commitMinSeq.seq' allows us to "ignore" commits that are _not_ nearer. (Want to find the
  //    // "nearest" or "closest" merge-base.)
  //    // The index 'minSeqIndex' points to the current index of 'commitMinSeq' within
  //    // 'reachableCommits' that is not yet known to be CANDIDATE.
  //    int minSeqIndex = 0;
  //    ShallowCommit commitMinSeq = reachableCommits.get(minSeqIndex);
  //
  //    // For all reachable commits that were not yet flagged as RESULT, flag all their direct
  // parents
  //    // as CANDIDATE and return the new candidates. 'newCandidates' holds the commits, "newest"
  // ones
  //    // (by 'seq') first.
  //    List<ShallowCommit> newCandidates =
  //        reachableCommits.stream()
  //            .peek(ShallowCommit::setResult)
  //            .flatMap(this::parentCommits)
  //            .filter(ShallowCommit::setCandidate)
  //            .sorted(comparing(ShallowCommit::seq).reversed())
  //            .collect(Collectors.toList());
  //
  //    // Remove CANDIDATE flag for now to allow walking through parents.
  //    newCandidates.forEach(ShallowCommit::clearCandidate);
  //
  //    // Iterate, start with the highest 'seq'. It should find all other commits during
  // parents-walk,
  //    // allowing us to terminate early.
  //    int reachableCommitCount = reachableCommits.size();
  //    // Number of commits that are flagged as a RESULT.
  //    int remainingResults = reachableCommitCount;
  //    Deque<ShallowCommit> deque = new ArrayDeque<>();
  //    for (int i = 0; i < newCandidates.size() && remainingResults > 1; i++) {
  //      // note: 'deque' is always empty here
  //
  //      ShallowCommit candidate = newCandidates.get(i);
  //      candidate.setCandidate();
  //      deque.add(candidate);
  //
  //      while (!deque.isEmpty()) {
  //        ShallowCommit c = deque.peek();
  //
  //        if (c.clearResult()) {
  //          // 'c' had the RESULT flag set
  //          if (--remainingResults == 0) {
  //            // All RESULTs processed - done with all candidates (will exit inner and outer
  // loop).
  //            break;
  //          }
  //
  //          // If 'c' is the commit "min-seq commit", push minSeq to the first non-CANDIDATE
  // commit.
  //          if (c.id().equals(commitMinSeq.id())) {
  //            while (minSeqIndex < reachableCommitCount - 1 && commitMinSeq.isCandidate()) {
  //              minSeqIndex++;
  //              commitMinSeq = reachableCommits.get(minSeqIndex);
  //            }
  //          }
  //        }
  //
  //        // Find the first "new" candidate commit with a higher 'seq', enqueue it and start over.
  //        if (c.seq() >= commitMinSeq.seq()) {
  //          // Get the first non-CANDIDATE flagged parent commit (then flagged as CANDIDATE).
  //          Optional<ShallowCommit> firstNonCandidate =
  //              parentCommits(c).filter(ShallowCommit::setCandidate).findFirst();
  //          if (firstNonCandidate.isPresent()) {
  //            // Found a commit that is "nearer" _and_ was not flagged as CANDIDATE.
  //            deque.addFirst(firstNonCandidate.get());
  //            continue;
  //          }
  //        }
  //
  //        // All candidates have been visited, remove 'c' from the deque.
  //        deque.remove();
  //      }
  //    }
  //
  //    // Clear RESULT flag and build the 'result' list ordered by seq. Need to '.collect()' here,
  //    // because we need the CANDIDATE flag as a filter condition, but have to clear it before
  //    // returning.
  //    List<ShallowCommit> result =
  //        reachableCommits.stream()
  //            .peek(ShallowCommit::clearResult)
  //            .filter(ShallowCommit::isNotCandidate)
  //            .sorted(comparing(ShallowCommit::seq))
  //            .collect(Collectors.toList());
  //
  //    // Clear CANDIDATE flag
  //    clearFlags(newCandidates, CANDIDATE);
  //
  //    return result;
  //  }

  //  /** Clears the given flags from all given commits recursively. */
  //  private void clearFlags(List<ShallowCommit> commits, int flags) {
  //    Deque<ShallowCommit> remainingParents = new ArrayDeque<>();
  //
  //    for (ShallowCommit commit : commits) {
  //      // Note: 'remainingParents' is mutated
  //      clearFlagsInner(remainingParents, commit, flags);
  //    }
  //
  //    while (!remainingParents.isEmpty()) {
  //      ShallowCommit commit = remainingParents.removeFirst();
  //      // Note: 'remainingParents' is mutated
  //      clearFlagsInner(remainingParents, commit, flags);
  //    }
  //  }

  //  private void clearFlagsInner(
  //      Deque<ShallowCommit> remainingParents, ShallowCommit commit, int flags) {
  //    // Clear commit flags of the given commit and its _direct_ parents (predecessors), repeat
  // until
  //    // the first "untouched" commit is reached (matching the logic elsewhere in this class).
  //    while (commit.clearFlagsIfAnySet(flags)) {
  //
  //      Iterator<ShallowCommit> parents = parentCommits(commit).iterator();
  //      if (!parents.hasNext()) {
  //        // no parents, at "beginning of time", nothing to do anymore
  //        return;
  //      }
  //
  //      // If 'commit' or any of its parents has any flag to be cleared, add it to
  // 'remainingParents'
  //      // so it is handled in the next iteration in 'clearFlags()'.
  //      // Start with the direct parent of the commit passed into 'clearFlagsInner()'.
  //      commit = parents.next();
  //      if (commit.isAnyFlagSet(flags)) {
  //        remainingParents.addLast(commit);
  //      }
  //      while (parents.hasNext()) {
  //        ShallowCommit parent = parents.next();
  //        if (parent.isAnyFlagSet(flags)) {
  //          remainingParents.addLast(parent);
  //        }
  //      }
  //    }
  //  }

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
    ShallowCommit shallow = commits.get(objId);
    if (shallow != null) {
      return shallow;
    }

    CommitObj commit = fetch(objId);
    if (commit == null) {
      throw new NoSuchElementException("Commit '" + objId + "' not found");
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

    shallow = new ShallowCommit(commit.id(), parents, commit.seq());
    commits.put(objId, shallow);
    return shallow;
  }

  private CommitObj fetch(ObjId objId) {
    CommitObj commit = prefetched.remove(objId);
    if (commit == null) {
      commit = loadBatch(objId);
    }
    if (commit != null) {
      rememberChainAhead(commit);
    }
    return commit;
  }

  /**
   * Loads {@code required} together with the commits that the walk is known to need next, in one
   * batch.
   */
  private CommitObj loadBatch(ObjId required) {
    List<ObjId> ids = new ArrayList<>();
    ids.add(required);
    // Take no more look-ahead than can be held, so that loaded commits never have to be evicted:
    // the nearest, and therefore next needed, commit would be the first to go.
    int capacity = PREFETCH_LIMIT - prefetched.size();
    for (Iterator<ObjId> iter = chainAhead.iterator(); iter.hasNext() && ids.size() <= capacity; ) {
      ObjId id = iter.next();
      iter.remove();
      if (!id.equals(required) && !commits.containsKey(id) && !prefetched.containsKey(id)) {
        ids.add(id);
      }
    }

    List<CommitObj> loaded = loadCommits().apply(ids);

    CommitObj result = null;
    for (int i = 0; i < ids.size(); i++) {
      CommitObj commit = loaded.get(i);
      if (commit == null) {
        continue;
      }
      if (i == 0) {
        result = commit;
      } else {
        prefetched.put(ids.get(i), commit);
      }
    }
    return result;
  }

  /**
   * Remembers {@link CommitObj#tail()}, the first-parent chain ahead of the given commit, so that
   * the next batch continues along the chain that is currently being walked. Merge parents are not
   * added here: those are only needed once the commit carrying them is processed.
   */
  private void rememberChainAhead(CommitObj commit) {
    for (ObjId id : commit.tail()) {
      if (EMPTY_OBJ_ID.equals(id) || chainAhead.size() >= PREFETCH_LIMIT) {
        break;
      }
      if (!commits.containsKey(id) && !prefetched.containsKey(id)) {
        chainAhead.add(id);
      }
    }
  }

  /**
   * Upper bound for the number of loaded, not yet processed commits held in memory, and with that
   * for the size of a batch. Enough for the two branches being merged to contribute one {@link
   * CommitObj#tail()} each.
   */
  private static final int PREFETCH_LIMIT = 2 * DEFAULT_PARENTS_PER_COMMIT;

  private final Object2ObjectHashMap<ObjId, ShallowCommit> commits = new Object2ObjectHashMap<>();

  /** Loaded commits that have not been turned into a {@link ShallowCommit} yet. */
  private final Map<ObjId, CommitObj> prefetched = new LinkedHashMap<>();

  /** Ids of commits ahead of the current position, used as the next batch. */
  private final Set<ObjId> chainAhead = new LinkedHashSet<>();
}
