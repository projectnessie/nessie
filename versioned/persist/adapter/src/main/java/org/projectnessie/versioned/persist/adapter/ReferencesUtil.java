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
package org.projectnessie.versioned.persist.adapter;

import com.google.common.annotations.Beta;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;

@Beta
public final class ReferencesUtil {
  private final DatabaseAdapter databaseAdapter;
  private final DatabaseAdapterConfig config;

  private ReferencesUtil(DatabaseAdapter databaseAdapter, DatabaseAdapterConfig config) {
    this.databaseAdapter = databaseAdapter;
    this.config = config;
  }

  public static ReferencesUtil forDatabaseAdapter(
      DatabaseAdapter databaseAdapter, DatabaseAdapterConfig config) {
    return new ReferencesUtil(databaseAdapter, config);
  }

  private static <K, V> Map<K, V> newOpenAddressingHashMap() {
    return new Object2ObjectHashMap<>(16, Hashing.DEFAULT_LOAD_FACTOR, false);
  }

  private static <T> Set<T> newOpenAddressingHashSet(int initialCapacity) {
    return new ObjectHashSet<>(initialCapacity, Hashing.DEFAULT_LOAD_FACTOR, false);
  }

  private static <T> Set<T> newOpenAddressingHashSet(Set<T> source) {
    ObjectHashSet<T> copy = new ObjectHashSet<>(source.size(), Hashing.DEFAULT_LOAD_FACTOR, false);
    if (source instanceof ObjectHashSet) {
      copy.addAll((ObjectHashSet<T>) source);
    } else {
      copy.addAll(source);
    }
    return copy;
  }

  private static <T> Set<T> newOpenAddressingHashSet() {
    return newOpenAddressingHashSet(ObjectHashSet.DEFAULT_INITIAL_CAPACITY);
  }

  /**
   * Identifies all heads and fork-points.
   *
   * <ul>
   *   <li>"Heads" are commits that are not referenced by other commits.
   *   <li>"Fork points" are commits that are the parent of more than one other commit. Knowing
   *       these commits can help to optimize the traversal of commit logs of multiple heads.
   * </ul>
   *
   * @param expectedCommitCount it is recommended to tell the implementation the total number of
   *     commits in the Nessie repository
   */
  public HeadsAndForkPoints identifyAllHeadsAndForkPoints(int expectedCommitCount) {
    // Using open-addressing implementation here, because it's much more space-efficient than
    // java.util.HashSet.
    Set<Hash> parents = newOpenAddressingHashSet(expectedCommitCount);
    Set<Hash> heads = newOpenAddressingHashSet();
    Set<Hash> forkPoints = newOpenAddressingHashSet();

    // Need to remember the time when the identification started, so that a follow-up
    // identifyReferencedAndUnreferencedHeads() knows when it can stop scanning a named-reference's
    // commit-log. identifyReferencedAndUnreferencedHeads() has to read up to the first commit
    // _before_ this timestamp to not commit-IDs as "unreferenced".
    //
    // Note: keep in mind, that scanAllCommitLogEntries() returns all commits in a
    // non-deterministic order. Example: if (at least) two commits are added to a branch while this
    // function is running, the original HEAD of that branch could otherwise be returned as
    // "unreferenced".
    long scanStartedAtInMicros = config.currentTimeInMicros();

    // scanAllCommitLogEntries() returns all commits in no specific order, parents may be scanned
    // before or after their children.
    try (Stream<CommitLogEntry> scan = databaseAdapter.scanAllCommitLogEntries()) {
      scan.forEach(
          entry -> {
            Hash parent = entry.getParents().get(0);
            if (!parents.add(parent)) {
              // If "parent" has already been added to the set of parents, then it must be a
              // fork point.
              forkPoints.add(parent);
            } else {
              // Commits in "parents" that are also contained in "heads" cannot be HEADs.
              // This can happen because the commits are scanned in "random order".
              // Should do this here to prevent the "heads" set from becoming unnecessarily big.
              heads.remove(parent);
            }

            Hash commitId = entry.getHash();
            if (!parents.contains(commitId)) {
              // If the commit-ID is not present in "parents", it must be a HEAD
              heads.add(commitId);
            }
          });
    }

    // Commits in "parents" that are also contained in "heads" cannot be HEADs.
    // This can happen because the commits are scanned in "random order".
    // TODO this 'removeIf' should not actually remove anything, because it should already be
    //  properly handled above.
    heads.removeIf(parents::contains);

    return HeadsAndForkPoints.of(heads, forkPoints, scanStartedAtInMicros);
  }

  /**
   * Identifies unreferenced heads and heads that are part of a named reference.
   *
   * <p>Requires the output of {@link #identifyAllHeadsAndForkPoints(int)}.
   */
  public ReferencedAndUnreferencedHeads identifyReferencedAndUnreferencedHeads(
      HeadsAndForkPoints headsAndForkPoints) throws ReferenceNotFoundException {
    Map<Hash, Set<NamedRef>> referenced = newOpenAddressingHashMap();
    Set<Hash> heads = headsAndForkPoints.getHeads();
    Set<Hash> unreferenced = newOpenAddressingHashSet(heads);

    long stopAtCommitTimeMicros =
        headsAndForkPoints.getScanStartedAtInMicros() - config.getAssumedWallClockDriftMicros();

    try (Stream<ReferenceInfo<ByteString>> namedRefs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      namedRefs.forEach(
          refInfo -> {
            try (Stream<CommitLogEntry> logs = databaseAdapter.commitLog(refInfo.getHash())) {
              if (!referenced.containsKey(refInfo.getHash())) {
                // Only need to traverse the commit log from the same commit-ID once.
                for (Iterator<CommitLogEntry> logIter = logs.iterator(); logIter.hasNext(); ) {
                  CommitLogEntry entry = logIter.next();

                  Hash commitId = entry.getHash();

                  if (referenced.containsKey(commitId)) {
                    // Already saw this commit-ID, can break
                    break;
                  }

                  if (heads.contains(commitId)) {
                    unreferenced.remove(entry.getHash());
                  }

                  if (entry.getCreatedTime() < stopAtCommitTimeMicros) {
                    // Must scan up to the commit created right before
                    // identifyAllHeadsAndForkPoints() started to not accidentally return
                    // commits in 'unreferencedHeads' that were the HEAD of a commit that happened
                    // after identifyAllHeadsAndForkPoints() started.
                    break;
                  }
                }
              }

              // Add the named reference to the reachable HEADs.
              referenced
                  .computeIfAbsent(refInfo.getHash(), x -> newOpenAddressingHashSet())
                  .add(refInfo.getNamedRef());
            } catch (ReferenceNotFoundException e) {
              throw new RuntimeException(e);
            }
          });
    }

    return ReferencedAndUnreferencedHeads.of(referenced, unreferenced);
  }
}
