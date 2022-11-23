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
package org.projectnessie.versioned.transfer;

import static java.util.Collections.singletonList;
import static org.projectnessie.versioned.transfer.ExportImportConstants.DEFAULT_EXPECTED_COMMIT_COUNT;

import com.google.common.primitives.Ints;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.HeadsAndForkPoints;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ReferencesUtil;

@Value.Immutable
public abstract class CommitLogOptimization {

  public static Builder builder() {
    return ImmutableCommitLogOptimization.builder();
  }

  @SuppressWarnings("UnusedReturnValue")
  public interface Builder {
    /** Mandatory, specify the {@code DatabaseAdapter} to use. */
    Builder databaseAdapter(DatabaseAdapter databaseAdapter);

    /**
     * When a {@link HeadsAndForkPoints} is available, for example from a {@link
     * ImportResult#headsAndForks()}, specify it to prevent one full scan of all commits.
     */
    Builder headsAndForks(HeadsAndForkPoints headsAndForks);

    /**
     * When a {@link HeadsAndForkPoints} is not available, it is recommended to specify the number
     * of commits. The default value is {@value
     * ExportImportConstants#DEFAULT_EXPECTED_COMMIT_COUNT}.
     */
    Builder totalCommitCount(int totalCommitCount);

    CommitLogOptimization build();
  }

  abstract DatabaseAdapter databaseAdapter();

  @Value.Default
  int totalCommitCount() {
    return DEFAULT_EXPECTED_COMMIT_COUNT;
  }

  @Nullable
  abstract HeadsAndForkPoints headsAndForks();

  public void optimize() {
    HeadsAndForkPoints headsAndForks = headsAndForks();
    if (headsAndForks == null) {
      ReferencesUtil refsUtil = ReferencesUtil.forDatabaseAdapter(databaseAdapter());
      headsAndForks = refsUtil.identifyAllHeadsAndForkPoints(totalCommitCount(), e -> {});
    }

    int parentsPerCommit = databaseAdapter().getConfig().getParentsPerCommit();

    CommitParentsState commitParentsState = new CommitParentsState(databaseAdapter());
    KeyListsState keyListState = new KeyListsState(databaseAdapter());

    for (Hash head : headsAndForks.getHeads()) {
      try (Stream<CommitLogEntry> log = databaseAdapter().commitLog(head)) {
        int delayedStop = -1;
        for (Iterator<CommitLogEntry> logIter = log.iterator(); logIter.hasNext(); ) {
          CommitLogEntry entry = logIter.next();

          if (delayedStop == -1) {
            if (commitParentsState.canStopIterating(entry)) {
              // Stop iterating the commit log when the current entry already has enough parents,
              // because we can assume that all previous commit log entries have enough parents.
              // But need to collect the commit-IDs of 'parentsPerCommit' more entries to populate
              // the parent-commits-ids of the previous 'parentsPerCommit' log entries as well.
              delayedStop = parentsPerCommit;
            }
          } else {
            if (delayedStop == 0) {
              commitParentsState.clear();
              break;
            }
            delayedStop--;
          }

          commitParentsState.handleEntry(entry);

          keyListState.handleEntry(entry);
        }
      } catch (ReferenceNotFoundException e) {
        throw new RuntimeException(e);
      }

      commitParentsState.drain();
    }

    keyListState.updateCommitsWithKeyLists();
  }

  /**
   * Collects the commit-ids that need to get an aggregated key list.
   *
   * <p>Orders commits to update by commit-sequence, so that key-list generation itself can benefit
   * from already built key-lists.
   */
  static final class KeyListsState {
    private final DatabaseAdapter databaseAdapter;
    private final int keyListDistance;
    private final List<List<Hash>> updateCommitsByKeyListSeq = new ArrayList<>();

    KeyListsState(DatabaseAdapter databaseAdapter) {
      this.keyListDistance = databaseAdapter.getConfig().getKeyListDistance();
      this.databaseAdapter = databaseAdapter;
    }

    void handleEntry(CommitLogEntry entry) {
      if (entry.getCommitSeq() > 0 && entry.getCommitSeq() % keyListDistance == 0) {
        int keyListSeq = Ints.checkedCast(entry.getCommitSeq() / keyListDistance) - 1;
        if (!entry.hasKeySummary()) {
          while (keyListSeq >= updateCommitsByKeyListSeq.size()) {
            updateCommitsByKeyListSeq.add(null);
          }
          Hash commitId = entry.getHash();
          List<Hash> commitIdsToUpdate = updateCommitsByKeyListSeq.get(keyListSeq);
          if (commitIdsToUpdate == null) {
            updateCommitsByKeyListSeq.set(keyListSeq, singletonList(commitId));
          } else {
            if (!commitIdsToUpdate.contains(commitId)) {
              if (commitIdsToUpdate.size() == 1) {
                commitIdsToUpdate = new ArrayList<>(commitIdsToUpdate);
                updateCommitsByKeyListSeq.set(keyListSeq, commitIdsToUpdate);
              }
              commitIdsToUpdate.add(commitId);
            }
          }
        }
      }
    }

    void updateCommitsWithKeyLists() {
      try (Stream<CommitLogEntry> entries =
          databaseAdapter.fetchCommitLogEntries(
              updateCommitsByKeyListSeq.stream()
                  .filter(Objects::nonNull)
                  .flatMap(Collection::stream))) {
        entries
            .map(
                entry -> {
                  try {
                    return databaseAdapter.rebuildKeyList(entry, h -> null);
                  } catch (ReferenceNotFoundException ignore) {
                    // Ignore this, because it can only happen if `entry`, which has been read
                    // before, no longer exists.
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .forEach(
                e -> {
                  try {
                    databaseAdapter.updateMultipleCommits(singletonList(e));
                  } catch (ReferenceNotFoundException ex) {
                    throw new RuntimeException(ex);
                  }
                });
      }
    }
  }

  /**
   * Keeps track of the recent N ({@code parents-per-commit + 1}) commit log entries to populate the
   * lists of parent commits, both {@link #handleEntry(CommitLogEntry) while scanning} the commit
   * log and when {@link #drain() reaching the "beginning of time"}.
   */
  static final class CommitParentsState {

    private final DatabaseAdapter databaseAdapter;
    private final int parentsPerCommit;
    private final ArrayDeque<CommitLogEntry> lastEntries;

    CommitParentsState(DatabaseAdapter databaseAdapter) {
      this.databaseAdapter = databaseAdapter;
      this.parentsPerCommit = databaseAdapter.getConfig().getParentsPerCommit();
      this.lastEntries = new ArrayDeque<>(this.parentsPerCommit + 1);
    }

    boolean canStopIterating(CommitLogEntry entry) {
      return entry.getParents().size() >= parentsPerCommit;
    }

    private boolean full() {
      return lastEntries.size() == this.parentsPerCommit + 1;
    }

    void handleEntry(CommitLogEntry entry) {
      if (full()) {
        lastEntries.removeFirst();
      }
      lastEntries.add(entry);
      if (full()) {
        updateLeastRecentCommitLogEntryParents();
      }
    }

    void drain() {
      while (!lastEntries.isEmpty()) {
        updateLeastRecentCommitLogEntryParents();
      }
    }

    private void updateLeastRecentCommitLogEntryParents() {
      CommitLogEntry commitToUpdate = lastEntries.removeFirst();
      List<Hash> currentParents = commitToUpdate.getParents();
      if (currentParents.size() >= parentsPerCommit) {
        return;
      }

      ImmutableCommitLogEntry.Builder newEntry =
          ImmutableCommitLogEntry.builder().from(commitToUpdate);

      List<Hash> calculatedParents = new ArrayList<>(lastEntries.size());
      lastEntries.forEach(e -> calculatedParents.add(e.getHash()));
      if (calculatedParents.size() < parentsPerCommit) {
        calculatedParents.add(databaseAdapter.noAncestorHash());
      }

      if (!calculatedParents.subList(0, currentParents.size()).equals(currentParents)) {
        throw new IllegalStateException(
            String.format(
                "commit %s calculated parents %s must start with the exsting list of parents %s",
                commitToUpdate.getHash().asString(), calculatedParents, currentParents));
      }

      newEntry.parents(calculatedParents);

      try {
        databaseAdapter.updateMultipleCommits(singletonList(newEntry.build()));
      } catch (ReferenceNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    void clear() {
      lastEntries.clear();
    }
  }
}
