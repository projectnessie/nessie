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
package org.projectnessie.gc.base;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.LogResponse;

public class TestLiveCommitsSpliterator {

  AtomicInteger commitId = new AtomicInteger();

  @Test
  void testCutoffTimestampFirst() {
    Instant cutOffTimestamp = Instant.now();
    List<LogResponse.LogEntry> allCommits = new ArrayList<>(generateCommits(10));
    List<LogResponse.LogEntry> traversedCommits = new ArrayList<>();
    Set<ContentKey> keys = new HashSet<>();
    keys.add(ContentKey.of("dummy"));
    // latest commit should be head, hence reverse the order.
    Collections.reverse(allCommits);
    Consumer<LogResponse.LogEntry> commitHandler =
        entry -> commitHandler(entry, keys, 8, traversedCommits);
    GCUtil.traverseLiveCommits(
        getCommitMetaPredicate(cutOffTimestamp),
        new MutableBoolean(true),
        keys,
        allCommits.stream(),
        commitHandler);
    // must traverse all commits as everything is live
    assertThat(allCommits).isEqualTo(traversedCommits);
  }

  @Test
  void testIndexWithinLiveCommits() {
    List<LogResponse.LogEntry> allCommits = new ArrayList<>(generateCommits(5));
    Instant cutOffTimestamp = Instant.now();
    allCommits.addAll(generateCommits(5));
    List<LogResponse.LogEntry> traversedCommits = new ArrayList<>();
    Set<ContentKey> keys = new HashSet<>();
    keys.add(ContentKey.of("dummy"));
    // latest commit should be head, hence reverse the order.
    Collections.reverse(allCommits);
    Consumer<LogResponse.LogEntry> commitHandler =
        entry -> commitHandler(entry, keys, 8, traversedCommits);
    GCUtil.traverseLiveCommits(
        getCommitMetaPredicate(cutOffTimestamp),
        new MutableBoolean(true),
        keys,
        allCommits.stream(),
        commitHandler);
    // must traverse all live commits (5)
    assertThat(allCommits.subList(0, 5)).isEqualTo(traversedCommits);
  }

  @Test
  void testIndexOutsideLiveCommits() {
    List<LogResponse.LogEntry> allCommits = new ArrayList<>(generateCommits(5));
    Instant cutOffTimestamp = Instant.now();
    allCommits.addAll(generateCommits(5));
    List<LogResponse.LogEntry> traversedCommits = new ArrayList<>();
    Set<ContentKey> keys = new HashSet<>();
    keys.add(ContentKey.of("dummy"));
    // latest commit should be head, hence reverse the order.
    Collections.reverse(allCommits);
    Consumer<LogResponse.LogEntry> commitHandler =
        entry -> commitHandler(entry, keys, 2, traversedCommits);
    GCUtil.traverseLiveCommits(
        getCommitMetaPredicate(cutOffTimestamp),
        new MutableBoolean(true),
        keys,
        allCommits.stream(),
        commitHandler);
    // must traverse all live commits (5) + commits till index (2)
    assertThat(allCommits.subList(0, 8)).isEqualTo(traversedCommits);
  }

  @Test
  void testIndexOutsideAllCommits() {
    List<LogResponse.LogEntry> allCommits = new ArrayList<>(generateCommits(5));
    Instant cutOffTimestamp = Instant.now();
    allCommits.addAll(generateCommits(5));
    List<LogResponse.LogEntry> traversedCommits = new ArrayList<>();
    Set<ContentKey> keys = new HashSet<>();
    keys.add(ContentKey.of("dummy"));
    // latest commit should be head, hence reverse the order.
    Collections.reverse(allCommits);
    Consumer<LogResponse.LogEntry> commitHandler =
        entry -> commitHandler(entry, keys, 50, traversedCommits);
    GCUtil.traverseLiveCommits(
        getCommitMetaPredicate(cutOffTimestamp),
        new MutableBoolean(true),
        keys,
        allCommits.stream(),
        commitHandler);
    // must traverse all the commits as the spliterator index is outside all commits
    assertThat(allCommits.subList(0, 10)).isEqualTo(traversedCommits);
  }

  @Test
  void testCutoffTimestampLast() {
    List<LogResponse.LogEntry> allCommits = new ArrayList<>(generateCommits(10));
    Instant cutOffTimestamp = Instant.now();
    List<LogResponse.LogEntry> traversedCommits = new ArrayList<>();
    Set<ContentKey> keys = new HashSet<>();
    keys.add(ContentKey.of("dummy"));
    // latest commit should be head, hence reverse the order.
    Collections.reverse(allCommits);
    Consumer<LogResponse.LogEntry> commitHandler =
        entry -> commitHandler(entry, keys, 8, traversedCommits);
    GCUtil.traverseLiveCommits(
        getCommitMetaPredicate(cutOffTimestamp),
        new MutableBoolean(true),
        keys,
        allCommits.stream(),
        commitHandler);
    // everything is expired, so traverse till the index.
    assertThat(allCommits.subList(0, 2)).isEqualTo(traversedCommits);
  }

  private Predicate<CommitMeta> getCommitMetaPredicate(Instant cutOffTimestamp) {
    return commitMeta ->
        // If the commit time is newer than (think: greater than or equal to) cutoff-time,
        // then commit is live.
        Objects.requireNonNull(commitMeta.getCommitTime()).compareTo(cutOffTimestamp) >= 0;
  }

  private List<LogResponse.LogEntry> generateCommits(int num) {
    List<LogResponse.LogEntry> entries = new ArrayList<>();
    IntStream.range(0, num)
        .forEach(
            i -> {
              ImmutableCommitMeta commitMeta =
                  CommitMeta.builder()
                      .hash(String.valueOf(commitId.get()))
                      .message("commit id: " + commitId.getAndIncrement())
                      .commitTime(Instant.now())
                      .build();
              entries.add(ImmutableLogEntry.builder().commitMeta(commitMeta).build());
            });
    return entries;
  }

  private void commitHandler(
      LogResponse.LogEntry entry,
      Set<ContentKey> liveKeys,
      int lastLiveCommitHeadIndex,
      List<LogResponse.LogEntry> traversedCommits) {
    if (String.valueOf(lastLiveCommitHeadIndex).equals(entry.getCommitMeta().getHash())) {
      // remove all entries so spliterator will stop traversing.
      liveKeys.remove(ContentKey.of("dummy"));
    }
    traversedCommits.add(entry);
  }
}
