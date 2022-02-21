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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.LogResponse;

public class TestLiveCommitsSpliterator {

  AtomicInteger commitId = new AtomicInteger();

  @Test
  void testFoundCommitsWithinIndex() {
    List<LogResponse.LogEntry> allCommits = new ArrayList<>(generateCommits(10));
    List<LogResponse.LogEntry> traversedCommits = new ArrayList<>();
    // latest commit should be head, hence reverse the order.
    Collections.reverse(allCommits);
    MutableBoolean foundAllCommitHeads = new MutableBoolean(false);
    Consumer<LogResponse.LogEntry> commitHandler =
        entry -> commitHandler(entry, foundAllCommitHeads, 8, traversedCommits);
    GCUtil.traverseLiveCommits(foundAllCommitHeads, allCommits.stream(), commitHandler);
    // stop at index. So should traverse only first two commit.
    assertThat(allCommits.subList(0, 2)).isEqualTo(traversedCommits);
  }

  @Test
  void testFoundCommitsOutsideIndex() {
    List<LogResponse.LogEntry> allCommits = new ArrayList<>(generateCommits(5));
    List<LogResponse.LogEntry> traversedCommits = new ArrayList<>();
    // latest commit should be head, hence reverse the order.
    Collections.reverse(allCommits);
    MutableBoolean foundAllCommitHeads = new MutableBoolean(false);
    Consumer<LogResponse.LogEntry> commitHandler =
        entry -> commitHandler(entry, foundAllCommitHeads, 12, traversedCommits);
    GCUtil.traverseLiveCommits(foundAllCommitHeads, allCommits.stream(), commitHandler);
    // must traverse all commits as everything is live
    assertThat(allCommits).isEqualTo(traversedCommits);
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
      MutableBoolean foundAllCommitHeads,
      int lastLiveCommitHeadIndex,
      List<LogResponse.LogEntry> traversedCommits) {
    if (String.valueOf(lastLiveCommitHeadIndex).equals(entry.getCommitMeta().getHash())) {
      foundAllCommitHeads.setTrue();
    }
    traversedCommits.add(entry);
  }
}
