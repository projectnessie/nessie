/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.testworker.SimpleStoreWorker;
import org.projectnessie.versioned.testworker.WithGlobalStateContent;

public abstract class AbstractCompactGlobalLog {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractCompactGlobalLog(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  public static Stream<Arguments> compactGlobalLog() {
    return Stream.of(Arguments.of(30, 0d), Arguments.of(30, .1d), Arguments.of(30, .9d));
  }

  @ParameterizedTest
  @MethodSource("compactGlobalLog")
  public void compactGlobalLog(int commits, double contentReuseProbability) throws Exception {
    Map<String, Map<String, String>> statistics = databaseAdapter.repoMaintenance();

    // If there's no statistics entry for global-log-compaction, then it's not a non-transactional
    // database adapter, so no global-log to compact.
    assumeThat(statistics).containsKey("compactGlobalLog");

    // An "empty repository" should not require compaction
    assertThat(statistics)
        .containsKey("compactGlobalLog")
        .extracting("compactGlobalLog", InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry("compacted", "false");

    BranchName branch = BranchName.of("compactGlobalLog");
    List<ContentId> contentIds = new ArrayList<>();
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    Map<ContentId, ByteString> currentGlobal = new HashMap<>();

    databaseAdapter.create(branch, databaseAdapter.noAncestorHash());

    Runnable verify =
        () -> {
          try (Stream<ContentIdAndBytes> globals =
              databaseAdapter.globalContent(new HashSet<>(contentIds))) {
            assertThat(globals)
                .hasSize(contentIds.size())
                .allSatisfy(
                    cb ->
                        assertThat(currentGlobal.get(cb.getContentId())).isEqualTo(cb.getValue()));
          }
        };

    for (int i = 0; i < commits; i++) {
      commitForGlobalLogCompaction(
          commits, contentReuseProbability, branch, contentIds, rand, currentGlobal, i);
    }

    // Verify
    verify.run();

    statistics = databaseAdapter.repoMaintenance();

    assertThat(statistics)
        .containsKey("compactGlobalLog")
        .extracting("compactGlobalLog", InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry("compacted", "true")
        .containsEntry("entries.puts", Long.toString(commits))
        .containsEntry("entries.uniquePuts", Long.toString(contentIds.size()))
        .containsEntry("entries.read", Long.toString(commits + 2))
        .containsEntry("entries.read.total", Long.toString(commits + 2));

    // Verify again
    verify.run();

    // Compact again, compaction must not run, because there is at least one compacted
    // global-log-entry in the first page (above only added 5 "uncompacted" global-log-entries).

    statistics = databaseAdapter.repoMaintenance();

    assertThat(statistics)
        .containsKey("compactGlobalLog")
        .extracting("compactGlobalLog", InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry("compacted", "false");

    // Add some more commits, but not enough to trigger compaction

    int additionalCommits = 5;
    for (int i = 0; i < additionalCommits; i++) {
      commitForGlobalLogCompaction(
          commits + additionalCommits,
          contentReuseProbability,
          branch,
          contentIds,
          rand,
          currentGlobal,
          i + commits);
    }

    // Compact again, compaction must not run, because there is at least one compacted
    // global-log-entry in the first page (above only added 5 "uncompacted" global-log-entries).

    statistics = databaseAdapter.repoMaintenance();

    assertThat(statistics)
        .containsKey("compactGlobalLog")
        .extracting("compactGlobalLog", InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry("compacted", "false");

    // Add some more commits, enough to trigger compaction again

    int additionalCommits2 = 15;
    for (int i = 0; i < additionalCommits2; i++) {
      commitForGlobalLogCompaction(
          commits + additionalCommits + additionalCommits2,
          contentReuseProbability,
          branch,
          contentIds,
          rand,
          currentGlobal,
          i + commits + additionalCommits);
    }

    // Compact again, compaction must run, because there is no compacted global-log-entry in the
    // first page of the global log.

    statistics = databaseAdapter.repoMaintenance();

    assertThat(statistics)
        .containsKey("compactGlobalLog")
        .extracting("compactGlobalLog", InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry("compacted", "true")
        .containsEntry("entries.uniquePuts", Long.toString(contentIds.size()));

    // Verify again
    verify.run();
  }

  private void commitForGlobalLogCompaction(
      int commits,
      double contentReuseProbability,
      BranchName branch,
      List<ContentId> contentIds,
      ThreadLocalRandom rand,
      Map<ContentId, ByteString> currentGlobal,
      int i)
      throws ReferenceConflictException, ReferenceNotFoundException {
    ContentId contentId;
    if (contentReuseProbability > rand.nextDouble() && !contentIds.isEmpty()) {
      contentId = contentIds.get(rand.nextInt(contentIds.size()));
    } else {
      contentId = ContentId.of("cid-" + i);
      contentIds.add(contentId);
    }

    Key key = Key.of("commit", Integer.toString(i));
    WithGlobalStateContent c =
        WithGlobalStateContent.withGlobal(
            "state for #" + i + " of " + commits,
            "value for #" + i + " of " + commits,
            contentId.getId());
    byte payload = SimpleStoreWorker.INSTANCE.getPayload(c);
    ByteString global = SimpleStoreWorker.INSTANCE.toStoreGlobalState(c);

    ImmutableCommitAttempt.Builder commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("commit#" + i))
            .addPuts(
                KeyWithBytes.of(
                    key, contentId, payload, SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(c)))
            .putGlobal(contentId, global);

    currentGlobal.put(contentId, global);

    databaseAdapter.commit(commit.build());
  }
}
