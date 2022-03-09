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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Key;
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
    return Stream.of(
        Arguments.of(10, 0d),
        Arguments.of(10, .1d),
        Arguments.of(10, .9d),
        Arguments.of(100, 0d),
        Arguments.of(100, .1d),
        Arguments.of(100, .9d));
  }

  @ParameterizedTest
  @MethodSource("compactGlobalLog")
  public void compactGlobalLog(int commits, double contentReuseProbability) throws Exception {
    // sneak peak, whether the database-adapter supports global-log-compaction
    assumeThat(databaseAdapter.repoMaintenance()).containsKey("compactGlobalLog");

    BranchName branch = BranchName.of("compactGlobalLog");
    List<ContentId> contentIds = new ArrayList<>();
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    Map<ContentId, ByteString> currentGlobal = new HashMap<>();

    databaseAdapter.create(branch, databaseAdapter.noAncestorHash());

    for (int i = 0; i < commits; i++) {
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
                      key,
                      contentId,
                      payload,
                      SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(c)))
              .putGlobal(contentId, global);

      currentGlobal.put(contentId, global);

      databaseAdapter.commit(commit.build());
    }

    // Verify
    try (Stream<ContentIdAndBytes> globals =
        databaseAdapter.globalContent(new HashSet<>(contentIds))) {
      assertThat(globals)
          .hasSize(contentIds.size())
          .allSatisfy(
              cb -> assertThat(currentGlobal.get(cb.getContentId())).isEqualTo(cb.getValue()));
    }

    Map<String, Map<String, String>> statistics = databaseAdapter.repoMaintenance();
    Map<String, String> compactStats = statistics.get("compactGlobalLog");

    assertThat(compactStats)
        .containsEntry("entries.puts", Long.toString(commits))
        .containsEntry("entries.uniquePuts", Long.toString(contentIds.size()))
        .containsEntry("entries.read", Long.toString(commits + 2))
        .containsEntry("entries.read.total", Long.toString(commits + 2));

    // Verify again
    try (Stream<ContentIdAndBytes> globals =
        databaseAdapter.globalContent(new HashSet<>(contentIds))) {
      assertThat(globals)
          .hasSize(contentIds.size())
          .allSatisfy(
              cb -> assertThat(currentGlobal.get(cb.getContentId())).isEqualTo(cb.getValue()));
    }
  }
}
