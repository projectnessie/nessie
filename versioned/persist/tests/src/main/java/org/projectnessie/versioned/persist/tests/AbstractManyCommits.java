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

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.testworker.SimpleStoreWorker;
import org.projectnessie.versioned.testworker.WithGlobalStateContent;

/**
 * Rather rudimentary test that verifies that multiple commits in a row work and the correct results
 * are returned for the commit-log, keys, global-states.
 */
public abstract class AbstractManyCommits {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractManyCommits(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 19, 20, 21, 39, 40, 41, 49, 50, 51, 255, 256, 257, 500})
  // Note: 1000 commits is quite the max that in-JVM H2 database can handle
  void manyCommits(int numCommits) throws Exception {
    BranchName branch = BranchName.of("manyCommits-" + numCommits);
    databaseAdapter.create(
        branch, databaseAdapter.hashOnReference(BranchName.of("main"), Optional.empty()));

    Hash[] commits = new Hash[numCommits];

    ContentId fixed = ContentId.of("FIXED");

    for (int i = 0; i < numCommits; i++) {
      Key key = Key.of("many", "commits", Integer.toString(numCommits));
      WithGlobalStateContent c =
          WithGlobalStateContent.withGlobal(
              "state for #" + i + " of " + numCommits,
              "value for #" + i + " of " + numCommits,
              fixed.getId());
      byte payload = SimpleStoreWorker.INSTANCE.getPayload(c);
      ImmutableCommitAttempt.Builder commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i + " of " + numCommits))
              .addPuts(
                  KeyWithBytes.of(
                      key, fixed, payload, SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(c)))
              .putGlobal(fixed, SimpleStoreWorker.INSTANCE.toStoreGlobalState(c));
      if (i > 0) {
        WithGlobalStateContent expected =
            WithGlobalStateContent.withGlobal(
                "state for #" + (i - 1) + " of " + numCommits,
                "value for #" + (i - 1) + " of " + numCommits,
                fixed.getId());
        commit.putExpectedStates(
            fixed, Optional.of(SimpleStoreWorker.INSTANCE.toStoreGlobalState(expected)));
      }
      Hash hash = databaseAdapter.commit(commit.build());
      commits[i] = hash;

      try (Stream<ContentIdAndBytes> globals =
          databaseAdapter.globalContent(Collections.singleton(fixed), bs -> payload)) {

        WithGlobalStateContent expected =
            WithGlobalStateContent.withGlobal(
                "state for #" + i + " of " + numCommits,
                "value for #" + i + " of " + numCommits,
                fixed.getId());

        assertThat(globals)
            .containsExactly(
                ContentIdAndBytes.of(
                    fixed, payload, SimpleStoreWorker.INSTANCE.toStoreGlobalState(expected)));
      }
    }

    try (Stream<CommitLogEntry> log =
        databaseAdapter.commitLog(databaseAdapter.hashOnReference(branch, Optional.empty()))) {
      assertThat(log.count()).isEqualTo(numCommits);
    }

    ExecutorService executor =
        Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()));
    try {
      CompletableFuture<Void> combinedFuture =
          CompletableFuture.allOf(
              IntStream.range(0, numCommits)
                  .mapToObj(i -> (Runnable) () -> verify(i, numCommits, branch, commits[i], fixed))
                  .map(r -> CompletableFuture.runAsync(r, executor))
                  .toArray((IntFunction<CompletableFuture<?>[]>) CompletableFuture[]::new));

      combinedFuture.get();

    } finally {
      executor.shutdown();
    }

    databaseAdapter.delete(branch, Optional.empty());
  }

  private void verify(int i, int numCommits, BranchName branch, Hash commit, ContentId contentId) {
    Key key = Key.of("many", "commits", Integer.toString(numCommits));

    try {
      commit = databaseAdapter.hashOnReference(branch, Optional.of(commit));
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    try {
      Map<Key, ContentAndState<ByteString>> values =
          databaseAdapter.values(
              commit, Collections.singletonList(key), KeyFilterPredicate.ALLOW_ALL);

      WithGlobalStateContent expected =
          WithGlobalStateContent.withGlobal(
              "state for #" + (numCommits - 1) + " of " + numCommits,
              "value for #" + i + " of " + numCommits,
              contentId.getId());

      ByteString expectValue = SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(expected);
      ByteString expectState = SimpleStoreWorker.INSTANCE.toStoreGlobalState(expected);
      ContentAndState<ByteString> expect = ContentAndState.of(expectValue, expectState);
      assertThat(values).containsExactly(Maps.immutableEntry(key, expect));
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    try (Stream<KeyWithType> keys = databaseAdapter.keys(commit, KeyFilterPredicate.ALLOW_ALL)) {
      assertThat(keys.map(KeyWithType::getKey)).containsExactly(key);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
