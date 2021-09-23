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

import com.google.protobuf.ByteString;
import java.util.Collections;
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
import org.projectnessie.versioned.persist.adapter.ContentsAndState;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.ContentsIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

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
    databaseAdapter.create(branch, databaseAdapter.toHash(BranchName.of("main")));

    Hash[] commits = new Hash[numCommits];

    ContentsId fixed = ContentsId.of("FIXED");

    for (int i = 0; i < numCommits; i++) {
      Key key = Key.of("many", "commits", Integer.toString(numCommits));
      ImmutableCommitAttempt.Builder commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i + " of " + numCommits))
              .addPuts(
                  KeyWithBytes.of(
                      key,
                      fixed,
                      (byte) 0,
                      ByteString.copyFromUtf8("value for #" + i + " of " + numCommits)))
              .putGlobal(fixed, ByteString.copyFromUtf8("state for #" + i + " of " + numCommits));
      if (i > 0) {
        commit.putExpectedStates(
            fixed,
            Optional.of(ByteString.copyFromUtf8("state for #" + (i - 1) + " of " + numCommits)));
      }
      Hash hash = databaseAdapter.commit(commit.build());
      commits[i] = hash;

      try (Stream<ContentsIdAndBytes> globals =
          databaseAdapter.globalContents(Collections.singleton(fixed), bs -> (byte) 0)) {
        assertThat(globals)
            .containsExactly(
                ContentsIdAndBytes.of(
                    fixed,
                    (byte) 0,
                    ByteString.copyFromUtf8("state for #" + i + " of " + numCommits)));
      }
    }

    try (Stream<CommitLogEntry> log = databaseAdapter.commitLog(databaseAdapter.toHash(branch))) {
      assertThat(log.count()).isEqualTo(numCommits);
    }

    ExecutorService executor =
        Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()));
    try {
      CompletableFuture<Void> combinedFuture =
          CompletableFuture.allOf(
              IntStream.range(0, numCommits)
                  .mapToObj(i -> (Runnable) () -> verify(i, numCommits, branch, commits[i]))
                  .map(r -> CompletableFuture.runAsync(r, executor))
                  .toArray((IntFunction<CompletableFuture<?>[]>) CompletableFuture[]::new));

      combinedFuture.get();

    } finally {
      executor.shutdown();
    }

    databaseAdapter.delete(branch, Optional.empty());
  }

  private void verify(int i, int numCommits, BranchName branch, Hash commit) {
    Key key = Key.of("many", "commits", Integer.toString(numCommits));

    try {
      commit = databaseAdapter.hashOnReference(branch, Optional.of(commit));
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    try (Stream<Optional<ContentsAndState<ByteString>>> x =
        databaseAdapter.values(
            commit, Collections.singletonList(key), KeyFilterPredicate.ALLOW_ALL)) {
      ByteString expectValue = ByteString.copyFromUtf8("value for #" + i + " of " + numCommits);
      ByteString expectState =
          ByteString.copyFromUtf8("state for #" + (numCommits - 1) + " of " + numCommits);
      ContentsAndState<ByteString> expect = ContentsAndState.of(expectValue, expectState);
      assertThat(x).containsExactly(Optional.of(expect));
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
