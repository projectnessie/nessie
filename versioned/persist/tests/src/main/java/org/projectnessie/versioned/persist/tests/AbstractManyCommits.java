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
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import com.google.common.collect.Maps;
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
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

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
  @ValueSource(ints = {0, 1, 19, 20, 21, 49, 50, 51})
  // Note: 1000 commits is quite the max that in-JVM H2 database can handle
  void manyCommits(int numCommits) throws Exception {
    BranchName branch = BranchName.of("manyCommits-" + numCommits);
    databaseAdapter.create(
        branch, databaseAdapter.hashOnReference(BranchName.of("main"), Optional.empty()));

    Hash[] commits = new Hash[numCommits];

    ContentId fixed = ContentId.of("FIXED");

    for (int i = 0; i < numCommits; i++) {
      ContentKey key = ContentKey.of("many-commits-" + numCommits);
      OnRefOnly c = OnRefOnly.onRef("value for #" + i + " of " + numCommits, fixed.getId());
      byte payload = (byte) payloadForContent(c);
      ImmutableCommitParams.Builder commit =
          ImmutableCommitParams.builder()
              .toBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i + " of " + numCommits))
              .addPuts(
                  KeyWithBytes.of(
                      key,
                      fixed,
                      payload,
                      DefaultStoreWorker.instance().toStoreOnReferenceState(c)));
      Hash hash = databaseAdapter.commit(commit.build()).getCommitHash();
      commits[i] = hash;
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
    ContentKey key = ContentKey.of("many-commits-" + numCommits);

    try {
      commit = databaseAdapter.hashOnReference(branch, Optional.of(commit));
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    try {
      Map<ContentKey, ContentAndState> values =
          databaseAdapter.values(
              commit, Collections.singletonList(key), KeyFilterPredicate.ALLOW_ALL);

      OnRefOnly expected =
          OnRefOnly.onRef("value for #" + i + " of " + numCommits, contentId.getId());

      ByteString expectValue = DefaultStoreWorker.instance().toStoreOnReferenceState(expected);
      ContentAndState expect = ContentAndState.of((byte) payloadForContent(expected), expectValue);
      assertThat(values).containsExactly(Maps.immutableEntry(key, expect));
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    try (Stream<KeyListEntry> keys = databaseAdapter.keys(commit, KeyFilterPredicate.ALLOW_ALL)) {
      assertThat(keys.map(KeyListEntry::getKey)).containsExactly(key);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
