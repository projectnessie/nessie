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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

/**
 * Verifies that a big-ish number of keys, split across multiple commits works and the correct
 * results are returned for the commit-log, keys, global-states. This test is especially useful to
 * verify that the embedded and nested key-lists (think: full-key-lists in a commit-log-entry) work
 * correctly.
 */
public abstract class AbstractManyKeys {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractManyKeys(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  static class ManyKeysParams {
    final int keys;
    final int commits;

    public ManyKeysParams(int keys, int commits) {
      this.keys = keys;
      this.commits = commits;
    }

    @Override
    public String toString() {
      return "keys=" + keys + ", commits=" + commits;
    }
  }

  static List<ManyKeysParams> manyKeysParams() {
    return Arrays.asList(
        // quite slow for a unit-test
        // new ManyKeysParams(20000, 25),
        // new ManyKeysParams(20000, 100),
        new ManyKeysParams(250, 25),
        new ManyKeysParams(1000, 25),
        new ManyKeysParams(1000, 100),
        new ManyKeysParams(5000, 25),
        new ManyKeysParams(5000, 100));
  }

  @ParameterizedTest
  @MethodSource("manyKeysParams")
  void manyKeys(ManyKeysParams params) throws Exception {
    BranchName main = BranchName.of("main");

    List<ImmutableCommitAttempt.Builder> commits =
        IntStream.range(0, params.commits)
            .mapToObj(
                i ->
                    ImmutableCommitAttempt.builder()
                        .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i))
                        .commitToBranch(main))
            .collect(Collectors.toList());
    AtomicInteger commitDist = new AtomicInteger();

    Set<Key> allKeys = new HashSet<>();

    IntStream.range(0, params.keys)
        .mapToObj(
            i -> {
              Key key =
                  Key.of(
                      "some",
                      Integer.toString(i),
                      "long",
                      "key",
                      "value",
                      "foobarbazfoobarbazfoobarbazfoobarbazfoobarbazfoobarbaz");
              allKeys.add(key);
              return KeyWithBytes.of(
                  key, ContentsId.of("cid-" + i), (byte) 0, ByteString.copyFromUtf8("value " + i));
            })
        .forEach(kb -> commits.get(commitDist.incrementAndGet() % params.commits).addPuts(kb));

    for (ImmutableCommitAttempt.Builder commit : commits) {
      databaseAdapter.commit(commit.build());
    }

    Hash mainHead = databaseAdapter.toHash(main);
    try (Stream<KeyWithType> keys = databaseAdapter.keys(mainHead, KeyFilterPredicate.ALLOW_ALL)) {
      List<Key> fetchedKeys = keys.map(KeyWithType::getKey).collect(Collectors.toList());

      // containsExactlyInAnyOrderElementsOf() is quite expensive and slow with Key's
      // implementation of 'Key.equals()' since it uses a collator.
      List<String> fetchedKeysStrings =
          fetchedKeys.stream().map(Key::toString).collect(Collectors.toList());
      List<String> allKeysStrings =
          allKeys.stream().map(Key::toString).collect(Collectors.toList());

      assertThat(fetchedKeysStrings)
          .hasSize(allKeysStrings.size())
          .containsExactlyInAnyOrderElementsOf(allKeysStrings);
    }
  }
}
