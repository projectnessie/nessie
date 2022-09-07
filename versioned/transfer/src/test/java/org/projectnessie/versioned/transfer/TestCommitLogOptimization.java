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

import static java.lang.Integer.parseInt;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.versioned.transfer.ExportImportTestUtil.expectedParents;
import static org.projectnessie.versioned.transfer.ExportImportTestUtil.intToHash;
import static org.projectnessie.versioned.transfer.ExportImportTestUtil.toCommitLogEntry;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

public class TestCommitLogOptimization {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 5, 10, 19, 20, 21, 30, 50, 100})
  public void singleBranch(int numCommits) {
    Map<Hash, CommitLogEntry> allCommits =
        IntStream.range(0, numCommits)
            .mapToObj(ExportImportTestUtil::toCommitLogEntry)
            .collect(toMap(CommitLogEntry::getHash, identity()));

    verify(numCommits, allCommits);
  }

  static Stream<Arguments> multipleBranches() {
    return IntStream.of(1, 3, 5, 10)
        .boxed()
        .flatMap(
            branches ->
                Stream.of(
                    arguments(branches, 1),
                    arguments(branches, 5),
                    arguments(branches, 10),
                    arguments(branches, 19),
                    arguments(branches, 20),
                    arguments(branches, 21),
                    arguments(branches, 30),
                    arguments(branches, 50)));
  }

  @ParameterizedTest
  @MethodSource("multipleBranches")
  public void multipleBranches(int branches, int commits) {
    int commitsOnMain = (branches + 2) * commits;

    IntStream ints =
        IntStream.concat(
            IntStream.range(0, commitsOnMain),
            IntStream.rangeClosed(1, branches)
                .flatMap(branch -> IntStream.range(0, commits).map(c -> (branch << 16) | c)));

    Map<Hash, CommitLogEntry> allCommits =
        ints.mapToObj(i -> toCommitLogEntry(i, commits))
            .collect(toMap(CommitLogEntry::getHash, identity()));

    verify(commits, allCommits);
  }

  protected void verify(int commits, Map<Hash, CommitLogEntry> allCommits) {
    int parentsPerCommit = 20;
    DatabaseAdapterConfig config = mock(DatabaseAdapterConfig.class);
    when(config.getAssumedWallClockDriftMicros()).thenReturn(0L);
    when(config.getParentsPerCommit()).thenReturn(parentsPerCommit);
    when(config.getKeyListDistance()).thenReturn(parentsPerCommit);
    when(config.currentTimeInMicros()).thenReturn(0L);

    Map<Hash, CommitLogEntry> updated = new HashMap<>();

    DatabaseAdapter databaseAdapter =
        (DatabaseAdapter)
            Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class[] {DatabaseAdapter.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "getConfig":
                      return config;
                    case "scanAllCommitLogEntries":
                      return allCommits.values().stream();
                    case "fetchCommitLogEntries":
                      {
                        @SuppressWarnings("unchecked")
                        Stream<Hash> hashes = (Stream<Hash>) args[0];
                        return hashes.map(
                            h -> {
                              CommitLogEntry e = updated.get(h);
                              if (e == null) {
                                e = allCommits.get(h);
                              }
                              return e;
                            });
                      }
                    case "commitLog":
                      {
                        List<CommitLogEntry> log = new ArrayList<>();
                        for (Hash head = (Hash) args[0]; ; ) {
                          CommitLogEntry c = allCommits.get(head);
                          if (c == null) {
                            break;
                          }
                          log.add(c);
                          head = c.getParents().get(0);
                        }
                        return log.stream();
                      }
                    case "writeMultipleCommits":
                      fail();
                      return null;
                    case "updateMultipleCommits":
                      {
                        @SuppressWarnings("unchecked")
                        List<CommitLogEntry> entries = (List<CommitLogEntry>) args[0];
                        entries.forEach(c -> updated.put(c.getHash(), c));
                        return null;
                      }
                    case "rebuildKeyList":
                      {
                        return (CommitLogEntry) args[0];
                      }
                    case "noAncestorHash":
                      return intToHash(-1);
                    default:
                      throw new UnsupportedOperationException(method.toString());
                  }
                });

    CommitLogOptimization opt =
        CommitLogOptimization.builder()
            .totalCommitCount(200)
            .databaseAdapter(databaseAdapter)
            .build();

    opt.optimize();

    Map<Hash, List<Hash>> expected =
        allCommits.keySet().stream()
            .mapToInt(id -> parseInt(id.asString(), 16))
            .boxed()
            .collect(
                toMap(
                    ExportImportTestUtil::intToHash,
                    i -> expectedParents(i, commits, parentsPerCommit)));

    Map<Hash, List<Hash>> updatedParents =
        updated.values().stream()
            .collect(toMap(CommitLogEntry::getHash, CommitLogEntry::getParents));

    assertThat(updatedParents).containsAllEntriesOf(expected).hasSize(expected.size());
  }
}
