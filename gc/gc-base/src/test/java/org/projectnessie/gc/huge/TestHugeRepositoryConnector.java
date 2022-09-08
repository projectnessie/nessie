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
package org.projectnessie.gc.huge;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

/**
 * Tests {@link HugeRepositoryConnector} (code for tests), not anything in the production code base.
 */
@SuppressWarnings("resource")
public class TestHugeRepositoryConnector {

  @Test
  void commitIdEncoding() {
    HugeRepositoryConnector objects = new HugeRepositoryConnector(5, 100, 3, 5);
    assertThat(objects.commitId(5, 600)).isEqualTo("%08x%08x", 5, 600);
    assertThat(objects.commitId(5, 501)).isEqualTo("%08x%08x", 5, 501);
    assertThat(objects.commitId(5, 500)).isEqualTo("%08x%08x", 0, 500);
    assertThat(objects.commitId(5, 1)).isEqualTo("%08x%08x", 0, 1);
  }

  @Test
  void references() throws Exception {
    assertThat(new HugeRepositoryConnector(1, 100, 3, 5).allReferences())
        .containsExactlyInAnyOrder(
            Branch.of("main", String.format("%08x%08x", 0, 300)),
            Branch.of("branch-1", String.format("%08x%08x", 1, 200)));
    assertThat(new HugeRepositoryConnector(5, 100, 3, 5).allReferences())
        .containsExactlyInAnyOrder(
            Branch.of("main", String.format("%08x%08x", 0, 700)),
            Branch.of("branch-1", String.format("%08x%08x", 1, 200)),
            Branch.of("branch-2", String.format("%08x%08x", 2, 300)),
            Branch.of("branch-3", String.format("%08x%08x", 3, 400)),
            Branch.of("branch-4", String.format("%08x%08x", 4, 500)),
            Branch.of("branch-5", String.format("%08x%08x", 5, 600)));
  }

  @Test
  void commitLog() throws Exception {
    HugeRepositoryConnector objects = new HugeRepositoryConnector(5, 100, 3, 5);
    assertThat(objects.commitLog(Branch.of("main", objects.commitId(0, 5))))
        .extracting(e -> e.getCommitMeta().getHash(), LogEntry::getParentCommitHash)
        .containsExactly(
            tuple(objects.commitId(0, 5), objects.commitId(0, 4)),
            tuple(objects.commitId(0, 4), objects.commitId(0, 3)),
            tuple(objects.commitId(0, 3), objects.commitId(0, 2)),
            tuple(objects.commitId(0, 2), objects.commitId(0, 1)),
            tuple(objects.commitId(0, 1), objects.commitId(0, 0)));

    assertThat(objects.commitLog(Branch.of("branch-5", objects.commitId(5, 600))))
        .extracting(e -> e.getCommitMeta().getHash(), LogEntry::getParentCommitHash)
        .containsExactlyElementsOf(
            IntStream.range(0, 600)
                .map(c -> 600 - c)
                .mapToObj(c -> tuple(objects.commitId(5, c), objects.commitId(5, c - 1)))
                .collect(Collectors.toList()));
  }

  static Stream<Arguments> verifyContentId() {
    return Stream.of(
        arguments(0, 1, "main-1", 0),
        arguments(0, 19, "main-19", 0),
        arguments(0, 20, "shared-1-0", 0),
        arguments(0, 21, "shared-1-1", 0),
        arguments(0, 24, "shared-3-0", 0),
        arguments(0, 25, "shared-3-1", 0),
        arguments(0, 28, "shared-5-0", 0),
        arguments(0, 29, "shared-5-1", 0),
        arguments(0, 30, "main-0", 1),
        arguments(0, 31, "main-1", 1),
        arguments(0, 49, "main-19", 1),
        arguments(0, 59, "shared-5-1", 1),
        arguments(0, 60, "main-0", 2),
        arguments(0, 61, "main-1", 2),
        arguments(0, 79, "main-19", 2),
        arguments(0, 80, "shared-1-0", 2),
        arguments(0, 81, "shared-1-1", 2),
        arguments(0, 84, "shared-3-0", 2),
        arguments(0, 85, "shared-3-1", 2),
        arguments(0, 88, "shared-5-0", 2),
        arguments(0, 89, "shared-5-1", 2),
        arguments(0, 110, null, -1),
        arguments(0, 111, null, -1),
        arguments(0, 112, "shared-2-0", 3),
        arguments(0, 113, "shared-2-1", 3),
        arguments(0, 114, "shared-3-0", 3),
        arguments(0, 115, "shared-3-1", 3),
        arguments(0, 230, null, -1),
        arguments(0, 231, null, -1),
        arguments(0, 232, null, -1),
        arguments(0, 233, null, -1),
        arguments(0, 234, "shared-3-0", 7),
        arguments(0, 235, "shared-3-1", 7));
  }

  @ParameterizedTest
  @MethodSource("verifyContentId")
  void verifyContentId(int refNum, int commitNum, String contentId, int snapshotId) {
    HugeRepositoryConnector objects = new HugeRepositoryConnector(5, 100, 3, 5);
    assertThat(objects.contentIdForCommit(refNum, commitNum)).isEqualTo(contentId);
    if (snapshotId >= 0) {
      assertThat(objects.modCountForCommit(refNum, commitNum)).isEqualTo(snapshotId);
    }
  }

  @Test
  void verifyContentSnapshotUniquenessAcrossReferences() {
    HugeRepositoryConnector objects = new HugeRepositoryConnector(5, 100, 3, 5);

    Set<Content.Type> types = singleton(ICEBERG_TABLE);

    Map<String, Map<Long, String>> contentIds = new HashMap<>();

    Map<String, Set<IcebergTable>> commitsAndContents = new HashMap<>();

    objects
        .allReferences()
        .flatMap(objects::commitLog)
        .forEach(
            logEntry -> {
              List<Operation> ops = logEntry.getOperations();
              if (ops != null) {
                for (Operation operation : ops) {
                  if (operation instanceof Operation.Put) {
                    Operation.Put put = (Operation.Put) operation;
                    IcebergTable table = (IcebergTable) put.getContent();
                    String contentId = table.getId();
                    long snapshotId = table.getSnapshotId();

                    commitsAndContents
                        .computeIfAbsent(logEntry.getCommitMeta().getHash(), x -> new HashSet<>())
                        .add(table);

                    assertThat(
                            contentIds
                                .computeIfAbsent(contentId, c -> new HashMap<>())
                                .put(snapshotId, logEntry.getCommitMeta().getHash()))
                        .matches(
                            v -> v == null || v.equals(logEntry.getCommitMeta().getHash()),
                            logEntry.getCommitMeta().getHash());

                    assertThat(
                            objects
                                .allContents(Detached.of(logEntry.getCommitMeta().getHash()), types)
                                .filter(e -> e.getKey().equals(put.getKey()))
                                .map(Map.Entry::getValue)
                                .findFirst())
                        .isEqualTo(Optional.of(table));
                  }
                }
              }
            });
  }
}
