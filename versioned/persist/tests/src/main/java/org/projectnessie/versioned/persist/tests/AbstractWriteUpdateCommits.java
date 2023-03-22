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
package org.projectnessie.versioned.persist.tests;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;

public abstract class AbstractWriteUpdateCommits {

  private final DatabaseAdapter databaseAdapter;
  private final boolean commitWritesValidated;

  protected AbstractWriteUpdateCommits(
      DatabaseAdapter databaseAdapter, boolean commitWritesValidated) {
    this.databaseAdapter = databaseAdapter;
    this.commitWritesValidated = commitWritesValidated;
  }

  Hash intAsHash(int i) {
    return Hash.of(String.format("%08x", i));
  }

  private CommitLogEntry helloCommit(int i) {
    return CommitLogEntry.of(
        i,
        intAsHash(i),
        i,
        singletonList(intAsHash(i - 1)),
        ByteString.copyFromUtf8("hello " + i),
        emptyList(),
        emptyList(),
        0,
        null,
        emptyList(),
        emptyList(),
        emptyList());
  }

  @Test
  void writeMultipleCommits() throws Exception {
    List<Hash> hashes =
        IntStream.rangeClosed(1, 10).mapToObj(this::intAsHash).collect(Collectors.toList());
    List<CommitLogEntry> commits =
        hashes.stream()
            .mapToInt(h -> Integer.parseInt(h.asString(), 16))
            .mapToObj(this::helloCommit)
            .collect(Collectors.toList());

    databaseAdapter.writeMultipleCommits(commits);

    try (Stream<CommitLogEntry> retrieved =
        databaseAdapter.fetchCommitLogEntries(hashes.stream())) {
      assertThat(retrieved).containsExactlyInAnyOrderElementsOf(commits);
    }

    List<CommitLogEntry> updatedCommits =
        commits.stream()
            .map(
                commit ->
                    ImmutableCommitLogEntry.builder()
                        .from(commit)
                        .metadata(ByteString.copyFromUtf8("updated " + commit.getHash().asString()))
                        .build())
            .collect(Collectors.toList());

    databaseAdapter.updateMultipleCommits(updatedCommits);

    try (Stream<CommitLogEntry> retrieved =
        databaseAdapter.fetchCommitLogEntries(hashes.stream())) {
      assertThat(retrieved).containsExactlyInAnyOrderElementsOf(updatedCommits);
    }
  }

  @Test
  void validateWriteUpdateCommitPreconditions() throws Exception {
    assumeThat(commitWritesValidated).isTrue();

    List<Hash> hashes =
        IntStream.rangeClosed(1, 10).mapToObj(this::intAsHash).collect(Collectors.toList());
    List<CommitLogEntry> commits =
        hashes.stream()
            .mapToInt(h -> Integer.parseInt(h.asString(), 16))
            .mapToObj(this::helloCommit)
            .collect(Collectors.toList());

    databaseAdapter.writeMultipleCommits(commits.subList(0, 2));
    assertThatThrownBy(() -> databaseAdapter.writeMultipleCommits(commits.subList(0, 2)))
        .isInstanceOf(ReferenceConflictException.class);

    assertThatThrownBy(() -> databaseAdapter.updateMultipleCommits(commits))
        .isInstanceOf(ReferenceNotFoundException.class);
  }
}
