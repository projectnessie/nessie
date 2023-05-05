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

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

/** Verifies handling of repo-description in the database-adapters. */
public abstract class AbstractDiff {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractDiff(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  void diff() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");

    Hash initialHash =
        databaseAdapter
            .create(branch, databaseAdapter.hashOnReference(main, Optional.empty()))
            .getHash();

    Hash[] commits = new Hash[3];
    for (int i = 0; i < commits.length; i++) {
      ImmutableCommitParams.Builder commit =
          ImmutableCommitParams.builder()
              .toBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit " + i));
      for (int k = 0; k < 3; k++) {
        OnRefOnly c = OnRefOnly.onRef("on-ref " + i + " for " + k, "cid-" + i + "-" + k);
        commit.addPuts(
            KeyWithBytes.of(
                ContentKey.of("key-" + k),
                ContentId.of("C" + k),
                (byte) payloadForContent(c),
                DefaultStoreWorker.instance().toStoreOnReferenceState(c)));
      }
      commits[i] = databaseAdapter.commit(commit.build()).getCommitHash();
    }

    try (Stream<Difference> diff =
        databaseAdapter.diff(
            databaseAdapter.hashOnReference(main, Optional.empty()),
            databaseAdapter.hashOnReference(branch, Optional.of(initialHash)),
            KeyFilterPredicate.ALLOW_ALL)) {
      assertThat(diff).isEmpty();
    }

    for (int i = 0; i < commits.length; i++) {
      try (Stream<Difference> diff =
          databaseAdapter.diff(
              databaseAdapter.hashOnReference(main, Optional.empty()),
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              KeyFilterPredicate.ALLOW_ALL)) {
        int c = i;
        assertThat(diff)
            .containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 3)
                    .mapToObj(
                        k -> {
                          OnRefOnly content =
                              OnRefOnly.onRef("on-ref " + c + " for " + k, "cid-" + c + "-" + k);
                          return Difference.of(
                              (byte) payloadForContent(content),
                              ContentKey.of("key-" + k),
                              Optional.empty(),
                              Optional.empty(),
                              Optional.of(
                                  DefaultStoreWorker.instance().toStoreOnReferenceState(content)));
                        })
                    .collect(Collectors.toList()));
      }
    }

    for (int i = 0; i < commits.length; i++) {
      try (Stream<Difference> diff =
          databaseAdapter.diff(
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              databaseAdapter.hashOnReference(main, Optional.empty()),
              KeyFilterPredicate.ALLOW_ALL)) {
        int c = i;
        assertThat(diff)
            .containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 3)
                    .mapToObj(
                        k -> {
                          OnRefOnly content =
                              OnRefOnly.onRef("on-ref " + c + " for " + k, "cid-" + c + "-" + k);
                          return Difference.of(
                              (byte) payloadForContent(content),
                              ContentKey.of("key-" + k),
                              Optional.empty(),
                              Optional.of(
                                  DefaultStoreWorker.instance().toStoreOnReferenceState(content)),
                              Optional.empty());
                        })
                    .collect(Collectors.toList()));
      }
    }

    for (int i = 1; i < commits.length; i++) {
      try (Stream<Difference> diff =
          databaseAdapter.diff(
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i - 1])),
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              KeyFilterPredicate.ALLOW_ALL)) {
        int c = i;
        assertThat(diff)
            .containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 3)
                    .mapToObj(
                        k -> {
                          OnRefOnly from =
                              OnRefOnly.onRef(
                                  "on-ref " + (c - 1) + " for " + k, "cid-" + (c - 1) + "-" + k);
                          OnRefOnly to =
                              OnRefOnly.onRef("on-ref " + c + " for " + k, "cid-" + c + "-" + k);
                          return Difference.of(
                              (byte) payloadForContent(from),
                              ContentKey.of("key-" + k),
                              Optional.empty(),
                              Optional.of(
                                  DefaultStoreWorker.instance().toStoreOnReferenceState(from)),
                              Optional.of(
                                  DefaultStoreWorker.instance().toStoreOnReferenceState(to)));
                        })
                    .collect(Collectors.toList()));
      }
    }
  }
}
