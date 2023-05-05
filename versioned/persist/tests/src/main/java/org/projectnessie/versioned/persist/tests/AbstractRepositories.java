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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.testworker.OnRefOnly;

/** Verifies handling of repo-description in the database-adapters. */
public abstract class AbstractRepositories {

  protected AbstractRepositories() {}

  @Test
  void nonExistentRepository(
      @NessieDbAdapter(initializeRepo = false)
          @NessieDbAdapterConfigItem(name = "repository.id", value = "non-existent")
          DatabaseAdapter nonExistent) {
    assertAll(
        () -> {
          try (Stream<ReferenceInfo<ByteString>> r =
              nonExistent.namedRefs(GetNamedRefsParams.DEFAULT)) {
            assertThat(r).isEmpty();
          }
        },
        () ->
            assertThatThrownBy(
                    () -> nonExistent.hashOnReference(BranchName.of("main"), Optional.empty()))
                .isInstanceOf(ReferenceNotFoundException.class),
        () ->
            assertThatThrownBy(() -> nonExistent.namedRef("main", GetNamedRefsParams.DEFAULT))
                .isInstanceOf(ReferenceNotFoundException.class));
  }

  @Test
  void multipleRepositories(
      @NessieDbAdapter @NessieDbAdapterConfigItem(name = "repository.id", value = "foo")
          DatabaseAdapter foo,
      @NessieDbAdapter @NessieDbAdapterConfigItem(name = "repository.id", value = "bar")
          DatabaseAdapter bar)
      throws Exception {

    BranchName main = BranchName.of("main");
    BranchName fooBranchName = BranchName.of("foo-branch");
    BranchName barBranchName = BranchName.of("bar-branch");
    ByteString fooCommitMeta = ByteString.copyFromUtf8("meta-foo");
    ByteString barCommitMeta = ByteString.copyFromUtf8("meta-bar");

    OnRefOnly fooValue = onRef("foo", "foo");
    OnRefOnly barValue = onRef("bar", "bar");
    foo.commit(
        ImmutableCommitParams.builder()
            .toBranch(main)
            .commitMetaSerialized(fooCommitMeta)
            .addPuts(
                KeyWithBytes.of(
                    ContentKey.of("foo"),
                    ContentId.of(fooValue.getId()),
                    (byte) payloadForContent(fooValue),
                    fooValue.serialized()))
            .build());
    bar.commit(
        ImmutableCommitParams.builder()
            .toBranch(main)
            .commitMetaSerialized(barCommitMeta)
            .addPuts(
                KeyWithBytes.of(
                    ContentKey.of("bar"),
                    ContentId.of(barValue.getId()),
                    (byte) payloadForContent(barValue),
                    barValue.serialized()))
            .build());

    Hash fooMain = foo.hashOnReference(main, Optional.empty());
    Hash barMain = bar.hashOnReference(main, Optional.empty());

    Hash fooBranch = foo.create(fooBranchName, fooMain).getHash();
    Hash barBranch = bar.create(barBranchName, barMain).getHash();

    assertThat(fooMain).isNotEqualTo(barMain).isEqualTo(fooBranch);
    assertThat(barMain).isNotEqualTo(fooMain).isEqualTo(barBranch);

    // Verify that key-prefix "foo" only sees "its" main-branch and foo-branch
    try (Stream<ReferenceInfo<ByteString>> refs = foo.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs)
          .containsExactlyInAnyOrder(
              ReferenceInfo.of(fooMain, main), ReferenceInfo.of(fooBranch, fooBranchName));
    }
    try (Stream<ReferenceInfo<ByteString>> refs = bar.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs)
          .containsExactlyInAnyOrder(
              ReferenceInfo.of(barMain, main), ReferenceInfo.of(barBranch, barBranchName));
    }
    assertThatThrownBy(() -> foo.commitLog(barBranch))
        .isInstanceOf(ReferenceNotFoundException.class);
    assertThatThrownBy(() -> bar.commitLog(fooBranch))
        .isInstanceOf(ReferenceNotFoundException.class);

    try (Stream<CommitLogEntry> log = foo.commitLog(fooBranch)) {
      assertThat(log.collect(Collectors.toList()))
          .extracting(CommitLogEntry::getMetadata)
          .containsExactlyInAnyOrder(fooCommitMeta);
    }
    try (Stream<CommitLogEntry> log = bar.commitLog(barBranch)) {
      assertThat(log.collect(Collectors.toList()))
          .extracting(CommitLogEntry::getMetadata)
          .containsExactlyInAnyOrder(barCommitMeta);
    }
  }
}
