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

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;

/**
 * Tests that verify {@link DatabaseAdapter} implementations. A few tests have similar pendants via
 * the tests against the {@code VersionStore}.
 */
@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterConfigItem(name = "max.key.list.size", value = "2048")
public abstract class AbstractDatabaseAdapterTest {
  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;

  @Nested
  public class GlobalStates extends AbstractGlobalStates {
    GlobalStates() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class CommitScenarios extends AbstractCommitScenarios {
    CommitScenarios() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class ManyCommits extends AbstractManyCommits {
    ManyCommits() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class ManyKeys extends AbstractManyKeys {
    ManyKeys() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class Concurrency extends AbstractConcurrency {
    Concurrency() {
      super(databaseAdapter);
    }
  }

  @Test
  void createBranch() throws Exception {
    BranchName create = BranchName.of("createBranch");
    createNamedRef(create, TagName.of(create.getName()));
  }

  @Test
  void createTag() throws Exception {
    TagName create = TagName.of("createTag");
    createNamedRef(create, BranchName.of(create.getName()));
  }

  private void createNamedRef(NamedRef create, NamedRef opposite) throws Exception {
    BranchName branch = BranchName.of("main");

    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.map(ReferenceInfo::getNamedRef)).containsExactlyInAnyOrder(branch);
    }

    Hash mainHash = databaseAdapter.hashOnReference(branch, Optional.empty());

    assertThatThrownBy(() -> databaseAdapter.hashOnReference(create, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    Hash createHash =
        databaseAdapter.create(create, databaseAdapter.hashOnReference(branch, Optional.empty()));
    assertThat(createHash).isEqualTo(mainHash);

    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.map(ReferenceInfo::getNamedRef)).containsExactlyInAnyOrder(branch, create);
    }

    assertThatThrownBy(
            () ->
                databaseAdapter.create(
                    create, databaseAdapter.hashOnReference(branch, Optional.empty())))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThat(databaseAdapter.hashOnReference(create, Optional.empty())).isEqualTo(createHash);
    assertThatThrownBy(() -> databaseAdapter.hashOnReference(opposite, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    assertThatThrownBy(
            () ->
                databaseAdapter.create(
                    BranchName.of(create.getName()),
                    databaseAdapter.hashOnReference(branch, Optional.empty())))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThatThrownBy(
            () -> databaseAdapter.delete(create, Optional.of(Hash.of("dead00004242fee18eef"))))
        .isInstanceOf(ReferenceConflictException.class);

    assertThatThrownBy(() -> databaseAdapter.delete(opposite, Optional.of(createHash)))
        .isInstanceOf(ReferenceNotFoundException.class);

    databaseAdapter.delete(create, Optional.of(createHash));

    assertThatThrownBy(() -> databaseAdapter.hashOnReference(create, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.map(ReferenceInfo::getNamedRef)).containsExactlyInAnyOrder(branch);
    }
  }

  @Test
  void verifyNotFoundAndConflictExceptionsForUnreachableCommit() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName unreachable = BranchName.of("unreachable");
    BranchName helper = BranchName.of("helper");

    databaseAdapter.create(unreachable, databaseAdapter.hashOnReference(main, Optional.empty()));
    Hash helperHead =
        databaseAdapter.create(helper, databaseAdapter.hashOnReference(main, Optional.empty()));

    Hash unreachableHead =
        databaseAdapter.commit(
            ImmutableCommitAttempt.builder()
                .commitToBranch(unreachable)
                .commitMetaSerialized(ByteString.copyFromUtf8("commit meta"))
                .addPuts(
                    KeyWithBytes.of(
                        Key.of("foo"),
                        ContentId.of("contentId"),
                        (byte) 0,
                        ByteString.copyFromUtf8("hello")))
                .build());

    assertAll(
        () ->
            assertThatThrownBy(
                    () -> databaseAdapter.hashOnReference(main, Optional.of(unreachableHead)))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage(
                    String.format(
                        "Could not find commit '%s' in reference '%s'.",
                        unreachableHead.asString(), main.getName())),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.commit(
                            ImmutableCommitAttempt.builder()
                                .commitToBranch(helper)
                                .expectedHead(Optional.of(unreachableHead))
                                .commitMetaSerialized(ByteString.copyFromUtf8("commit meta"))
                                .addPuts(
                                    KeyWithBytes.of(
                                        Key.of("bar"),
                                        ContentId.of("contentId-no-no"),
                                        (byte) 0,
                                        ByteString.copyFromUtf8("hello")))
                                .build()))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage(
                    String.format(
                        "Could not find commit '%s' in reference '%s'.",
                        unreachableHead.asString(), helper.getName())),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.assign(
                            helper,
                            Optional.of(unreachableHead),
                            databaseAdapter.hashOnReference(main, Optional.empty())))
                .isInstanceOf(ReferenceConflictException.class)
                .hasMessage(
                    String.format(
                        "Named-reference '%s' is not at expected hash '%s', but at '%s'.",
                        helper.getName(), unreachableHead.asString(), helperHead.asString())));
  }

  @Test
  void assign() throws Exception {
    BranchName main = BranchName.of("main");
    TagName tag = TagName.of("tag");
    TagName branch = TagName.of("branch");

    databaseAdapter.create(branch, databaseAdapter.hashOnReference(main, Optional.empty()));
    databaseAdapter.create(tag, databaseAdapter.hashOnReference(main, Optional.empty()));

    Hash beginning = databaseAdapter.hashOnReference(main, Optional.empty());

    Hash[] commits = new Hash[3];
    for (int i = 0; i < commits.length; i++) {
      commits[i] =
          databaseAdapter.commit(
              ImmutableCommitAttempt.builder()
                  .commitToBranch(main)
                  .commitMetaSerialized(ByteString.copyFromUtf8("commit meta " + i))
                  .addPuts(
                      KeyWithBytes.of(
                          Key.of("bar", Integer.toString(i)),
                          ContentId.of("contentId-" + i),
                          (byte) 0,
                          ByteString.copyFromUtf8("hello " + i)))
                  .build());
    }

    Hash expect = beginning;
    for (Hash commit : commits) {
      assertThat(
              Arrays.asList(
                  databaseAdapter.hashOnReference(branch, Optional.empty()),
                  databaseAdapter.hashOnReference(tag, Optional.empty())))
          .containsExactly(expect, expect);

      databaseAdapter.assign(tag, Optional.of(expect), commit);

      databaseAdapter.assign(branch, Optional.of(expect), commit);

      expect = commit;
    }

    assertThat(
            Arrays.asList(
                databaseAdapter.hashOnReference(branch, Optional.empty()),
                databaseAdapter.hashOnReference(tag, Optional.empty())))
        .containsExactly(commits[commits.length - 1], commits[commits.length - 1]);
  }

  @Test
  void diff() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");

    Hash initialHash =
        databaseAdapter.create(branch, databaseAdapter.hashOnReference(main, Optional.empty()));

    Hash[] commits = new Hash[3];
    for (int i = 0; i < commits.length; i++) {
      ImmutableCommitAttempt.Builder commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit " + i));
      for (int k = 0; k < 3; k++) {
        commit.addPuts(
            KeyWithBytes.of(
                Key.of("key", Integer.toString(k)),
                ContentId.of("C" + k),
                (byte) 0,
                ByteString.copyFromUtf8("value " + i + " for " + k)));
      }
      commits[i] = databaseAdapter.commit(commit.build());
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
                        k ->
                            Difference.of(
                                Key.of("key", Integer.toString(k)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(ByteString.copyFromUtf8("value " + c + " for " + k))))
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
                        k ->
                            Difference.of(
                                Key.of("key", Integer.toString(k)),
                                Optional.empty(),
                                Optional.of(ByteString.copyFromUtf8("value " + c + " for " + k)),
                                Optional.empty()))
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
                        k ->
                            Difference.of(
                                Key.of("key", Integer.toString(k)),
                                Optional.empty(),
                                Optional.of(
                                    ByteString.copyFromUtf8("value " + (c - 1) + " for " + k)),
                                Optional.of(ByteString.copyFromUtf8("value " + c + " for " + k))))
                    .collect(Collectors.toList()));
      }
    }
  }

  @Test
  void recreateDefaultBranch() throws Exception {
    BranchName main = BranchName.of("main");
    Hash mainHead = databaseAdapter.hashOnReference(main, Optional.empty());
    databaseAdapter.delete(main, Optional.of(mainHead));

    assertThatThrownBy(() -> databaseAdapter.hashOnReference(main, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    databaseAdapter.create(main, null);
    databaseAdapter.hashOnReference(main, Optional.empty());
  }

  @Nested
  public class MergeTransplant extends AbstractMergeTransplant {
    MergeTransplant() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class GetNamedReferences extends AbstractGetNamedReferences {
    GetNamedReferences() {
      super(databaseAdapter);
    }
  }

  @Test
  void nonExistentKeyPrefixTest(
      @NessieDbAdapter(initializeRepo = false)
          @NessieDbAdapterConfigItem(name = "key.prefix", value = "non-existent")
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
            assertThatThrownBy(
                    () ->
                        nonExistent
                            .namedRef(BranchName.of("main"), GetNamedRefsParams.DEFAULT)
                            .getHash())
                .isInstanceOf(ReferenceNotFoundException.class));
  }

  @Test
  void keyPrefixBasic(
      @NessieDbAdapter @NessieDbAdapterConfigItem(name = "key.prefix", value = "foo")
          DatabaseAdapter foo,
      @NessieDbAdapter @NessieDbAdapterConfigItem(name = "key.prefix", value = "bar")
          DatabaseAdapter bar)
      throws Exception {

    BranchName main = BranchName.of("main");
    BranchName fooBranchName = BranchName.of("foo-branch");
    BranchName barBranchName = BranchName.of("bar-branch");
    ByteString fooCommitMeta = ByteString.copyFromUtf8("meta-foo");
    ByteString barCommitMeta = ByteString.copyFromUtf8("meta-bar");

    foo.commit(
        ImmutableCommitAttempt.builder()
            .commitToBranch(main)
            .commitMetaSerialized(fooCommitMeta)
            .addPuts(
                KeyWithBytes.of(
                    Key.of("foo"), ContentId.of("foo"), (byte) 0, ByteString.copyFromUtf8("foo")))
            .build());
    bar.commit(
        ImmutableCommitAttempt.builder()
            .commitToBranch(main)
            .commitMetaSerialized(barCommitMeta)
            .addPuts(
                KeyWithBytes.of(
                    Key.of("bar"), ContentId.of("bar"), (byte) 0, ByteString.copyFromUtf8("bar")))
            .build());

    Hash fooMain = foo.hashOnReference(main, Optional.empty());
    Hash barMain = bar.hashOnReference(main, Optional.empty());

    Hash fooBranch = foo.create(fooBranchName, fooMain);
    Hash barBranch = bar.create(barBranchName, barMain);

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
