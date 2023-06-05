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
package org.projectnessie.versioned.tests;

import static com.google.common.collect.Streams.stream;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.CommitValidator;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.testworker.OnRefOnly;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractCommits extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  private static final OnRefOnly V_1_1 = newOnRef("v1_1");
  private static final OnRefOnly V_1_2 = newOnRef("v1_2");
  private static final OnRefOnly V_1_3 = newOnRef("v1_3");
  private static final OnRefOnly V_2_1 = newOnRef("v2_1");
  private static final OnRefOnly V_2_2 = newOnRef("v2_2");
  private static final OnRefOnly V_3_1 = newOnRef("v3_1");
  private static final OnRefOnly V_3_2 = newOnRef("v3_2");
  private static final OnRefOnly V_4_1 = newOnRef("v4_1");
  private static final OnRefOnly NEW_v2_1 = newOnRef("new_v2_1");

  protected AbstractCommits(VersionStore store) {
    super(store);
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to it
   * - Check that another commit with no operations can be added with the initial hash
   * - Check the commit can be listed
   * - Check that the commit can be deleted
   */
  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void commitToBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");

    final Hash createHash = store().create(branch, Optional.empty()).getHash();
    final Hash initialHash = store().hashOnReference(branch, Optional.empty(), emptyList());
    soft.assertThat(createHash).isEqualTo(initialHash);

    CommitResult<Commit> result =
        store()
            .commit(
                branch,
                Optional.of(initialHash),
                CommitMeta.fromMessage("Some commit"),
                emptyList());
    soft.assertThat(result.getCommit().getParentHash()).isEqualTo(initialHash);
    soft.assertThat(result.getCommit().getOperations()).isNullOrEmpty();
    soft.assertThat(result.getTargetBranch()).isEqualTo(branch);

    final Hash commitHash0 = result.getCommitHash();
    final Hash commitHash = store().hashOnReference(branch, Optional.empty(), emptyList());
    soft.assertThat(commitHash).isEqualTo(commitHash0);

    soft.assertThat(commitHash).isNotEqualTo(initialHash);
    result =
        store()
            .commit(
                branch,
                Optional.of(initialHash),
                CommitMeta.fromMessage("Another commit"),
                emptyList());
    soft.assertThat(result.getCommit().getParentHash()).isEqualTo(commitHash);
    soft.assertThat(result.getCommit().getOperations()).isNullOrEmpty();
    soft.assertThat(result.getTargetBranch()).isEqualTo(branch);

    final Hash anotherCommitHash = store().hashOnReference(branch, Optional.empty(), emptyList());

    soft.assertThat(commitsList(branch, false))
        .contains(
            commit(anotherCommitHash, "Another commit", commitHash),
            commit(commitHash, "Some commit", initialHash));
    soft.assertThat(commitsList(commitHash, false))
        .contains(commit(commitHash, "Some commit", initialHash));

    soft.assertThatThrownBy(() -> store().delete(branch, Optional.of(initialHash)))
        .isInstanceOf(ReferenceConflictException.class);

    store().delete(branch, Optional.of(anotherCommitHash));
    soft.assertThatThrownBy(() -> store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isInstanceOf(ReferenceNotFoundException.class);
    try (PaginationIterator<ReferenceInfo<CommitMeta>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      soft.assertThat(stream(str).filter(this::filterMainBranch)).isEmpty();
    }
    soft.assertThatThrownBy(() -> store().delete(branch, Optional.of(commitHash)))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  /*
   * Test:
   * - Create a new branch
   * - Add 3 commits in succession with no conflicts to it with put and delete operations
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void commitSomeOperations() throws Exception {
    BranchName branch = BranchName.of("foo");

    ContentKey keyT1 = ContentKey.of("t1");
    ContentKey keyT2 = ContentKey.of("t2");
    ContentKey keyT3 = ContentKey.of("t3");
    ContentKey keyT4 = ContentKey.of("t4");

    Hash base = store().create(branch, Optional.empty()).getHash();

    Hash initialCommit =
        commit("Initial Commit")
            .put("t1", V_1_1)
            .put("t2", V_2_1)
            .put("t3", V_3_1)
            .toBranch(branch);
    Content t1 = store().getValue(branch, ContentKey.of("t1")).content();

    Hash secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2.withId(t1.getId()))
            .delete("t2")
            .delete("t3")
            .put("t4", V_4_1)
            .toBranch(branch);

    Hash thirdCommit = commit("Third Commit").put("t2", V_2_2).unchanged("t4").toBranch(branch);

    soft.assertThat(commitsList(branch, false))
        .contains(
            commit(thirdCommit, "Third Commit", secondCommit),
            commit(secondCommit, "Second Commit", initialCommit),
            commit(initialCommit, "Initial Commit", base));

    try (PaginationIterator<KeyEntry> keys =
        store().getKeys(branch, null, false, NO_KEY_RESTRICTIONS)) {
      soft.assertThat(stream(keys).map(e -> e.getKey().contentKey()))
          .containsExactlyInAnyOrder(keyT1, keyT2, keyT4);
    }

    try (PaginationIterator<KeyEntry> keys =
        store().getKeys(secondCommit, null, false, NO_KEY_RESTRICTIONS)) {
      soft.assertThat(stream(keys).map(e -> e.getKey().contentKey()))
          .containsExactlyInAnyOrder(keyT1, keyT4);
    }

    try (PaginationIterator<KeyEntry> keys =
        store().getKeys(initialCommit, null, false, NO_KEY_RESTRICTIONS)) {
      soft.assertThat(stream(keys).map(e -> e.getKey().contentKey()))
          .containsExactlyInAnyOrder(keyT1, keyT2, keyT3);
    }

    soft.assertThat(
            contentsWithoutId(
                store().getValues(secondCommit, Arrays.asList(keyT1, keyT2, keyT3, keyT4))))
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(keyT1, V_1_2, keyT4, V_4_1));

    soft.assertThat(
            contentsWithoutId(
                store().getValues(initialCommit, Arrays.asList(keyT1, keyT2, keyT3, keyT4))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                keyT1, V_1_1,
                keyT2, V_2_1,
                keyT3, V_3_1));

    soft.assertThat(contentWithoutId(store().getValue(branch, keyT1))).isEqualTo(V_1_2);
    soft.assertThat(contentWithoutId(store().getValue(branch, keyT2))).isEqualTo(V_2_2);
    soft.assertThat(store().getValue(branch, keyT3)).isNull();
    soft.assertThat(contentWithoutId(store().getValue(branch, keyT4))).isEqualTo(V_4_1);

    soft.assertThat(contentWithoutId(store().getValue(secondCommit, keyT1))).isEqualTo(V_1_2);
    soft.assertThat(store().getValue(secondCommit, keyT2)).isNull();
    soft.assertThat(store().getValue(secondCommit, keyT3)).isNull();
    soft.assertThat(contentWithoutId(store().getValue(secondCommit, keyT4))).isEqualTo(V_4_1);

    soft.assertThat(contentWithoutId(store().getValue(initialCommit, keyT1))).isEqualTo(V_1_1);
    soft.assertThat(contentWithoutId(store().getValue(initialCommit, keyT2))).isEqualTo(V_2_1);
    soft.assertThat(contentWithoutId(store().getValue(initialCommit, keyT3))).isEqualTo(V_3_1);
    soft.assertThat(store().getValue(initialCommit, keyT4)).isNull();
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit for 3 keys
   * - Add a commit based on initial commit for first key
   * - Add a commit based on initial commit for second key
   * - Add a commit based on initial commit for third  key
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void commitNonConflictingOperations() throws Exception {
    BranchName branch = BranchName.of("foo");

    Hash base = store().create(branch, Optional.empty()).getHash();

    Hash initialCommit =
        commit("Initial Commit")
            .put("t1", V_1_1)
            .put("t2", V_2_1)
            .put("t3", V_3_1)
            .toBranch(branch);
    Content t1 = store().getValue(branch, ContentKey.of("t1")).content();
    Content t3 = store().getValue(branch, ContentKey.of("t3")).content();

    Hash t1Commit =
        commit("T1 Commit")
            .fromReference(initialCommit)
            .put("t1", V_1_2.withId(t1.getId()))
            .toBranch(branch);
    t1 = store().getValue(branch, ContentKey.of("t1")).content();

    Hash t2Commit = commit("T2 Commit").fromReference(initialCommit).delete("t2").toBranch(branch);
    Hash t3Commit =
        commit("T3 Commit").fromReference(initialCommit).unchanged("t3").toBranch(branch);
    Hash extraCommit =
        commit("Extra Commit")
            .fromReference(t1Commit)
            .put("t1", V_1_3.withId(t1.getId()))
            .put("t3", V_3_2.withId(t3.getId()))
            .toBranch(branch);
    Hash newT2Commit =
        commit("New T2 Commit").fromReference(t2Commit).put("t2", NEW_v2_1).toBranch(branch);

    soft.assertThat(commitsList(branch, false))
        .contains(
            commit(newT2Commit, "New T2 Commit", extraCommit),
            commit(extraCommit, "Extra Commit", t3Commit),
            commit(t3Commit, "T3 Commit", t2Commit),
            commit(t2Commit, "T2 Commit", t1Commit),
            commit(t1Commit, "T1 Commit", initialCommit),
            commit(initialCommit, "Initial Commit", base));

    try (PaginationIterator<KeyEntry> keys =
        store().getKeys(branch, null, false, NO_KEY_RESTRICTIONS)) {
      soft.assertThat(stream(keys).map(e -> e.getKey().contentKey()))
          .containsExactlyInAnyOrder(ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3"));
    }

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        branch,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_3,
                ContentKey.of("t2"), NEW_v2_1,
                ContentKey.of("t3"), V_3_2));

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newT2Commit,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_3,
                ContentKey.of("t2"), NEW_v2_1,
                ContentKey.of("t3"), V_3_2));

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        extraCommit,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_3,
                ContentKey.of("t3"), V_3_2));

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        t3Commit,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_2,
                ContentKey.of("t3"), V_3_1));

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        t2Commit,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_2,
                ContentKey.of("t3"), V_3_1));

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        t1Commit,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_2,
                ContentKey.of("t2"), V_2_1,
                ContentKey.of("t3"), V_3_1));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void commitConflictingOperationsLegacy() throws Exception {
    assumeThat(isNewStorageModel()).isFalse();

    BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    Hash initialCommit =
        commit("Initial Commit").put("t1", V_1_1).put("t2", V_2_1).toBranch(branch);

    Content t1 = store().getValue(branch, ContentKey.of("t1")).content();
    store().getValue(branch, ContentKey.of("t2"));

    Hash secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2.withId(t1.getId()))
            .delete("t2")
            .put("t3", V_3_1)
            .toBranch(branch);
    store().getValue(branch, ContentKey.of("t3"));

    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t1", V_1_3)
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class);
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t2", V_2_2)
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class);
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t3", V_3_2)
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class);

    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .delete("t1")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class);
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .delete("t2")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class);
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .delete("t3")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class);

    // Checking the state hasn't changed
    soft.assertThat(store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isEqualTo(secondCommit);
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void commitConflictingOperations() throws Exception {
    assumeThat(isNewStorageModel()).isTrue();

    BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    Hash initialCommit =
        commit("Initial Commit").put("t1", V_1_1).put("t2", V_2_1).toBranch(branch);

    Content t1 = store().getValue(branch, ContentKey.of("t1")).content();
    Content t2 = store().getValue(branch, ContentKey.of("t2")).content();

    Hash secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2.withId(t1.getId()))
            .delete("t2")
            .put("t3", V_3_1)
            .toBranch(branch);
    Content t3 = store().getValue(branch, ContentKey.of("t3")).content();

    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t1", V_1_3)
                    .toBranch(branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("New value to update existing key 't1' has no content ID");
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t4", V_4_1)
                    .unchanged("t2")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith("Key 't2' does not exist.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(ConflictType.KEY_DOES_NOT_EXIST, ContentKey.of("t2"), "key 't2' does not exist"));
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t4", V_4_1)
                    .unchanged("t1")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith("Values of existing and expected content for key 't1' are different.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(
                ConflictType.VALUE_DIFFERS,
                ContentKey.of("t1"),
                "values of existing and expected content for key 't1' are different"));
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t1", V_1_3.withId(t1.getId()))
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith("Values of existing and expected content for key 't1' are different.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(
                ConflictType.VALUE_DIFFERS,
                ContentKey.of("t1"),
                "values of existing and expected content for key 't1' are different"));
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t2", V_2_2.withId(t2.getId()))
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith("Key 't2' does not exist.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(ConflictType.KEY_DOES_NOT_EXIST, ContentKey.of("t2"), "key 't2' does not exist"));
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t3", V_3_2.withId(t3.getId()))
                    .toBranch(branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("New value for key 't3' must not have a content ID");
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .put("t3", V_3_2)
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith("Key 't3' already exists.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(ConflictType.KEY_EXISTS, ContentKey.of("t3"), "key 't3' already exists"));

    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .delete("t1")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith("Values of existing and expected content for key 't1' are different.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(
                ConflictType.VALUE_DIFFERS,
                ContentKey.of("t1"),
                "values of existing and expected content for key 't1' are different"));
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .delete("t2")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith("Key 't2' does not exist.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(ConflictType.KEY_DOES_NOT_EXIST, ContentKey.of("t2"), "key 't2' does not exist"));
    soft.assertThatThrownBy(
            () ->
                commit("Conflicting Commit")
                    .fromReference(initialCommit)
                    .delete("t3")
                    .toBranch(branch))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageEndingWith(
            "Payload of existing and expected content for key 't3' are different.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(
                ConflictType.PAYLOAD_DIFFERS,
                ContentKey.of("t3"),
                "payload of existing and expected content for key 't3' are different"));

    // Checking the state hasn't changed
    soft.assertThat(store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isEqualTo(secondCommit);
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - force commit put operations
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void forceCommitConflictingOperations() throws Exception {
    BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    commit("Initial Commit").put("t1", V_1_1).put("t2", V_2_1).toBranch(branch);
    Content t1 = store().getValue(branch, ContentKey.of("t1")).content();

    commit("Second Commit")
        .put("t1", V_1_2.withId(t1.getId()))
        .delete("t2")
        .put("t3", V_3_1)
        .toBranch(branch);
    Content t3 = store().getValue(branch, ContentKey.of("t3")).content();

    Hash putCommit =
        forceCommit("Conflicting Commit")
            .put("t1", V_1_3.withId(t1.getId()))
            .put("t2", V_2_2)
            .put("t3", V_3_2.withId(t3.getId()))
            .toBranch(branch);

    soft.assertThat(store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isEqualTo(putCommit);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        branch,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_3,
                ContentKey.of("t2"), V_2_2,
                ContentKey.of("t3"), V_3_2));

    Hash unchangedCommit =
        commit("Conflicting Commit")
            .unchanged("t1")
            .unchanged("t2")
            .unchanged("t3")
            .toBranch(branch);
    soft.assertThat(store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isEqualTo(unchangedCommit);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        branch,
                        Arrays.asList(
                            ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_3,
                ContentKey.of("t2"), V_2_2,
                ContentKey.of("t3"), V_3_2));

    Hash deleteCommit =
        commit("Conflicting Commit").delete("t1").delete("t2").delete("t3").toBranch(branch);
    soft.assertThat(store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isEqualTo(deleteCommit);
    soft.assertThat(
            store()
                .getValues(
                    branch,
                    Arrays.asList(ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3"))))
        .isEmpty();
  }

  /*
   * Test:
   *  - Check that store allows storing the same value under different keys
   */
  @Test
  public void commitDuplicateValues() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    store().create(branch, Optional.empty());
    OnRefOnly foo1 = newOnRef("foo");
    OnRefOnly foo2 = newOnRef("foo");
    store()
        .commit(
            branch,
            Optional.empty(),
            CommitMeta.fromMessage("metadata"),
            ImmutableList.of(put("keyA", foo1), put("keyB", foo2)));

    soft.assertThat(contentWithoutId(store().getValue(branch, ContentKey.of("keyA"))))
        .isEqualTo(foo1);
    soft.assertThat(contentWithoutId(store().getValue(branch, ContentKey.of("keyB"))))
        .isEqualTo(foo2);
  }

  /*
   * Test:
   * - Check that store throws RNFE if branch doesn't exist
   */
  @Test
  public void commitWithInvalidBranch() {
    final BranchName branch = BranchName.of("unknown");

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("New commit"),
                        emptyList()))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  /*
   * Test:
   * - Check that store throws RNFE if reference hash doesn't exist
   */
  @Test
  public void commitWithUnknownReference()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.of(Hash.of("1234567890abcdef")),
                        CommitMeta.fromMessage("New commit"),
                        emptyList()))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  /*
   * Test:
   * - Check that store throws IllegalArgumentException if reference hash is not in branch ancestry
   */
  @Test
  public void commitWithInvalidReference()
      throws ReferenceNotFoundException,
          ReferenceConflictException,
          ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("foo");
    Hash initialHash = store().create(branch, Optional.empty()).getHash();

    Hash commitHash =
        store()
            .commit(
                branch,
                Optional.of(initialHash),
                CommitMeta.fromMessage("Some commit"),
                emptyList())
            .getCommitHash();

    BranchName branch2 = BranchName.of("bar");
    store().create(branch2, Optional.empty());

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch2,
                        Optional.of(commitHash),
                        CommitMeta.fromMessage("Another commit"),
                        emptyList()))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  static Stream<Arguments> renames() {
    return Stream.of(
        arguments(false, false),
        arguments(false, true),
        arguments(true, false),
        arguments(true, true));
  }

  @ParameterizedTest
  @MethodSource("renames")
  void renames(boolean reverse, boolean recreate) throws Exception {
    BranchName branch = BranchName.of("foo");
    Hash initialHash = store().create(branch, Optional.empty()).getHash();

    ContentKey original = ContentKey.of("original");
    ContentKey renamed = ContentKey.of("renamed");

    Hash committed =
        store()
            .commit(
                branch,
                Optional.of(initialHash),
                CommitMeta.fromMessage("Some commit"),
                Collections.singletonList(Put.of(original, IcebergTable.of("loc", 1, 2, 3, 4))))
            .getCommitHash();

    Content table = store().getValue(branch, original).content();

    Delete deleteOp = Delete.of(original);
    Put putOp =
        Put.of(
            renamed,
            recreate
                ? IcebergTable.of("recreated", 1, 2, 3, 4)
                : IcebergTable.of("renamed", 1, 2, 3, 4, table.getId()));

    List<Operation> ops = reverse ? Arrays.asList(putOp, deleteOp) : Arrays.asList(deleteOp, putOp);

    store().commit(branch, Optional.of(committed), CommitMeta.fromMessage("Rename commit"), ops);

    soft.assertThat(store().getValues(branch, Arrays.asList(original, renamed)))
        .containsKey(renamed)
        .doesNotContainKey(original);
  }

  @Test
  void commitWithValidation() throws Exception {
    BranchName branch = BranchName.of("main");
    ContentKey key = ContentKey.of("table0");
    Hash branchHead = store().getNamedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash();

    RuntimeException exception = new ArithmeticException("Whatever");
    soft.assertThatThrownBy(
            () ->
                doCommitWithValidation(
                    branch,
                    key,
                    validation -> {
                      // do some operations here
                      try {
                        assertThat(store().getValue(branch, key)).isNull();
                        try (PaginationIterator<KeyEntry> ignore =
                            store().getKeys(branch, null, false, NO_KEY_RESTRICTIONS)) {}
                      } catch (ReferenceNotFoundException e) {
                        throw new RuntimeException(e);
                      }

                      // let the custom commit-validation fail
                      throw exception;
                    }))
        .isSameAs(exception);

    soft.assertThat(store().getNamedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash())
        .isEqualTo(branchHead);
    soft.assertThat(store().getValue(branch, key)).isNull();
  }

  void doCommitWithValidation(BranchName branch, ContentKey key, CommitValidator validator)
      throws Exception {
    store()
        .commit(
            branch,
            Optional.empty(),
            CommitMeta.fromMessage("initial commit meta"),
            Collections.singletonList(Put.of(key, newOnRef("some value"))),
            validator,
            (k, c) -> {});
  }

  @Test
  void duplicateKeys() {
    BranchName branch = BranchName.of("main");

    ContentKey key = ContentKey.of("my.awesome.table");
    String tableRefState = "table ref state";

    Content createValue1 = newOnRef("no no - not this");
    Content createValue2 = newOnRef(tableRefState);

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("initial"),
                        Arrays.asList(Put.of(key, createValue1), Put.of(key, createValue2))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("initial"),
                        Arrays.asList(Unchanged.of(key), Put.of(key, createValue2))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("initial"),
                        Arrays.asList(Delete.of(key), Put.of(key, createValue2))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("initial"),
                        Arrays.asList(Delete.of(key), Unchanged.of(key))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());
  }
}
