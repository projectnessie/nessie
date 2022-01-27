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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringStoreWorker.TestEnum;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;

public abstract class AbstractTransplant extends AbstractNestedVersionStore {
  protected AbstractTransplant(VersionStore<String, String, TestEnum> store) {
    super(store);
  }

  private Hash initialHash;
  private Hash firstCommit;
  private Hash secondCommit;
  private Hash thirdCommit;
  private List<Commit<String, String>> commits;

  String transplanted(String commitMeta) {
    return commitMeta + ", transplanted";
  }

  @BeforeEach
  protected void setupCommits() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    initialHash = store().hashOnReference(branch, Optional.empty());

    firstCommit =
        commit("Initial Commit")
            .put("t1", "v1_1")
            .put("t2", "v2_1")
            .put("t3", "v3_1")
            .toBranch(branch);

    secondCommit =
        commit("Second Commit")
            .put("t1", "v1_2")
            .delete("t2")
            .delete("t3")
            .put("t4", "v4_1")
            .toBranch(branch);

    thirdCommit = commit("Third Commit").put("t2", "v2_2").unchanged("t4").toBranch(branch);

    commits = commitsList(branch, false);
  }

  @Test
  protected void checkTransplantOnEmptyBranch() throws VersionStoreException {
    checkTransplantOnEmptyBranch(Function.identity());
  }

  @Test
  protected void checkTransplantOnEmptyBranchModify() throws VersionStoreException {
    checkTransplantOnEmptyBranch(this::transplanted);
  }

  private void checkTransplantOnEmptyBranch(Function<String, String> commitMetaModify)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_1");
    store().create(newBranch, Optional.empty());

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            commitMetaModify);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), "v1_2",
                Key.of("t2"), "v2_2",
                Key.of("t4"), "v4_1"));

    assertCommitMeta(commitsList(newBranch, false).subList(0, 3), commits, commitMetaModify);
  }

  @Test
  protected void checkTransplantWithPreviousCommit() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            Function.identity());
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(
                        Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), "v1_2",
                Key.of("t2"), "v2_2",
                Key.of("t4"), "v4_1",
                Key.of("t5"), "v5_1"));
  }

  @Test
  protected void checkTransplantWitConflictingCommit() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_3");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

    assertThrows(
        ReferenceConflictException.class,
        () ->
            store()
                .transplant(
                    newBranch,
                    Optional.of(initialHash),
                    Arrays.asList(firstCommit, secondCommit, thirdCommit),
                    Function.identity()));
  }

  @Test
  protected void checkTransplantWithDelete() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_4");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put("t1", "v1_4").toBranch(newBranch);
    commit("Another commit").delete("t1").toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            Function.identity());
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), "v1_2",
                Key.of("t2"), "v2_2",
                Key.of("t4"), "v4_1"));
  }

  @Test
  protected void checkTransplantOnNonExistingBranch() {
    final BranchName newBranch = BranchName.of("bar_5");
    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .transplant(
                    newBranch,
                    Optional.of(initialHash),
                    Arrays.asList(firstCommit, secondCommit, thirdCommit),
                    Function.identity()));
  }

  @Test
  protected void checkTransplantWithNonExistingCommit() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_6");
    store().create(newBranch, Optional.empty());
    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .transplant(
                    newBranch,
                    Optional.of(initialHash),
                    Collections.singletonList(Hash.of("1234567890abcdef")),
                    Function.identity()));
  }

  @Test
  protected void checkTransplantWithNoExpectedHash() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_7");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put("t5", "v5_1").toBranch(newBranch);
    commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.empty(),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            Function.identity());
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(
                        Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), "v1_2",
                Key.of("t2"), "v2_2",
                Key.of("t4"), "v4_1",
                Key.of("t5"), "v5_1"));
  }

  @Test
  protected void checkTransplantWithCommitsInWrongOrder() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_8");
    store().create(newBranch, Optional.empty());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            store()
                .transplant(
                    newBranch,
                    Optional.empty(),
                    Arrays.asList(secondCommit, firstCommit, thirdCommit),
                    Function.identity()));
  }

  @Test
  protected void checkInvalidBranchHash() throws VersionStoreException {
    final BranchName anotherBranch = BranchName.of("bar");
    store().create(anotherBranch, Optional.empty());
    final Hash unrelatedCommit =
        commit("Another Commit")
            .put("t1", "v1_1")
            .put("t2", "v2_1")
            .put("t3", "v3_1")
            .toBranch(anotherBranch);

    final BranchName newBranch = BranchName.of("bar_1");
    store().create(newBranch, Optional.empty());

    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .transplant(
                    newBranch,
                    Optional.of(unrelatedCommit),
                    Arrays.asList(firstCommit, secondCommit, thirdCommit),
                    Function.identity()));
  }

  @Test
  protected void transplantBasic() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit),
            Function.identity());
    assertThat(
            store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), "v1_2",
                Key.of("t4"), "v4_1",
                Key.of("t5"), "v5_1"));
  }
}
