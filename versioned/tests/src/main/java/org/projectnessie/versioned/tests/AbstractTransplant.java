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
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;
import org.projectnessie.versioned.testworker.OnRefOnly;

public abstract class AbstractTransplant extends AbstractNestedVersionStore {

  private static final OnRefOnly V_1_1 = newOnRef("v1_1");
  private static final OnRefOnly V_1_2 = newOnRef("v1_2");
  private static final OnRefOnly V_1_4 = newOnRef("v1_4");
  private static final OnRefOnly V_2_2 = newOnRef("v2_2");
  private static final OnRefOnly V_2_1 = newOnRef("v2_1");
  private static final OnRefOnly V_3_1 = newOnRef("v3_1");
  private static final OnRefOnly V_4_1 = newOnRef("v4_1");
  private static final OnRefOnly V_5_1 = newOnRef("v5_1");

  protected AbstractTransplant(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  private Hash initialHash;
  private Hash firstCommit;
  private Hash secondCommit;
  private Hash thirdCommit;
  private List<Commit<CommitMessage, BaseContent>> commits;

  private MetadataRewriter<CommitMessage> createMetadataRewriter(String suffix) {
    return new MetadataRewriter<CommitMessage>() {
      @Override
      public CommitMessage rewriteSingle(CommitMessage metadata) {
        return metadata;
      }

      @Override
      public CommitMessage squash(List<CommitMessage> metadata) {
        return CommitMessage.commitMessage(
            metadata.stream()
                .map(cm -> cm.getMessage() + suffix)
                .collect(Collectors.joining("\n-----------------------------------\n")));
      }
    };
  }

  @BeforeEach
  protected void setupCommits() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    initialHash = store().hashOnReference(branch, Optional.empty());

    firstCommit =
        commit("Initial Commit")
            .put("t1", V_1_1)
            .put("t2", V_2_1)
            .put("t3", V_3_1)
            .toBranch(branch);

    secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2)
            .delete("t2")
            .delete("t3")
            .put("t4", V_4_1)
            .toBranch(branch);

    thirdCommit = commit("Third Commit").put("t2", V_2_2).unchanged("t4").toBranch(branch);

    commits = commitsList(branch, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantOnEmptyBranch(boolean individualCommits)
      throws VersionStoreException {
    checkTransplantOnEmptyBranch(createMetadataRewriter(""), individualCommits);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantOnEmptyBranchModify(boolean individualCommits)
      throws VersionStoreException {
    BranchName newBranch =
        checkTransplantOnEmptyBranch(createMetadataRewriter(""), individualCommits);

    if (!individualCommits) {
      assertThat(commitsList(newBranch, false))
          .first()
          .extracting(Commit::getCommitMeta)
          .extracting(CommitMessage::getMessage)
          .asString()
          .contains(
              commits.stream()
                  .map(Commit::getCommitMeta)
                  .map(CommitMessage::getMessage)
                  .toArray(String[]::new));
    }
  }

  private BranchName checkTransplantOnEmptyBranch(
      MetadataRewriter<CommitMessage> commitMetaModify, boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_1");
    store().create(newBranch, Optional.empty());

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            commitMetaModify,
            individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1));

    if (individualCommits) {
      assertCommitMeta(commitsList(newBranch, false).subList(0, 3), commits, commitMetaModify);
    }

    return newBranch;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithPreviousCommit(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    commit("Unrelated commit").put("t5", V_5_1).toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            createMetadataRewriter(""),
            individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(
                        Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1,
                Key.of("t5"), V_5_1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWitConflictingCommit(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_3");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put("t1", V_1_4).toBranch(newBranch);

    assertThrows(
        ReferenceConflictException.class,
        () ->
            store()
                .transplant(
                    newBranch,
                    Optional.of(initialHash),
                    Arrays.asList(firstCommit, secondCommit, thirdCommit),
                    createMetadataRewriter(""),
                    individualCommits));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithDelete(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_4");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put("t1", V_1_4).toBranch(newBranch);
    commit("Another commit").delete("t1").toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            createMetadataRewriter(""),
            individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantOnNonExistingBranch(boolean individualCommits) {
    final BranchName newBranch = BranchName.of("bar_5");
    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .transplant(
                    newBranch,
                    Optional.of(initialHash),
                    Arrays.asList(firstCommit, secondCommit, thirdCommit),
                    createMetadataRewriter(""),
                    individualCommits));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithNonExistingCommit(boolean individualCommits)
      throws VersionStoreException {
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
                    createMetadataRewriter(""),
                    individualCommits));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithNoExpectedHash(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_7");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put("t5", V_5_1).toBranch(newBranch);
    commit("Another commit").put("t1", V_1_4).toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.empty(),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            createMetadataRewriter(""),
            individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(
                        Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1,
                Key.of("t5"), V_5_1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithCommitsInWrongOrder(boolean individualCommits)
      throws VersionStoreException {
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
                    createMetadataRewriter(""),
                    individualCommits));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkInvalidBranchHash(boolean individualCommits) throws VersionStoreException {
    final BranchName anotherBranch = BranchName.of("bar");
    store().create(anotherBranch, Optional.empty());
    final Hash unrelatedCommit =
        commit("Another Commit")
            .put("t1", V_1_1)
            .put("t2", V_2_1)
            .put("t3", V_3_1)
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
                    createMetadataRewriter(""),
                    individualCommits));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void transplantBasic(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    commit("Unrelated commit").put("t5", V_5_1).toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit),
            createMetadataRewriter(""),
            individualCommits);
    assertThat(
            store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t4"), V_4_1,
                Key.of("t5"), V_5_1));
  }
}
