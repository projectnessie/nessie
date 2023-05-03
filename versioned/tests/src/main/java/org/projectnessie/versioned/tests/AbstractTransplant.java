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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.testworker.OnRefOnly;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractTransplant extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  private static final String T_1 = "t1";
  private static final String T_2 = "t2";
  private static final String T_3 = "t3";
  private static final String T_4 = "t4";
  private static final String T_5 = "t5";

  private static final OnRefOnly V_1_1 = newOnRef("v1_1");
  private static final OnRefOnly V_1_2 = newOnRef("v1_2");
  private static final OnRefOnly V_1_4 = newOnRef("v1_4");
  private static final OnRefOnly V_2_2 = newOnRef("v2_2");
  private static final OnRefOnly V_2_1 = newOnRef("v2_1");
  private static final OnRefOnly V_3_1 = newOnRef("v3_1");
  private static final OnRefOnly V_4_1 = newOnRef("v4_1");
  private static final OnRefOnly V_5_1 = newOnRef("v5_1");

  protected AbstractTransplant(VersionStore store) {
    super(store);
  }

  private Hash initialHash;
  private Hash firstCommit;
  private Hash secondCommit;
  private Hash thirdCommit;
  private List<Commit> commits;

  private MetadataRewriter<CommitMeta> createMetadataRewriter(String suffix) {
    return new MetadataRewriter<CommitMeta>() {
      @Override
      public CommitMeta rewriteSingle(CommitMeta metadata) {
        return CommitMeta.fromMessage(metadata.getMessage());
      }

      @Override
      public CommitMeta squash(List<CommitMeta> metadata) {
        return CommitMeta.fromMessage(
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
        commit("Initial Commit").put(T_1, V_1_1).put(T_2, V_2_1).put(T_3, V_3_1).toBranch(branch);

    Content t1 = store().getValue(branch, ContentKey.of("t1"));

    secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2.withId(t1))
            .delete(T_2)
            .delete(T_3)
            .put(T_4, V_4_1)
            .toBranch(branch);

    thirdCommit = commit("Third Commit").put(T_2, V_2_2).unchanged(T_4).toBranch(branch);

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
          .extracting(CommitMeta::getMessage)
          .asString()
          .contains(
              commits.stream()
                  .map(Commit::getCommitMeta)
                  .map(CommitMeta::getMessage)
                  .toArray(String[]::new));
    }
  }

  private BranchName checkTransplantOnEmptyBranch(
      MetadataRewriter<CommitMeta> commitMetaModify, boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_1");
    store().create(newBranch, Optional.empty());

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            commitMetaModify,
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4)))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1));

    if (individualCommits) {
      assertCommitMeta(
          soft, commitsList(newBranch, false).subList(0, 3), commits, commitMetaModify);
    }

    return newBranch;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithPreviousCommit(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    commit("Unrelated commit").put(T_5, V_5_1).toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            createMetadataRewriter(""),
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4),
                            ContentKey.of(T_5)))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1,
                ContentKey.of(T_5), V_5_1));
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void checkTransplantWithConflictingCommit(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_3");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put(T_1, V_1_4).toBranch(newBranch);

    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        newBranch,
                        Optional.of(initialHash),
                        Arrays.asList(firstCommit, secondCommit, thirdCommit),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceConflictException.class);
    if (dryRun) {
      checkpoint.assertNoWrites();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithDelete(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_4");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put(T_1, V_1_4).toBranch(newBranch);
    commit("Another commit").delete(T_1).toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            createMetadataRewriter(""),
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4)))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1));
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void checkTransplantOnNonExistingBranch(boolean individualCommits, boolean dryRun) {
    final BranchName newBranch = BranchName.of("bar_5");
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        newBranch,
                        Optional.of(initialHash),
                        Arrays.asList(firstCommit, secondCommit, thirdCommit),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void checkTransplantWithNonExistingCommit(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_6");
    store().create(newBranch, Optional.empty());
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        newBranch,
                        Optional.of(initialHash),
                        Collections.singletonList(Hash.of("1234567890abcdef")),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithNoExpectedHash(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_7");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put(T_5, V_5_1).toBranch(newBranch);
    Content t5 = store().getValue(newBranch, ContentKey.of(T_5));
    commit("Another commit").put(T_5, V_1_4.withId(t5)).toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.empty(),
            Arrays.asList(firstCommit, secondCommit, thirdCommit),
            createMetadataRewriter(""),
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4),
                            ContentKey.of(T_5)))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1,
                ContentKey.of(T_5), V_1_4));
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void checkTransplantWithCommitsInWrongOrder(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_8");
    store().create(newBranch, Optional.empty());

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .transplant(
                        newBranch,
                        Optional.empty(),
                        Arrays.asList(secondCommit, firstCommit, thirdCommit),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false));
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void checkInvalidBranchHash(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    final BranchName anotherBranch = BranchName.of("bar");
    store().create(anotherBranch, Optional.empty());
    final Hash unrelatedCommit =
        commit("Another Commit")
            .put(T_1, V_1_1)
            .put(T_2, V_2_1)
            .put(T_3, V_3_1)
            .toBranch(anotherBranch);

    final BranchName newBranch = BranchName.of("bar_1");
    store().create(newBranch, Optional.empty());

    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        newBranch,
                        Optional.of(unrelatedCommit),
                        Arrays.asList(firstCommit, secondCommit, thirdCommit),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void transplantBasic(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    commit("Unrelated commit").put(T_5, V_5_1).toBranch(newBranch);

    store()
        .transplant(
            newBranch,
            Optional.of(initialHash),
            Arrays.asList(firstCommit, secondCommit),
            createMetadataRewriter(""),
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(ContentKey.of(T_1), ContentKey.of(T_4), ContentKey.of(T_5)))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_4), V_4_1,
                ContentKey.of(T_5), V_5_1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void transplantEmptyCommit(boolean individualCommits) throws VersionStoreException {
    BranchName source = BranchName.of("source");
    BranchName target = BranchName.of("target");
    store().create(source, Optional.of(this.initialHash));
    store().create(target, Optional.of(this.initialHash));

    ContentKey key1 = ContentKey.of("key1");
    ContentKey key2 = ContentKey.of("key2");

    // Add two commits to the target branch

    Hash targetHead =
        store()
            .commit(
                target,
                Optional.empty(),
                CommitMeta.fromMessage("target 1"),
                singletonList(Put.of(key1, V_1_1)));

    targetHead =
        store()
            .commit(
                target,
                Optional.of(targetHead),
                CommitMeta.fromMessage("target 2"),
                singletonList(Put.of(key2, V_2_1)));

    // Add two commits to the source branch, with conflicting changes to key1 and key2

    Hash source1 =
        store()
            .commit(
                source,
                Optional.empty(),
                CommitMeta.fromMessage("source 1"),
                singletonList(Put.of(key1, V_1_2)));

    Hash source2 =
        store()
            .commit(
                source,
                Optional.of(source1),
                CommitMeta.fromMessage("source 2"),
                singletonList(Put.of(key2, V_2_2)));

    // Transplant the source branch into the target branch, with a drop of key1 and key2

    store()
        .transplant(
            target,
            Optional.of(targetHead),
            Arrays.asList(source1, source2),
            createMetadataRewriter(", merge-drop"),
            individualCommits,
            ImmutableMap.of(
                key1,
                MergeKeyBehavior.of(key1, MergeBehavior.DROP),
                key2,
                MergeKeyBehavior.of(key2, MergeBehavior.DROP)),
            MergeBehavior.NORMAL,
            false,
            false);

    // No new commit should have been created in the target branch
    try (PaginationIterator<Commit> iterator = store().getCommits(target, true)) {
      Hash newTargetHead = iterator.next().getHash();
      assertThat(newTargetHead).isEqualTo(targetHead);
    }
  }
}
