/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.impl;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.ALL;
import static org.projectnessie.model.FetchOption.MINIMAL;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;
import static org.projectnessie.model.Reference.ReferenceType.TAG;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;

public abstract class AbstractTestReferences extends BaseTestServiceImpl {

  @ParameterizedTest
  @ValueSource(ints = {0, 20, 22})
  public void referencesPaging(int numRefs) throws BaseNessieClientServerException {
    Branch defaultBranch = treeApi().getDefaultBranch();
    int pageSize = 5;

    IntFunction<Branch> branch =
        i -> i == 0 ? defaultBranch : Branch.of("branch-" + i, defaultBranch.getHash());

    for (int i = 1; i < numRefs; i++) {
      createBranch(branch.apply(i).getName());
    }

    if (numRefs == 0) {
      // The main branch's always there
      numRefs = 1;
    }

    List<Reference> references = pagedReferences(MINIMAL, null, pageSize, numRefs);

    soft.assertThat(references)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, numRefs).mapToObj(branch).collect(toSet()));
  }

  @Test
  public void defaultBranchProtection() throws BaseNessieClientServerException {
    Branch defaultBranch = treeApi().getDefaultBranch();

    soft.assertThatThrownBy(
            () ->
                treeApi().deleteReference(BRANCH, defaultBranch.getName(), defaultBranch.getHash()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be deleted");

    soft.assertThatThrownBy(() -> createBranch(defaultBranch.getName(), defaultBranch))
        .isInstanceOf(NessieConflictException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void getAllReferences() {
    assertThat(allReferences())
        .anySatisfy(
            r ->
                assertThat(r.getName())
                    .isEqualTo(configApi().getServerConfig().getDefaultBranch()));
  }

  @Test
  public void getUnknownReference() {
    assertThatThrownBy(() -> treeApi().getReferenceByName("unknown123", MINIMAL))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining("unknown123");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "HEAD",
        "DETACHED",
        "cafebabedeadbeef",
        "a234567890123456",
        "CAFEBABEDEADBEEF",
        "A234567890123456",
        "caffee20",
        "caffee2022"
      })
  public void forbiddenReferenceNames(String refName) throws NessieNotFoundException {
    Branch main = treeApi().getDefaultBranch();

    assertThat(
            Stream.of(
                Branch.of(refName, null),
                Tag.of(refName, null),
                Branch.of(refName, main.getHash()),
                Tag.of(refName, main.getHash())))
        .allSatisfy(
            ref ->
                assertThatThrownBy(
                        () ->
                            treeApi()
                                .createReference(
                                    ref.getName(),
                                    ref instanceof Branch ? BRANCH : TAG,
                                    ref.getHash(),
                                    main.getName()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Validation.FORBIDDEN_REF_NAME_MESSAGE));
  }

  @Test
  public void createReferences() throws Exception {
    Branch main = treeApi().getDefaultBranch();

    String tagName1 = "createReferences_tag1";
    String tagName2 = "createReferences_tag2";
    String branchName1 = "createReferences_branch1";
    String branchName2 = "createReferences_branch2";

    // invalid source ref & null hash
    soft.assertThatThrownBy(() -> createTag(tagName2, Branch.of("unknownSource", main.getHash())))
        .isInstanceOf(NessieReferenceNotFoundException.class)
        .hasMessageContainingAll("'unknownSource'", "not");
    // Tag without sourceRefName & null hash
    soft.assertThatThrownBy(() -> treeApi().createReference(tagName1, TAG, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Target hash must be provided.");
    // Tag without hash
    soft.assertThatThrownBy(() -> treeApi().createReference(tagName1, TAG, null, main.getName()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Target hash must be provided.");
    // legit Tag with name + hash
    Tag refTag1 = createTag(tagName2, main);
    soft.assertThat(refTag1).isEqualTo(Tag.of(tagName2, main.getHash()));

    // Branch without hash
    soft.assertThatThrownBy(
            () -> treeApi().createReference(branchName1, BRANCH, null, main.getName()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Target hash must be provided");
    // Branch with name + hash
    Reference refBranch2 =
        treeApi().createReference(branchName2, BRANCH, main.getHash(), main.getName());
    soft.assertThat(refBranch2).isEqualTo(Branch.of(branchName2, main.getHash()));
  }

  @Test
  public void getAndDeleteBranch() throws Exception {
    Branch branch = createBranch("testBranch");
    assertThat(treeApi().deleteReference(BRANCH, branch.getName(), branch.getHash()))
        .isEqualTo(branch);
  }

  @Test
  public void getAndDeleteTag() throws Exception {
    Tag tag = createTag("testTag", treeApi().getDefaultBranch());
    assertThat(treeApi().deleteReference(TAG, tag.getName(), tag.getHash())).isEqualTo(tag);
  }

  @ParameterizedTest
  @ValueSource(strings = {"normal", "with-no_space", "slash/thing"})
  public void referenceNames(String refNamePart) throws BaseNessieClientServerException {
    String tagName = "tag" + refNamePart;
    String branchName = "branch" + refNamePart;
    String branchName2 = "branch2" + refNamePart;

    String root = "ref_name_" + refNamePart.replaceAll("[^a-z]", "");
    Branch main = createBranch(root);

    IcebergTable meta = IcebergTable.of("meep", 42, 42, 42, 42);
    main =
        commit(
                main,
                CommitMeta.builder()
                    .message("common-merge-ancestor")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build(),
                Put.of(ContentKey.of("meep"), meta))
            .getTargetBranch();
    String someHash = main.getHash();

    Reference createdTag = createTag(tagName, Branch.of(main.getName(), someHash));
    soft.assertThat(createdTag).isEqualTo(Tag.of(tagName, someHash));
    Reference createdBranch1 = createBranch(branchName, Branch.of(main.getName(), someHash));
    soft.assertThat(createdBranch1).isEqualTo(Branch.of(branchName, someHash));
    Reference createdBranch2 = createBranch(branchName2, Branch.of(main.getName(), someHash));
    soft.assertThat(createdBranch2).isEqualTo(Branch.of(branchName2, someHash));

    Map<String, Reference> references =
        allReferences().stream()
            .filter(r -> root.equals(r.getName()) || r.getName().endsWith(refNamePart))
            .collect(Collectors.toMap(Reference::getName, Function.identity()));

    soft.assertThat(references)
        .containsAllEntriesOf(
            ImmutableMap.of(
                main.getName(),
                main,
                createdTag.getName(),
                createdTag,
                createdBranch1.getName(),
                createdBranch1,
                createdBranch2.getName(),
                createdBranch2));
    soft.assertThat(references.get(main.getName())).isInstanceOf(Branch.class);
    soft.assertThat(references.get(createdTag.getName())).isInstanceOf(Tag.class);
    soft.assertThat(references.get(createdBranch1.getName())).isInstanceOf(Branch.class);
    soft.assertThat(references.get(createdBranch2.getName())).isInstanceOf(Branch.class);

    Reference tagRef = references.get(tagName);
    Reference branchRef = references.get(branchName);
    Reference branchRef2 = references.get(branchName2);

    String tagHash = tagRef.getHash();
    String branchHash = branchRef.getHash();
    String branchHash2 = branchRef2.getHash();

    soft.assertThat(getReference(tagName)).isEqualTo(tagRef);
    soft.assertThat(getReference(branchName)).isEqualTo(branchRef);

    soft.assertThat(entries(tagName, null)).isNotNull();
    soft.assertThat(entries(branchName, null)).isNotNull();

    soft.assertThat(commitLog(tagName)).isNotNull();
    soft.assertThat(commitLog(branchName)).isNotNull();

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge,
    // delete) will fail
    meta = IcebergTable.of("foo", 42, 42, 42, 42);
    commit(
        Branch.of(branchName, branchHash),
        fromMessage("One dummy op"),
        Put.of(ContentKey.of("some-key"), meta));
    List<LogEntry> log = commitLog(branchName);
    String newHash = log.get(0).getCommitMeta().getHash();

    treeApi().assignReference(TAG, tagName, tagHash, Branch.of(branchName, newHash));
    treeApi().assignReference(BRANCH, branchName, newHash, Branch.of(branchName, newHash));

    treeApi()
        .mergeRefIntoBranch(
            branchName2,
            branchHash2,
            branchName,
            newHash,
            null,
            Collections.emptyList(),
            MergeBehavior.NORMAL,
            false,
            false,
            false);
  }

  @Test
  public void filterReferences() throws BaseNessieClientServerException {
    commit(
        createBranch("refs.branch.1"),
        fromMessage("some awkward message"),
        Put.of(ContentKey.of("hello.world.BaseTable"), IcebergView.of("path1", 1, 1)));
    Branch b2 =
        commit(
                createBranch("other-development"),
                fromMessage("invent awesome things"),
                Put.of(ContentKey.of("cool.stuff.Caresian"), IcebergView.of("path2", 1, 1)))
            .getTargetBranch();
    commit(
        createBranch("archive"),
        fromMessage("boring old stuff"),
        Put.of(ContentKey.of("super.old.Numbers"), IcebergView.of("path3", 1, 1)));
    Tag t1 = createTag("my-tag", b2);

    soft.assertThat(allReferences(MINIMAL, "ref.name == 'other-development'"))
        .hasSize(1)
        .allSatisfy(
            ref ->
                assertThat(ref)
                    .isInstanceOf(Branch.class)
                    .extracting(Reference::getName, Reference::getHash)
                    .containsExactly(b2.getName(), b2.getHash()));
    soft.assertThat(allReferences(MINIMAL, "refType == 'TAG'"))
        .allSatisfy(ref -> assertThat(ref).isInstanceOf(Tag.class));
    soft.assertThat(allReferences(MINIMAL, "refType == 'BRANCH'"))
        .allSatisfy(ref -> assertThat(ref).isInstanceOf(Branch.class));
    soft.assertThat(
            allReferences(MINIMAL, "has(refMeta.numTotalCommits) && refMeta.numTotalCommits < 0"))
        .isEmpty();
    soft.assertThat(allReferences(ALL, "commit.message == 'invent awesome things'"))
        .hasSize(2)
        .allSatisfy(ref -> assertThat(ref.getName()).isIn(b2.getName(), t1.getName()));
    soft.assertThat(
            allReferences(ALL, "refType == 'TAG' && commit.message == 'invent awesome things'"))
        .hasSize(1)
        .allSatisfy(ref -> assertThat(ref.getName()).isEqualTo(t1.getName()));
  }

  @Test
  public void testReferencesHaveMetadataProperties() throws BaseNessieClientServerException {
    String branchPrefix = "branchesHaveMetadataProperties";
    String tagPrefix = "tagsHaveMetadataProperties";
    int numBranches = 3;
    int commitsPerBranch = 3;

    for (int i = 0; i < numBranches; i++) {
      Branch r = createBranch(branchPrefix + i);
      String currentHash = r.getHash();
      currentHash = createCommits(r, 1, commitsPerBranch, currentHash);

      createTag(tagPrefix + i, Branch.of(r.getName(), currentHash));
    }
    // not fetching additional metadata
    List<Reference> references = allReferences();
    Branch main = treeApi().getDefaultBranch();

    soft.assertThat(
            references.stream()
                .filter(r -> r.getName().startsWith(branchPrefix))
                .map(r -> (Branch) r))
        .hasSize(numBranches)
        .allSatisfy(branch -> assertThat(branch.getMetadata()).isNull());

    soft.assertThat(
            references.stream().filter(r -> r.getName().startsWith(tagPrefix)).map(r -> (Tag) r))
        .hasSize(numBranches)
        .allSatisfy(tag -> assertThat(tag.getMetadata()).isNull());

    // fetching additional metadata for each reference
    references = allReferences(ALL, null);
    soft.assertThat(
            references.stream()
                .filter(r -> r.getName().startsWith(branchPrefix))
                .map(r -> (Branch) r))
        .hasSize(numBranches)
        .allSatisfy(
            branch ->
                verifyMetadataProperties(commitsPerBranch, 0, branch, main, commitsPerBranch));

    soft.assertThat(
            references.stream().filter(r -> r.getName().startsWith(tagPrefix)).map(r -> (Tag) r))
        .hasSize(numBranches)
        .allSatisfy(this::verifyMetadataProperties);
  }

  @Test
  public void testSingleReferenceHasMetadataProperties() throws BaseNessieClientServerException {
    String branchName = "singleBranchHasMetadataProperties";
    String tagName = "singleTagHasMetadataProperties";
    int numCommits = 3;

    Branch r = createBranch(branchName);
    String currentHash = r.getHash();
    currentHash = createCommits(r, 1, numCommits, currentHash);
    createTag(tagName, Branch.of(r.getName(), currentHash));

    // not fetching additional metadata for a single branch
    Reference ref = treeApi().getReferenceByName(branchName, MINIMAL);
    soft.assertThat(ref).isNotNull().isInstanceOf(Branch.class).extracting("metadata").isNull();

    // not fetching additional metadata for a single tag
    ref = treeApi().getReferenceByName(tagName, MINIMAL);
    soft.assertThat(ref).isNotNull().isInstanceOf(Tag.class).extracting("metadata").isNull();

    // fetching additional metadata for a single branch
    ref = treeApi().getReferenceByName(branchName, ALL);
    soft.assertThat(ref).isNotNull().isInstanceOf(Branch.class);
    verifyMetadataProperties(
        numCommits,
        0,
        (Branch) ref,
        getReference(configApi().getServerConfig().getDefaultBranch()),
        numCommits);

    // fetching additional metadata for a single tag
    ref = treeApi().getReferenceByName(tagName, ALL);
    soft.assertThat(ref).isNotNull().isInstanceOf(Tag.class);
    verifyMetadataProperties((Tag) ref);
  }

  void verifyMetadataProperties(
      int expectedCommitsAhead,
      int expectedCommitsBehind,
      Branch branch,
      Reference reference,
      long expectedCommits)
      throws NessieNotFoundException {
    CommitMeta commitMeta = commitMetaForVerify(branch);

    ReferenceMetadata referenceMetadata = branch.getMetadata();
    soft.assertThat(referenceMetadata)
        .isNotNull()
        .extracting(
            ReferenceMetadata::getNumCommitsAhead,
            ReferenceMetadata::getNumCommitsBehind,
            ReferenceMetadata::getCommitMetaOfHEAD,
            ReferenceMetadata::getCommonAncestorHash,
            ReferenceMetadata::getNumTotalCommits)
        .containsExactly(
            expectedCommitsAhead,
            expectedCommitsBehind,
            commitMeta,
            reference.getHash(),
            expectedCommits);
  }

  void verifyMetadataProperties(Tag tag) throws NessieNotFoundException {
    CommitMeta commitMeta = commitMetaForVerify(tag);

    ReferenceMetadata referenceMetadata = tag.getMetadata();
    soft.assertThat(referenceMetadata)
        .isNotNull()
        .extracting(
            ReferenceMetadata::getNumCommitsAhead,
            ReferenceMetadata::getNumCommitsBehind,
            ReferenceMetadata::getCommitMetaOfHEAD,
            ReferenceMetadata::getCommonAncestorHash,
            ReferenceMetadata::getNumTotalCommits)
        .containsExactly(null, null, commitMeta, null, 3L);
  }

  private CommitMeta commitMetaForVerify(Reference ref) throws NessieNotFoundException {
    List<LogEntry> commits =
        treeApi()
            .getCommitLog(
                ref.getName(),
                MINIMAL,
                null,
                null,
                null,
                null,
                new DirectPagedCountingResponseHandler<>(1, token -> {}));
    soft.assertThat(commits).hasSize(1);
    return commits.get(0).getCommitMeta();
  }
}
