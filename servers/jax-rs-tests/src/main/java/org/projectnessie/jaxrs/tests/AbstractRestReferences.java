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
package org.projectnessie.jaxrs.tests;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestReferences extends AbstractRestMisc {
  @NessieApiVersions(versions = NessieApiVersion.V2)
  @ParameterizedTest
  @ValueSource(ints = {0, 20, 22})
  public void referencesPaging(int numRefs) throws BaseNessieClientServerException {
    try {
      getApi().getAllReferences().pageToken("Zm9v").maxRecords(20).get();
    } catch (NessieBadRequestException e) {
      if (!e.getMessage().contains("Paging not supported")) {
        throw e;
      }
      abort("DatabaseAdapter implementations / PersistVersionStore do not support paging");
    }

    Branch defaultBranch = getApi().getDefaultBranch();
    int pageSize = 5;

    IntFunction<Branch> branch =
        i -> i == 0 ? defaultBranch : Branch.of("branch-" + i, defaultBranch.getHash());

    for (int i = 1; i < numRefs; i++) {
      getApi()
          .createReference()
          .sourceRefName(defaultBranch.getName())
          .reference(branch.apply(i))
          .create();
    }

    if (numRefs == 0) {
      // The main branch's always there
      numRefs = 1;
    }

    Set<Reference> references = new HashSet<>();
    String token = null;
    for (int i = 0; ; i += pageSize) {
      ReferencesResponse response =
          getApi().getAllReferences().maxRecords(pageSize).pageToken(token).get();

      for (Reference ref : response.getReferences()) {
        soft.assertThat(references.add(ref))
            .describedAs("offset: %d , reference: %s", i, ref)
            .isTrue();
      }
      soft.assertThat(references).hasSize(Math.min(i + pageSize, numRefs));
      if (i + pageSize < numRefs) {
        soft.assertThat(response.getToken())
            .describedAs("offset: %d", i)
            .isNotEmpty()
            .isNotEqualTo(token);
        soft.assertThat(response.isHasMore()).describedAs("offset: %d", i).isTrue();
        token = response.getToken();
      } else {
        soft.assertThat(response.getToken()).describedAs("offset: %d", i).isNull();
        soft.assertThat(response.isHasMore()).describedAs("offset: %d", i).isFalse();
        break;
      }
    }

    soft.assertThat(references)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, numRefs).mapToObj(branch).collect(toSet()));
  }

  @Test
  public void defaultBranchProtection() throws BaseNessieClientServerException {
    Branch defaultBranch = getApi().getDefaultBranch();

    soft.assertThatThrownBy(() -> getApi().deleteBranch().branch(defaultBranch).delete())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("cannot be deleted");

    soft.assertThatThrownBy(
            () ->
                getApi()
                    .createReference()
                    .reference(Branch.of(defaultBranch.getName(), null))
                    .create())
        .isInstanceOf(NessieConflictException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void getAllReferences() throws Exception {
    assertThat(getApi().getAllReferences().stream())
        .anySatisfy(r -> assertThat(r.getName()).isEqualTo("main"));
  }

  @Test
  public void getUnknownReference() {
    assertThatThrownBy(() -> getApi().getReference().refName("unknown123").get())
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
    String mainHash = getApi().getReference().refName("main").get().getHash();

    assertThat(
            Stream.of(
                Branch.of(refName, null),
                Tag.of(refName, null),
                Branch.of(refName, mainHash),
                Tag.of(refName, mainHash)))
        .allSatisfy(
            ref ->
                assertThatThrownBy(
                        () ->
                            getApi()
                                .createReference()
                                .sourceRefName("main")
                                .reference(ref)
                                .create())
                    .isInstanceOf(NessieBadRequestException.class)
                    .hasMessageContaining(Validation.FORBIDDEN_REF_NAME_MESSAGE));
  }

  @Test
  public void createReferences() throws Exception {
    String mainHash = getApi().getReference().refName("main").get().getHash();

    String tagName1 = "createReferences_tag1";
    String tagName2 = "createReferences_tag2";
    String branchName1 = "createReferences_branch1";
    String branchName2 = "createReferences_branch2";

    // invalid source ref & null hash
    soft.assertThatThrownBy(
            () ->
                getApi()
                    .createReference()
                    .sourceRefName("unknownSource")
                    .reference(Tag.of(tagName2, null))
                    .create())
        .isInstanceOf(NessieReferenceNotFoundException.class)
        .hasMessageContainingAll("'unknownSource'", "not");
    // Tag without sourceRefName & null hash
    soft.assertThatThrownBy(
            () -> getApi().createReference().reference(Tag.of(tagName1, null)).create())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining("Tag-creation requires a target named-reference and hash.");
    // Tag without hash
    soft.assertThatThrownBy(
            () ->
                getApi()
                    .createReference()
                    .sourceRefName("main")
                    .reference(Tag.of(tagName1, null))
                    .create())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining("Tag-creation requires a target named-reference and hash.");
    // legit Tag with name + hash
    Reference refTag1 =
        getApi()
            .createReference()
            .sourceRefName("main")
            .reference(Tag.of(tagName2, mainHash))
            .create();
    soft.assertThat(refTag1).isEqualTo(Tag.of(tagName2, mainHash));

    // Branch without hash
    Reference refBranch1 =
        getApi()
            .createReference()
            .sourceRefName("main")
            .reference(Branch.of(branchName1, null))
            .create();
    soft.assertThat(refBranch1).isEqualTo(Branch.of(branchName1, mainHash));
    // Branch with name + hash
    Reference refBranch2 =
        getApi()
            .createReference()
            .sourceRefName("main")
            .reference(Branch.of(branchName2, mainHash))
            .create();
    soft.assertThat(refBranch2).isEqualTo(Branch.of(branchName2, mainHash));
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void getAndDeleteBranch() throws Exception {
    Branch branch = createBranch("testBranch");
    assertThat(getApi().deleteBranch().branch(branch).getAndDelete()).isEqualTo(branch);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void getAndDeleteTag() throws Exception {
    Tag tag = createTag("testTag", getApi().getDefaultBranch());
    assertThat(getApi().deleteTag().tag(tag).getAndDelete()).isEqualTo(tag);
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
        getApi()
            .commitMultipleOperations()
            .branchName(main.getName())
            .hash(main.getHash())
            .commitMeta(
                CommitMeta.builder()
                    .message("common-merge-ancestor")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Put.of(ContentKey.of("meep"), meta))
            .commit();
    String someHash = main.getHash();

    Reference createdTag =
        getApi()
            .createReference()
            .sourceRefName(main.getName())
            .reference(Tag.of(tagName, someHash))
            .create();
    soft.assertThat(createdTag).isEqualTo(Tag.of(tagName, someHash));
    Reference createdBranch1 =
        getApi()
            .createReference()
            .sourceRefName(main.getName())
            .reference(Branch.of(branchName, someHash))
            .create();
    soft.assertThat(createdBranch1).isEqualTo(Branch.of(branchName, someHash));
    Reference createdBranch2 =
        getApi()
            .createReference()
            .sourceRefName(main.getName())
            .reference(Branch.of(branchName2, someHash))
            .create();
    soft.assertThat(createdBranch2).isEqualTo(Branch.of(branchName2, someHash));

    Map<String, Reference> references =
        getApi().getAllReferences().stream()
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

    soft.assertThat(getApi().getReference().refName(tagName).get()).isEqualTo(tagRef);
    soft.assertThat(getApi().getReference().refName(branchName).get()).isEqualTo(branchRef);

    EntriesResponse entries = getApi().getEntries().refName(tagName).get();
    soft.assertThat(entries).isNotNull();
    entries = getApi().getEntries().refName(branchName).get();
    soft.assertThat(entries).isNotNull();

    LogResponse log = getApi().getCommitLog().refName(tagName).get();
    soft.assertThat(log).isNotNull();
    log = getApi().getCommitLog().refName(branchName).get();
    soft.assertThat(log).isNotNull();

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge,
    // delete) will fail
    meta = IcebergTable.of("foo", 42, 42, 42, 42);
    getApi()
        .commitMultipleOperations()
        .branchName(branchName)
        .hash(branchHash)
        .operation(Put.of(ContentKey.of("some-key"), meta))
        .commitMeta(CommitMeta.fromMessage("One dummy op"))
        .commit();
    log = getApi().getCommitLog().refName(branchName).get();
    String newHash = log.getLogEntries().get(0).getCommitMeta().getHash();

    getApi()
        .assignTag()
        .tagName(tagName)
        .hash(tagHash)
        .assignTo(Branch.of(branchName, newHash))
        .assign();
    getApi()
        .assignBranch()
        .branchName(branchName)
        .hash(newHash)
        .assignTo(Branch.of(branchName, newHash))
        .assign();

    getApi()
        .mergeRefIntoBranch()
        .branchName(branchName2)
        .hash(branchHash2)
        .fromRefName(branchName)
        .fromHash(newHash)
        .merge();
  }

  @Test
  public void filterReferences() throws BaseNessieClientServerException {
    getApi()
        .commitMultipleOperations()
        .branch(createBranch("refs.branch.1"))
        .commitMeta(CommitMeta.fromMessage("some awkward message"))
        .operation(
            Put.of(
                ContentKey.of("hello.world.BaseTable"),
                IcebergView.of("path1", 1, 1, "Spark", "SELECT ALL THE THINGS")))
        .commit();
    Branch b2 =
        getApi()
            .commitMultipleOperations()
            .branch(createBranch("other-development"))
            .commitMeta(CommitMeta.fromMessage("invent awesome things"))
            .operation(
                Put.of(
                    ContentKey.of("cool.stuff.Caresian"),
                    IcebergView.of("path2", 1, 1, "Spark", "CARTESIAN JOINS ARE AWESOME")))
            .commit();
    getApi()
        .commitMultipleOperations()
        .branch(createBranch("archive"))
        .commitMeta(CommitMeta.fromMessage("boring old stuff"))
        .operation(
            Put.of(
                ContentKey.of("super.old.Numbers"),
                IcebergView.of("path3", 1, 1, "Spark", "AGGREGATE EVERYTHING")))
        .commit();
    Tag t1 =
        (Tag)
            getApi()
                .createReference()
                .reference(Tag.of("my-tag", b2.getHash()))
                .sourceRefName(b2.getName())
                .create();

    soft.assertThat(getApi().getAllReferences().filter("ref.name == 'other-development'").stream())
        .hasSize(1)
        .allSatisfy(
            ref ->
                assertThat(ref)
                    .isInstanceOf(Branch.class)
                    .extracting(Reference::getName, Reference::getHash)
                    .containsExactly(b2.getName(), b2.getHash()));
    soft.assertThat(getApi().getAllReferences().filter("refType == 'TAG'").stream())
        .allSatisfy(ref -> assertThat(ref).isInstanceOf(Tag.class));
    soft.assertThat(getApi().getAllReferences().filter("refType == 'BRANCH'").stream())
        .allSatisfy(ref -> assertThat(ref).isInstanceOf(Branch.class));
    soft.assertThat(
            getApi()
                .getAllReferences()
                .filter("has(refMeta.numTotalCommits) && refMeta.numTotalCommits < 0")
                .stream())
        .isEmpty();
    soft.assertThat(
            getApi()
                .getAllReferences()
                .fetch(FetchOption.ALL)
                .filter("commit.message == 'invent awesome things'")
                .stream())
        .hasSize(2)
        .allSatisfy(ref -> assertThat(ref.getName()).isIn(b2.getName(), t1.getName()));
    soft.assertThat(
            getApi()
                .getAllReferences()
                .fetch(FetchOption.ALL)
                .filter("refType == 'TAG' && commit.message == 'invent awesome things'")
                .stream())
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
      Reference r =
          getApi().createReference().reference(Branch.of(branchPrefix + i, null)).create();
      String currentHash = r.getHash();
      currentHash = createCommits(r, 1, commitsPerBranch, currentHash);

      getApi()
          .createReference()
          .reference(Tag.of(tagPrefix + i, currentHash))
          .sourceRefName(r.getName())
          .create();
    }
    // not fetching additional metadata
    List<Reference> references = getApi().getAllReferences().stream().collect(Collectors.toList());
    Optional<Reference> main =
        references.stream().filter(r -> r.getName().equals("main")).findFirst();
    soft.assertThat(main).isPresent();

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
    references =
        getApi().getAllReferences().fetch(FetchOption.ALL).stream().collect(Collectors.toList());
    soft.assertThat(
            references.stream()
                .filter(r -> r.getName().startsWith(branchPrefix))
                .map(r -> (Branch) r))
        .hasSize(numBranches)
        .allSatisfy(
            branch ->
                verifyMetadataProperties(
                    commitsPerBranch, 0, branch, main.get(), commitsPerBranch));

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

    Reference r = getApi().createReference().reference(Branch.of(branchName, null)).create();
    String currentHash = r.getHash();
    currentHash = createCommits(r, 1, numCommits, currentHash);
    getApi()
        .createReference()
        .reference(Tag.of(tagName, currentHash))
        .sourceRefName(r.getName())
        .create();

    // not fetching additional metadata for a single branch
    Reference ref = getApi().getReference().refName(branchName).get();
    soft.assertThat(ref).isNotNull().isInstanceOf(Branch.class).extracting("metadata").isNull();

    // not fetching additional metadata for a single tag
    ref = getApi().getReference().refName(tagName).get();
    soft.assertThat(ref).isNotNull().isInstanceOf(Tag.class).extracting("metadata").isNull();

    // fetching additional metadata for a single branch
    ref = getApi().getReference().refName(branchName).fetch(FetchOption.ALL).get();
    soft.assertThat(ref).isNotNull().isInstanceOf(Branch.class);
    verifyMetadataProperties(
        numCommits, 0, (Branch) ref, getApi().getReference().refName("main").get(), numCommits);

    // fetching additional metadata for a single tag
    ref = getApi().getReference().refName(tagName).fetch(FetchOption.ALL).get();
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
        getApi().getCommitLog().refName(ref.getName()).maxRecords(1).get().getLogEntries();
    soft.assertThat(commits).hasSize(1);
    return commits.get(0).getCommitMeta();
  }
}
