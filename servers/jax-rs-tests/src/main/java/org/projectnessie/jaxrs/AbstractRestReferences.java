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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
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
  @Test
  public void createRecreateDefaultBranch() throws BaseNessieClientServerException {
    getApi().deleteBranch().branch(getApi().getDefaultBranch()).delete();

    Reference main = getApi().createReference().reference(Branch.of("main", null)).create();
    assertThat(main).isNotNull();
    assertThat(main.getName()).isEqualTo("main");
    assertThat(main.getHash()).isNotNull();
    assertThat(getApi().getReference().refName("main").get()).isEqualTo(main);
  }

  @Test
  public void getAllReferences() {
    ReferencesResponse references = getApi().getAllReferences().get();
    assertThat(references.getReferences())
        .anySatisfy(r -> assertThat(r.getName()).isEqualTo("main"));
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
  public void createReferences() throws NessieNotFoundException {
    String mainHash = getApi().getReference().refName("main").get().getHash();

    String tagName1 = "createReferences_tag1";
    String tagName2 = "createReferences_tag2";
    String branchName1 = "createReferences_branch1";
    String branchName2 = "createReferences_branch2";

    assertAll(
        // invalid source ref & null hash
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .createReference()
                            .sourceRefName("unknownSource")
                            .reference(Tag.of(tagName2, null))
                            .create())
                .isInstanceOf(NessieReferenceNotFoundException.class)
                .hasMessageContainingAll("'unknownSource'", "not"),
        // Tag without sourceRefName & null hash
        () ->
            assertThatThrownBy(
                    () -> getApi().createReference().reference(Tag.of(tagName1, null)).create())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("Tag-creation requires a target named-reference and hash."),
        // Tag without hash
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .createReference()
                            .sourceRefName("main")
                            .reference(Tag.of(tagName1, null))
                            .create())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("Tag-creation requires a target named-reference and hash."),
        // legit Tag with name + hash
        () -> {
          Reference refTag1 =
              getApi()
                  .createReference()
                  .sourceRefName("main")
                  .reference(Tag.of(tagName2, mainHash))
                  .create();
          assertEquals(Tag.of(tagName2, mainHash), refTag1);
        },
        // Branch without hash
        () -> {
          Reference refBranch1 =
              getApi()
                  .createReference()
                  .sourceRefName("main")
                  .reference(Branch.of(branchName1, null))
                  .create();
          assertEquals(Branch.of(branchName1, mainHash), refBranch1);
        },
        // Branch with name + hash
        () -> {
          Reference refBranch2 =
              getApi()
                  .createReference()
                  .sourceRefName("main")
                  .reference(Branch.of(branchName2, mainHash))
                  .create();
          assertEquals(Branch.of(branchName2, mainHash), refBranch2);
        });
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
    assertEquals(Tag.of(tagName, someHash), createdTag);
    Reference createdBranch1 =
        getApi()
            .createReference()
            .sourceRefName(main.getName())
            .reference(Branch.of(branchName, someHash))
            .create();
    assertEquals(Branch.of(branchName, someHash), createdBranch1);
    Reference createdBranch2 =
        getApi()
            .createReference()
            .sourceRefName(main.getName())
            .reference(Branch.of(branchName2, someHash))
            .create();
    assertEquals(Branch.of(branchName2, someHash), createdBranch2);

    Map<String, Reference> references =
        getApi().getAllReferences().get().getReferences().stream()
            .filter(r -> root.equals(r.getName()) || r.getName().endsWith(refNamePart))
            .collect(Collectors.toMap(Reference::getName, Function.identity()));

    assertThat(references)
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
    assertThat(references.get(main.getName())).isInstanceOf(Branch.class);
    assertThat(references.get(createdTag.getName())).isInstanceOf(Tag.class);
    assertThat(references.get(createdBranch1.getName())).isInstanceOf(Branch.class);
    assertThat(references.get(createdBranch2.getName())).isInstanceOf(Branch.class);

    Reference tagRef = references.get(tagName);
    Reference branchRef = references.get(branchName);
    Reference branchRef2 = references.get(branchName2);

    String tagHash = tagRef.getHash();
    String branchHash = branchRef.getHash();
    String branchHash2 = branchRef2.getHash();

    assertThat(getApi().getReference().refName(tagName).get()).isEqualTo(tagRef);
    assertThat(getApi().getReference().refName(branchName).get()).isEqualTo(branchRef);

    EntriesResponse entries = getApi().getEntries().refName(tagName).get();
    assertThat(entries).isNotNull();
    entries = getApi().getEntries().refName(branchName).get();
    assertThat(entries).isNotNull();

    LogResponse log = getApi().getCommitLog().refName(tagName).get();
    assertThat(log).isNotNull();
    log = getApi().getCommitLog().refName(branchName).get();
    assertThat(log).isNotNull();

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
    Branch b1 =
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
    Branch b3 =
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

    assertThat(
            getApi()
                .getAllReferences()
                .filter("ref.name == 'other-development'")
                .get()
                .getReferences())
        .hasSize(1)
        .allSatisfy(
            ref ->
                assertThat(ref)
                    .isInstanceOf(Branch.class)
                    .extracting(Reference::getName, Reference::getHash)
                    .containsExactly(b2.getName(), b2.getHash()));
    assertThat(getApi().getAllReferences().filter("refType == 'TAG'").get().getReferences())
        .allSatisfy(ref -> assertThat(ref).isInstanceOf(Tag.class));
    assertThat(getApi().getAllReferences().filter("refType == 'BRANCH'").get().getReferences())
        .allSatisfy(ref -> assertThat(ref).isInstanceOf(Branch.class));
    assertThat(
            getApi()
                .getAllReferences()
                .filter("has(refMeta.numTotalCommits) && refMeta.numTotalCommits < 0")
                .get()
                .getReferences())
        .isEmpty();
    assertThat(
            getApi()
                .getAllReferences()
                .fetch(FetchOption.ALL)
                .filter("commit.message == 'invent awesome things'")
                .get()
                .getReferences())
        .hasSize(2)
        .allSatisfy(ref -> assertThat(ref.getName()).isIn(b2.getName(), t1.getName()));
    assertThat(
            getApi()
                .getAllReferences()
                .fetch(FetchOption.ALL)
                .filter("refType == 'TAG' && commit.message == 'invent awesome things'")
                .get()
                .getReferences())
        .hasSize(1)
        .allSatisfy(ref -> assertThat(ref.getName()).isEqualTo(t1.getName()));
  }

  @Test
  public void testReferencesHaveMetadataProperties() throws BaseNessieClientServerException {
    String branchPrefix = "branchesHaveMetadataProperties";
    String tagPrefix = "tagsHaveMetadataProperties";
    int numBranches = 5;
    int commitsPerBranch = 10;

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
    List<Reference> references = getApi().getAllReferences().get().getReferences();
    Optional<Reference> main =
        references.stream().filter(r -> r.getName().equals("main")).findFirst();
    assertThat(main).isPresent();

    assertThat(
            references.stream()
                .filter(r -> r.getName().startsWith(branchPrefix))
                .map(r -> (Branch) r))
        .hasSize(numBranches)
        .allSatisfy(branch -> assertThat(branch.getMetadata()).isNull());

    assertThat(references.stream().filter(r -> r.getName().startsWith(tagPrefix)).map(r -> (Tag) r))
        .hasSize(numBranches)
        .allSatisfy(tag -> assertThat(tag.getMetadata()).isNull());

    // fetching additional metadata for each reference
    references = getApi().getAllReferences().fetch(FetchOption.ALL).get().getReferences();
    assertThat(
            references.stream()
                .filter(r -> r.getName().startsWith(branchPrefix))
                .map(r -> (Branch) r))
        .hasSize(numBranches)
        .allSatisfy(
            branch ->
                verifyMetadataProperties(
                    commitsPerBranch, 0, branch, main.get(), commitsPerBranch));

    assertThat(references.stream().filter(r -> r.getName().startsWith(tagPrefix)).map(r -> (Tag) r))
        .hasSize(numBranches)
        .allSatisfy(this::verifyMetadataProperties);
  }

  @Test
  public void testSingleReferenceHasMetadataProperties() throws BaseNessieClientServerException {
    String branchName = "singleBranchHasMetadataProperties";
    String tagName = "singleTagHasMetadataProperties";
    int numCommits = 10;

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
    assertThat(ref).isNotNull().isInstanceOf(Branch.class);
    assertThat(ref).isNotNull().isInstanceOf(Branch.class).extracting("metadata").isNull();

    // not fetching additional metadata for a single tag
    ref = getApi().getReference().refName(tagName).get();
    assertThat(ref).isNotNull().isInstanceOf(Tag.class).extracting("metadata").isNull();

    // fetching additional metadata for a single branch
    ref = getApi().getReference().refName(branchName).fetch(FetchOption.ALL).get();
    assertThat(ref).isNotNull().isInstanceOf(Branch.class);
    verifyMetadataProperties(
        numCommits, 0, (Branch) ref, getApi().getReference().refName("main").get(), numCommits);

    // fetching additional metadata for a single tag
    ref = getApi().getReference().refName(tagName).fetch(FetchOption.ALL).get();
    assertThat(ref).isNotNull().isInstanceOf(Tag.class);
    verifyMetadataProperties((Tag) ref);
  }

  void verifyMetadataProperties(
      int expectedCommitsAhead,
      int expectedCommitsBehind,
      Branch branch,
      Reference reference,
      long expectedCommits)
      throws NessieNotFoundException {
    List<LogEntry> commits =
        getApi().getCommitLog().refName(branch.getName()).maxRecords(1).get().getLogEntries();
    assertThat(commits).hasSize(1);
    CommitMeta commitMeta = commits.get(0).getCommitMeta();

    ReferenceMetadata referenceMetadata = branch.getMetadata();
    assertThat(referenceMetadata).isNotNull();
    assertThat(referenceMetadata.getNumCommitsAhead()).isEqualTo(expectedCommitsAhead);
    assertThat(referenceMetadata.getNumCommitsBehind()).isEqualTo(expectedCommitsBehind);
    assertThat(referenceMetadata.getCommitMetaOfHEAD()).isEqualTo(commitMeta);
    assertThat(referenceMetadata.getCommonAncestorHash()).isEqualTo(reference.getHash());
    assertThat(referenceMetadata.getNumTotalCommits()).isEqualTo(expectedCommits);
  }

  void verifyMetadataProperties(Tag tag) throws NessieNotFoundException {
    List<LogEntry> commits =
        getApi().getCommitLog().refName(tag.getName()).maxRecords(1).get().getLogEntries();
    assertThat(commits).hasSize(1);
    CommitMeta commitMeta = commits.get(0).getCommitMeta();

    ReferenceMetadata referenceMetadata = tag.getMetadata();
    assertThat(referenceMetadata).isNotNull();
    assertThat(referenceMetadata.getNumCommitsAhead()).isNull();
    assertThat(referenceMetadata.getNumCommitsBehind()).isNull();
    assertThat(referenceMetadata.getCommitMetaOfHEAD()).isEqualTo(commitMeta);
    assertThat(referenceMetadata.getCommonAncestorHash()).isNull();
    assertThat(referenceMetadata.getNumTotalCommits()).isEqualTo(10);
  }
}
