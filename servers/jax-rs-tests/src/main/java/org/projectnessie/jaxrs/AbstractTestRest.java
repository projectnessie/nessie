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
package org.projectnessie.jaxrs;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieRefLogNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MutableReference;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.SqlView.Dialect;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transaction;

public abstract class AbstractTestRest {
  private NessieApiV1 api;

  static {
    // Note: REST tests validate some locale-specific error messages, but expect on the messages to
    // be in ENGLISH. However, the JRE's startup classes (in particular class loaders) may cause the
    // default Locale to be initialized before Maven is able to override the user.language system
    // property. Therefore, we explicitly set the default Locale to ENGLISH here to match tests'
    // expectations.
    Locale.setDefault(Locale.ENGLISH);
  }

  protected void init(URI uri) {
    NessieApiV1 api = HttpClientBuilder.builder().withUri(uri).build(NessieApiV1.class);
    init(api);
  }

  protected void init(NessieApiV1 api) {
    this.api = api;
  }

  @BeforeEach
  public void setUp() {
    init(URI.create("http://localhost:19121/api/v1"));
  }

  @AfterEach
  public void tearDown() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    for (Reference ref : api.getAllReferences().get().getReferences()) {
      if (ref.getName().equals(defaultBranch.getName())) {
        continue;
      }
      api.deleteReference().reference(ref).delete();
    }

    api.close();
  }

  public NessieApiV1 getApi() {
    return api;
  }

  @Test
  public void testSupportedApiVersions() {
    assertThat(api.getConfig().getMaxSupportedApiVersion()).isEqualTo(1);
  }

  @Test
  public void createRecreateDefaultBranch() throws BaseNessieClientServerException {
    api.deleteReference().reference(api.getDefaultBranch()).delete();

    Reference main = api.createReference().createAs(Branch.of("main", null));
    assertThat(main).isNotNull();
    assertThat(main.getName()).isEqualTo("main");
    assertThat(main.getHash()).isNotNull();
    assertThat(api.getReference().refName("main").get()).isEqualTo(main);
  }

  @Test
  public void getAllReferences() {
    ReferencesResponse references = api.getAllReferences().get();
    assertThat(references.getReferences())
        .anySatisfy(r -> assertThat(r.getName()).isEqualTo("main"));
  }

  @Test
  public void createReferences() throws NessieNotFoundException {
    String mainHash = api.getReference().refName("main").get().getHash();

    String tagName1 = "createReferences_tag1";
    String tagName2 = "createReferences_tag2";
    String branchName1 = "createReferences_branch1";
    String branchName2 = "createReferences_branch2";

    assertAll(
        // invalid source ref & null hash
        () ->
            assertThatThrownBy(
                    () ->
                        api.createReference()
                            .sourceRefName("unknownSource")
                            .createAs(Tag.of(tagName2, null)))
                .isInstanceOf(NessieReferenceNotFoundException.class)
                .hasMessageContainingAll("'unknownSource'", "not"),
        // Tag without sourceRefName & null hash
        () ->
            assertThatThrownBy(() -> api.createReference().createAs(Tag.of(tagName1, null)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("Tag-creation requires a target named-reference and hash."),
        // Tag without hash
        () ->
            assertThatThrownBy(
                    () ->
                        api.createReference()
                            .sourceRefName("main")
                            .createAs(Tag.of(tagName1, null)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("Tag-creation requires a target named-reference and hash."),
        // legit Tag with name + hash
        () -> {
          Reference refTag1 =
              api.createReference().sourceRefName("main").createAs(Tag.of(tagName2, mainHash));
          assertEquals(Tag.of(tagName2, mainHash), refTag1);
        },
        // Branch without hash
        () -> {
          Reference refBranch1 =
              api.createReference().sourceRefName("main").createAs(Branch.of(branchName1, null));
          assertEquals(Branch.of(branchName1, mainHash), refBranch1);
        },
        // Branch with name + hash
        () -> {
          Reference refBranch2 =
              api.createReference()
                  .sourceRefName("main")
                  .createAs(Branch.of(branchName2, mainHash));
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
        api.commitMultipleOperations()
            .commitMeta(
                CommitMeta.builder()
                    .message("common-merge-ancestor")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Put.of(ContentKey.of("meep"), meta))
            .commitTo(main);
    String someHash = main.getHash();

    Reference createdTag =
        api.createReference().sourceRefName(main.getName()).createAs(Tag.of(tagName, someHash));
    assertEquals(Tag.of(tagName, someHash), createdTag);
    Reference createdBranch1 =
        api.createReference()
            .sourceRefName(main.getName())
            .createAs(Branch.of(branchName, someHash));
    assertEquals(Branch.of(branchName, someHash), createdBranch1);
    Reference createdBranch2 =
        api.createReference()
            .sourceRefName(main.getName())
            .createAs(Branch.of(branchName2, someHash));
    assertEquals(Branch.of(branchName2, someHash), createdBranch2);

    Map<String, Reference> references =
        api.getAllReferences().get().getReferences().stream()
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
    Branch branchRef = (Branch) references.get(branchName);
    Branch branchRef2 = (Branch) references.get(branchName2);

    assertThat(api.getReference().refName(tagName).get()).isEqualTo(tagRef);
    assertThat(api.getReference().refName(branchName).get()).isEqualTo(branchRef);

    EntriesResponse entries = api.getEntries().refName(tagName).get();
    assertThat(entries).isNotNull();
    entries = api.getEntries().refName(branchName).get();
    assertThat(entries).isNotNull();

    LogResponse log = api.getCommitLog().refName(tagName).get();
    assertThat(log).isNotNull();
    log = api.getCommitLog().refName(branchName).get();
    assertThat(log).isNotNull();

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge,
    // delete) will fail
    meta = IcebergTable.of("foo", 42, 42, 42, 42);
    api.commitMultipleOperations()
        .operation(Put.of(ContentKey.of("some-key"), meta))
        .commitMeta(CommitMeta.fromMessage("One dummy op"))
        .commitTo(branchRef);
    log = api.getCommitLog().refName(branchName).get();
    String newHash = log.getLogEntries().get(0).getCommitMeta().getHash();

    api.assignReference().reference(tagRef).assignTo(Branch.of(branchName, newHash)).assign();
    api.assignReference()
        .reference(branchRef.withHash(newHash))
        .assignTo(Branch.of(branchName, newHash))
        .assign();

    api.mergeRef().reference(branchRef2).fromRefName(branchName).fromHash(newHash).merge();
  }

  @Test
  public void allBranchTypes() throws BaseNessieClientServerException {

    Instant past = Instant.ofEpochMilli(Instant.now().minusSeconds(1000000L).toEpochMilli());
    Instant future = Instant.ofEpochMilli(Instant.now().plusSeconds(1000000L).toEpochMilli());

    Branch main = api.getDefaultBranch();

    Tag tag = Tag.of("tag", main.getHash());
    api.createReference().sourceRefName("main").createAs(tag);

    Branch normalBranch = Branch.of("branch-normal", main.getHash());
    api.createReference().sourceRefName("main").createAs(normalBranch);

    assertThatThrownBy(
            () -> {
              Branch alreadyExpiredBranch = Branch.of("branch-already-expired", main.getHash());
              api.createReference()
                  .sourceRefName("main")
                  .expireAt(past)
                  .createAs(alreadyExpiredBranch);
            })
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Only transaction can have an expire timestamp");

    assertThatThrownBy(
            () -> {
              Tag alreadyExpiredTag = Tag.of("tag-already-expired", main.getHash());
              api.createReference()
                  .sourceRefName("main")
                  .expireAt(past)
                  .createAs(alreadyExpiredTag);
            })
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Only transaction can have an expire timestamp");

    Transaction normalTxn = Transaction.of("txn-normal", main.getHash());
    api.createReference().sourceRefName("main").createAs(normalTxn);

    Transaction alreadyExpiredTxn = Transaction.of("txn-already-expired", main.getHash());
    api.createReference().sourceRefName("main").expireAt(past).createAs(alreadyExpiredTxn);

    Transaction notExpiredTxn = Transaction.of("txn-not-expired", main.getHash());
    api.createReference().sourceRefName("main").expireAt(future).createAs(notExpiredTxn);

    assertThat(api.getAllReferences().fetch(FetchOption.MINIMAL).get().getReferences())
        .containsExactlyInAnyOrder(main, tag, normalBranch);

    Function<Reference, Reference> adjustRef =
        r -> {
          if (r instanceof Branch) {
            return Branch.builder().from((Branch) r).metadata(null).build();
          } else if (r instanceof Transaction) {
            return Transaction.builder().from((Transaction) r).metadata(null).build();
          }
          return Tag.builder().from((Tag) r).metadata(null).build();
        };

    assertThat(api.getAllReferences().fetch(FetchOption.MINIMAL).get().getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(main, tag, normalBranch);

    //

    assertThat(
            api.getAllReferences()
                .fetch(FetchOption.MINIMAL)
                .includeExpired(true)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(main, tag, normalBranch);

    assertThat(
            api.getAllReferences()
                .fetch(FetchOption.MINIMAL)
                .includeExpired(false)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(main, tag, normalBranch);

    assertThat(
            api.getAllReferences()
                .fetch(FetchOption.MINIMAL)
                .includeTransactions(true)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(
            main, tag, normalBranch, normalTxn, alreadyExpiredTxn, notExpiredTxn);

    assertThat(
            api.getAllReferences()
                .fetch(FetchOption.MINIMAL)
                .includeTransactions(false)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(main, tag, normalBranch);

    //

    assertThat(
            api.getAllReferences()
                .includeTransactions(true)
                .includeExpired(true)
                .fetch(FetchOption.MINIMAL)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(
            main, tag, normalBranch, normalTxn, alreadyExpiredTxn, notExpiredTxn);

    assertThat(
            api.getAllReferences()
                .includeTransactions(true)
                .includeExpired(false)
                .fetch(FetchOption.MINIMAL)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(main, tag, normalBranch, normalTxn, notExpiredTxn);

    assertThat(
            api.getAllReferences()
                .includeTransactions(false)
                .includeExpired(true)
                .fetch(FetchOption.MINIMAL)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(main, tag, normalBranch);

    assertThat(
            api.getAllReferences()
                .includeTransactions(false)
                .includeExpired(false)
                .fetch(FetchOption.MINIMAL)
                .get()
                .getReferences())
        .map(adjustRef)
        .containsExactlyInAnyOrder(main, tag, normalBranch);
  }

  @Test
  public void filterReferences() throws BaseNessieClientServerException {
    Branch b1 =
        getApi()
            .commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("some awkward message"))
            .operation(
                Put.of(
                    ContentKey.of("hello.world.BaseTable"),
                    SqlView.of(SqlView.Dialect.SPARK, "SELECT ALL THE THINGS")))
            .commitTo(createBranch("refs.branch.1"));
    Branch b2 =
        getApi()
            .commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("invent awesome things"))
            .operation(
                Put.of(
                    ContentKey.of("cool.stuff.Caresian"),
                    SqlView.of(SqlView.Dialect.SPARK, "CARTESIAN JOINS ARE AWESOME")))
            .commitTo(createBranch("other-development"));
    Branch b3 =
        getApi()
            .commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("boring old stuff"))
            .operation(
                Put.of(
                    ContentKey.of("super.old.Numbers"),
                    SqlView.of(SqlView.Dialect.SPARK, "AGGREGATE EVERYTHING")))
            .commitTo(createBranch("archive"));
    Tag t1 =
        getApi()
            .createReference()
            .sourceRefName(b2.getName())
            .createAs(Tag.of("my-tag", b2.getHash()));

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
  public void filterCommitLogOperations() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogOperations");

    branch =
        getApi()
            .commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("some awkward message"))
            .operation(
                Put.of(
                    ContentKey.of("hello", "world", "BaseTable"),
                    SqlView.of(SqlView.Dialect.SPARK, "SELECT ALL THE THINGS")))
            .operation(
                Put.of(
                    ContentKey.of("dlrow", "olleh", "BaseTable"),
                    SqlView.of(SqlView.Dialect.SPARK, "SELECT ALL THE THINGS")))
            .commitTo(branch);

    assertThat(
            api.getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.type == 'PUT')")
                .get()
                .getLogEntries())
        .hasSize(1);
    assertThat(
            api.getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.key.startsWith('hello.world.'))")
                .get()
                .getLogEntries())
        .hasSize(1);
    assertThat(
            api.getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.key.startsWith('not.there.'))")
                .get()
                .getLogEntries())
        .isEmpty();
    assertThat(
            api.getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.name == 'BaseTable')")
                .get()
                .getLogEntries())
        .hasSize(1);
    assertThat(
            api.getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.name == 'ThereIsNoSuchTable')")
                .get()
                .getLogEntries())
        .isEmpty();
  }

  @Test
  public void filterCommitLogByAuthor() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByAuthor");

    int numAuthors = 5;
    int commitsPerAuthor = 10;

    String currentHash = branch.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = api.getCommitLog().refName(branch.getName()).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(numAuthors * commitsPerAuthor);

    log = api.getCommitLog().refName(branch.getName()).filter("commit.author == 'author-3'").get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(commitsPerAuthor);
    log.getLogEntries()
        .forEach(commit -> assertThat(commit.getCommitMeta().getAuthor()).isEqualTo("author-3"));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter("commit.author == 'author-3' && commit.committer == 'random-committer'")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).isEmpty();

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter("commit.author == 'author-3' && commit.committer == ''")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(commitsPerAuthor);
    log.getLogEntries()
        .forEach(commit -> assertThat(commit.getCommitMeta().getAuthor()).isEqualTo("author-3"));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter("commit.author in ['author-1', 'author-3', 'author-4']")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(commitsPerAuthor * 3);
    log.getLogEntries()
        .forEach(
            commit ->
                assertThat(ImmutableList.of("author-1", "author-3", "author-4"))
                    .contains(commit.getCommitMeta().getAuthor()));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter("!(commit.author in ['author-1', 'author-0'])")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(commitsPerAuthor * 3);
    log.getLogEntries()
        .forEach(
            commit ->
                assertThat(ImmutableList.of("author-2", "author-3", "author-4"))
                    .contains(commit.getCommitMeta().getAuthor()));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter("commit.author.matches('au.*-(2|4)')")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(commitsPerAuthor * 2);
    log.getLogEntries()
        .forEach(
            commit ->
                assertThat(ImmutableList.of("author-2", "author-4"))
                    .contains(commit.getCommitMeta().getAuthor()));
  }

  @Test
  public void filterCommitLogByTimeRange() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByTimeRange");

    int numAuthors = 5;
    int commitsPerAuthor = 10;
    int expectedTotalSize = numAuthors * commitsPerAuthor;

    String currentHash = branch.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = api.getCommitLog().refName(branch.getName()).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(expectedTotalSize);

    Instant initialCommitTime =
        log.getLogEntries().get(log.getLogEntries().size() - 1).getCommitMeta().getCommitTime();
    assertThat(initialCommitTime).isNotNull();
    Instant lastCommitTime = log.getLogEntries().get(0).getCommitMeta().getCommitTime();
    assertThat(lastCommitTime).isNotNull();
    Instant fiveMinLater = initialCommitTime.plus(5, ChronoUnit.MINUTES);

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter(
                String.format("timestamp(commit.commitTime) > timestamp('%s')", initialCommitTime))
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(expectedTotalSize - 1);
    log.getLogEntries()
        .forEach(
            commit ->
                assertThat(commit.getCommitMeta().getCommitTime()).isAfter(initialCommitTime));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter(String.format("timestamp(commit.commitTime) < timestamp('%s')", fiveMinLater))
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(expectedTotalSize);
    log.getLogEntries()
        .forEach(
            commit -> assertThat(commit.getCommitMeta().getCommitTime()).isBefore(fiveMinLater));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter(
                String.format(
                    "timestamp(commit.commitTime) > timestamp('%s') && timestamp(commit.commitTime) < timestamp('%s')",
                    initialCommitTime, lastCommitTime))
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(expectedTotalSize - 2);
    log.getLogEntries()
        .forEach(
            commit ->
                assertThat(commit.getCommitMeta().getCommitTime())
                    .isAfter(initialCommitTime)
                    .isBefore(lastCommitTime));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter(String.format("timestamp(commit.commitTime) > timestamp('%s')", fiveMinLater))
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).isEmpty();
  }

  @Test
  public void filterCommitLogByProperties() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByProperties");

    int numAuthors = 5;
    int commitsPerAuthor = 10;

    String currentHash = branch.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = api.getCommitLog().refName(branch.getName()).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(numAuthors * commitsPerAuthor);

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter("commit.properties['prop1'] == 'val1'")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(numAuthors * commitsPerAuthor);
    log.getLogEntries()
        .forEach(
            commit ->
                assertThat(commit.getCommitMeta().getProperties().get("prop1")).isEqualTo("val1"));

    log =
        api.getCommitLog()
            .refName(branch.getName())
            .filter("commit.properties['prop1'] == 'val3'")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).isEmpty();
  }

  @Test
  public void filterCommitLogByCommitRange() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByCommitRange");

    int numCommits = 10;

    String currentHash = branch.getHash();
    createCommits(branch, 1, numCommits, currentHash);
    LogResponse entireLog = api.getCommitLog().refName(branch.getName()).get();
    assertThat(entireLog).isNotNull();
    assertThat(entireLog.getLogEntries()).hasSize(numCommits);

    // if startHash > endHash, then we return all commits starting from startHash
    String startHash = entireLog.getLogEntries().get(numCommits / 2).getCommitMeta().getHash();
    String endHash = entireLog.getLogEntries().get(0).getCommitMeta().getHash();
    LogResponse log =
        api.getCommitLog().refName(branch.getName()).hashOnRef(endHash).untilHash(startHash).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(numCommits / 2 + 1);

    for (int i = 0, j = numCommits - 1; i < j; i++, j--) {
      startHash = entireLog.getLogEntries().get(j).getCommitMeta().getHash();
      endHash = entireLog.getLogEntries().get(i).getCommitMeta().getHash();
      log =
          api.getCommitLog()
              .refName(branch.getName())
              .hashOnRef(endHash)
              .untilHash(startHash)
              .get();
      assertThat(log).isNotNull();
      assertThat(log.getLogEntries()).hasSize(numCommits - (i * 2));
      assertThat(ImmutableList.copyOf(entireLog.getLogEntries()).subList(i, j + 1))
          .containsExactlyElementsOf(log.getLogEntries());
    }
  }

  protected String createCommits(
      Branch branch, int numAuthors, int commitsPerAuthor, String currentHash)
      throws BaseNessieClientServerException {
    Branch current = branch.withHash(currentHash);
    for (int j = 0; j < numAuthors; j++) {
      String author = "author-" + j;
      for (int i = 0; i < commitsPerAuthor; i++) {
        IcebergTable meta = IcebergTable.of("some-file-" + i, 42, 42, 42, 42);
        Branch newHead =
            api.commitMultipleOperations()
                .commitMeta(
                    CommitMeta.builder()
                        .author(author)
                        .message("committed-by-" + author)
                        .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                        .build())
                .operation(Put.of(ContentKey.of("table" + i), meta))
                .commitTo(current);
        assertThat(newHead).isNotEqualTo(current).isEqualTo(current.withHash(newHead.getHash()));
        current = newHead;
      }
    }
    return current.getHash();
  }

  @Test
  public void commitLogPagingAndFilteringByAuthor() throws BaseNessieClientServerException {
    Branch branch = createBranch("commitLogPagingAndFiltering");

    int numAuthors = 3;
    int commits = 45;
    int pageSizeHint = 10;
    int expectedTotalSize = numAuthors * commits;

    createCommits(branch, numAuthors, commits, branch.getHash());
    LogResponse log = api.getCommitLog().refName(branch.getName()).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(expectedTotalSize);

    String author = "author-1";
    List<String> messagesOfAuthorOne =
        log.getLogEntries().stream()
            .map(LogEntry::getCommitMeta)
            .filter(c -> author.equals(c.getAuthor()))
            .map(CommitMeta::getMessage)
            .collect(Collectors.toList());
    verifyPaging(branch.getName(), commits, pageSizeHint, messagesOfAuthorOne, author);

    List<String> allMessages =
        log.getLogEntries().stream()
            .map(LogEntry::getCommitMeta)
            .map(CommitMeta::getMessage)
            .collect(Collectors.toList());
    List<CommitMeta> completeLog =
        StreamingUtil.getCommitLogStream(
                api, branch.getName(), null, null, null, OptionalInt.of(pageSizeHint))
            .map(LogEntry::getCommitMeta)
            .collect(Collectors.toList());
    assertThat(completeLog.stream().map(CommitMeta::getMessage))
        .containsExactlyElementsOf(allMessages);
  }

  @Test
  public void commitLogPaging() throws BaseNessieClientServerException {
    Branch branch = createBranch("commitLogPaging");

    int commits = 95;
    int pageSizeHint = 10;

    Branch current = branch;
    List<String> allMessages = new ArrayList<>();
    for (int i = 0; i < commits; i++) {
      String msg = "message-for-" + i;
      allMessages.add(msg);
      IcebergTable tableMeta = IcebergTable.of("some-file-" + i, 42, 42, 42, 42);
      Branch newHead =
          api.commitMultipleOperations()
              .commitMeta(CommitMeta.fromMessage(msg))
              .operation(Put.of(ContentKey.of("table"), tableMeta))
              .commitTo(current);
      assertThat(newHead).isNotEqualTo(current).isEqualTo(current.withHash(newHead.getHash()));
      current = newHead;
    }
    Collections.reverse(allMessages);

    verifyPaging(branch.getName(), commits, pageSizeHint, allMessages, null);

    List<CommitMeta> completeLog =
        StreamingUtil.getCommitLogStream(
                api, branch.getName(), null, null, null, OptionalInt.of(pageSizeHint))
            .map(LogEntry::getCommitMeta)
            .collect(Collectors.toList());
    assertEquals(
        completeLog.stream().map(CommitMeta::getMessage).collect(Collectors.toList()), allMessages);
  }

  protected Branch createBranch(String name) throws BaseNessieClientServerException {
    Branch main = api.getDefaultBranch();
    Branch branch = Branch.of(name, main.getHash());
    Branch created = api.createReference().sourceRefName("main").createAs(branch);
    assertThat(created).isEqualTo(branch);
    return branch;
  }

  @Test
  public void transplant() throws BaseNessieClientServerException {
    Branch base = createBranch("transplant-base");
    Branch branch = createBranch("transplant-branch");

    IcebergTable table1 = IcebergTable.of("transplant-table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("transplant-table2", 43, 43, 43, 43);

    Branch committed1 =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("test-transplant-branch1"))
            .operation(Put.of(ContentKey.of("key1"), table1))
            .commitTo(branch);
    assertThat(committed1.getHash()).isNotNull();

    Branch committed2 =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("test-transplant-branch2"))
            .operation(Put.of(ContentKey.of("key1"), table1, table1))
            .commitTo(committed1);
    assertThat(committed2.getHash()).isNotNull();

    int commitsToTransplant = 2;

    LogResponse logBranch =
        api.getCommitLog()
            .refName(branch.getName())
            .untilHash(branch.getHash())
            .maxRecords(commitsToTransplant)
            .get();

    api.commitMultipleOperations()
        .commitMeta(CommitMeta.fromMessage("test-transplant-main"))
        .operation(Put.of(ContentKey.of("key2"), table2))
        .commitTo(base);

    api.transplantCommits()
        .hashesToTransplant(ImmutableList.of(committed1.getHash(), committed2.getHash()))
        .fromRefName(branch.getName())
        .reference(base)
        .transplant();

    LogResponse log = api.getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly(
            "test-transplant-branch2", "test-transplant-branch1", "test-transplant-main");

    // Verify that the commit-timestamp was updated
    LogResponse logOfTransplanted =
        api.getCommitLog().refName(base.getName()).maxRecords(commitsToTransplant).get();
    assertThat(
            logOfTransplanted.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime))
        .isNotEqualTo(
            logBranch.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime));

    assertThat(
            api.getEntries().refName(base.getName()).get().getEntries().stream()
                .map(e -> e.getName().getName()))
        .containsExactlyInAnyOrder("key1", "key2");
  }

  @Test
  public void merge() throws BaseNessieClientServerException {
    Branch base = createBranch("merge-base");
    Branch branch = createBranch("merge-branch");

    IcebergTable table1 = IcebergTable.of("merge-table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("merge-table2", 43, 43, 43, 43);

    Branch committed1 =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("test-merge-branch1"))
            .operation(Put.of(ContentKey.of("key1"), table1))
            .commitTo(branch);
    assertThat(committed1.getHash()).isNotNull();

    Branch committed2 =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("test-merge-branch2"))
            .operation(Put.of(ContentKey.of("key1"), table1, table1))
            .commitTo(committed1);
    assertThat(committed2.getHash()).isNotNull();

    int commitsToMerge = 2;

    LogResponse logBranch =
        api.getCommitLog()
            .refName(branch.getName())
            .untilHash(branch.getHash())
            .maxRecords(commitsToMerge)
            .get();

    api.commitMultipleOperations()
        .commitMeta(CommitMeta.fromMessage("test-merge-main"))
        .operation(Put.of(ContentKey.of("key2"), table2))
        .commitTo(base);

    api.mergeRef().reference(base).fromRef(committed2).merge();

    LogResponse log = api.getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly("test-merge-branch2", "test-merge-branch1", "test-merge-main");

    // Verify that the commit-timestamp was updated
    LogResponse logOfMerged =
        api.getCommitLog().refName(base.getName()).maxRecords(commitsToMerge).get();
    assertThat(
            logOfMerged.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime))
        .isNotEqualTo(
            logBranch.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime));

    assertThat(
            api.getEntries().refName(base.getName()).get().getEntries().stream()
                .map(e -> e.getName().getName()))
        .containsExactlyInAnyOrder("key1", "key2");
  }

  protected void verifyPaging(
      String branchName,
      int commits,
      int pageSizeHint,
      List<String> commitMessages,
      String filterByAuthor)
      throws NessieNotFoundException {
    String pageToken = null;
    for (int pos = 0; pos < commits; pos += pageSizeHint) {
      String filter = null;
      if (null != filterByAuthor) {
        filter = String.format("commit.author=='%s'", filterByAuthor);
      }
      LogResponse response =
          api.getCommitLog()
              .refName(branchName)
              .maxRecords(pageSizeHint)
              .pageToken(pageToken)
              .filter(filter)
              .get();
      if (pos + pageSizeHint <= commits) {
        assertTrue(response.isHasMore());
        assertNotNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, pos + pageSizeHint),
            response.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getMessage)
                .collect(Collectors.toList()));
        pageToken = response.getToken();
      } else {
        assertFalse(response.isHasMore());
        assertNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, commitMessages.size()),
            response.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getMessage)
                .collect(Collectors.toList()));
        break;
      }
    }
  }

  @Test
  public void multiget() throws BaseNessieClientServerException {
    Branch branch = createBranch("foo");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable ta = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergTable tb = IcebergTable.of("path2", 42, 42, 42, 42);
    api.commitMultipleOperations()
        .operation(Put.of(a, ta))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commitTo(branch);
    api.commitMultipleOperations()
        .operation(Put.of(b, tb))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .commitTo(branch);
    Map<ContentKey, Content> response =
        api.getContent().key(a).key(b).key(ContentKey.of("noexist")).refName("foo").get();
    assertThat(response)
        .containsEntry(a, ta)
        .containsEntry(b, tb)
        .doesNotContainKey(ContentKey.of("noexist"));
  }

  public static final class ContentAndOperationType {
    final Type type;
    final Operation operation;
    final Operation globalOperation;

    public ContentAndOperationType(Type type, Operation operation) {
      this(type, operation, null);
    }

    public ContentAndOperationType(Type type, Operation operation, Operation globalOperation) {
      this.type = type;
      this.operation = operation;
      this.globalOperation = globalOperation;
    }

    @Override
    public String toString() {
      String s = opString(operation);
      if (globalOperation != null) {
        s = "_" + opString(globalOperation);
      }
      return s + "_" + operation.getKey().toPathString();
    }

    private static String opString(Operation operation) {
      if (operation instanceof Put) {
        return "Put_" + ((Put) operation).getContent().getClass().getSimpleName();
      } else {
        return operation.getClass().getSimpleName();
      }
    }
  }

  public static Stream<ContentAndOperationType> contentAndOperationTypes() {
    return Stream.of(
        new ContentAndOperationType(
            Type.ICEBERG_TABLE,
            Put.of(ContentKey.of("iceberg"), IcebergTable.of("/iceberg/table", 42, 42, 42, 42))),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentKey.of("view_dremio"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.DREMIO)
                    .sqlText("SELECT foo FROM dremio")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentKey.of("view_presto"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.PRESTO)
                    .sqlText("SELECT foo FROM presto")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentKey.of("view_spark"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.SPARK)
                    .sqlText("SELECT foo FROM spark")
                    .build())),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE,
            Put.of(
                ContentKey.of("delta"),
                ImmutableDeltaLakeTable.builder()
                    .addCheckpointLocationHistory("checkpoint")
                    .addMetadataLocationHistory("metadata")
                    .build())),
        new ContentAndOperationType(Type.ICEBERG_TABLE, Delete.of(ContentKey.of("iceberg_delete"))),
        new ContentAndOperationType(
            Type.ICEBERG_TABLE, Unchanged.of(ContentKey.of("iceberg_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentKey.of("view_dremio_delete"))),
        new ContentAndOperationType(
            Type.VIEW, Unchanged.of(ContentKey.of("view_dremio_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentKey.of("view_spark_delete"))),
        new ContentAndOperationType(Type.VIEW, Unchanged.of(ContentKey.of("view_spark_unchanged"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Delete.of(ContentKey.of("delta_delete"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Unchanged.of(ContentKey.of("delta_unchanged"))));
  }

  @Test
  public void verifyAllContentAndOperationTypes() throws BaseNessieClientServerException {
    Branch branch = createBranch("contentAndOperationAll");

    CommitMultipleOperationsBuilder commit =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("verifyAllContentAndOperationTypes"));
    contentAndOperationTypes()
        .flatMap(
            c ->
                c.globalOperation == null
                    ? Stream.of(c.operation)
                    : Stream.of(c.operation, c.globalOperation))
        .forEach(commit::operation);
    commit.commitTo(branch);

    List<Entry> entries = api.getEntries().refName(branch.getName()).get().getEntries();
    List<Entry> expect =
        contentAndOperationTypes()
            .filter(c -> c.operation instanceof Put)
            .map(c -> Entry.builder().type(c.type).name(c.operation.getKey()).build())
            .collect(Collectors.toList());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
  }

  @ParameterizedTest
  @MethodSource("contentAndOperationTypes")
  public void verifyContentAndOperationTypesIndividually(
      ContentAndOperationType contentAndOperationType) throws BaseNessieClientServerException {
    Branch branch = createBranch("contentAndOperation_" + contentAndOperationType);
    CommitMultipleOperationsBuilder commit =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("commit " + contentAndOperationType))
            .operation(contentAndOperationType.operation);
    if (contentAndOperationType.globalOperation != null) {
      commit.operation(contentAndOperationType.globalOperation);
    }
    commit.commitTo(branch);
    List<Entry> entries = api.getEntries().refName(branch.getName()).get().getEntries();
    // Oh, yea - this is weird. The property ContentAndOperationType.operation.key.namespace is null
    // (!!!)
    // here, because somehow JUnit @MethodSource implementation re-constructs the objects returned
    // from
    // the source-method contentAndOperationTypes.
    ContentKey fixedContentKey =
        ContentKey.of(contentAndOperationType.operation.getKey().getElements());
    List<Entry> expect =
        contentAndOperationType.operation instanceof Put
            ? singletonList(
                Entry.builder().name(fixedContentKey).type(contentAndOperationType.type).build())
            : emptyList();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
  }

  @Test
  public void filterEntriesByType() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterTypes");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable tam = IcebergTable.of("path1", 42, 42, 42, 42);
    SqlView tb =
        ImmutableSqlView.builder().sqlText("select * from table").dialect(Dialect.DREMIO).build();
    api.commitMultipleOperations()
        .operation(Put.of(a, tam))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commitTo(branch);
    api.commitMultipleOperations()
        .operation(Put.of(b, tb))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .commitTo(branch);
    List<Entry> entries = api.getEntries().refName(branch.getName()).get().getEntries();
    List<Entry> expected =
        asList(
            Entry.builder().name(a).type(Type.ICEBERG_TABLE).build(),
            Entry.builder().name(b).type(Type.VIEW).build());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);

    entries =
        api.getEntries()
            .refName(branch.getName())
            .filter("entry.contentType=='ICEBERG_TABLE'")
            .get()
            .getEntries();
    assertEquals(singletonList(expected.get(0)), entries);

    entries =
        api.getEntries()
            .refName(branch.getName())
            .filter("entry.contentType=='VIEW'")
            .get()
            .getEntries();
    assertEquals(singletonList(expected.get(1)), entries);

    entries =
        api.getEntries()
            .refName(branch.getName())
            .filter("entry.contentType in ['ICEBERG_TABLE', 'VIEW']")
            .get()
            .getEntries();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void filterEntriesByNamespace() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterEntriesByNamespace");
    ContentKey first = ContentKey.of("a", "b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b", "c", "secondTable");
    ContentKey third = ContentKey.of("a", "thirdTable");
    ContentKey fourth = ContentKey.of("a", "fourthTable");
    api.commitMultipleOperations()
        .operation(Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commitTo(branch);
    api.commitMultipleOperations()
        .operation(Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .commitTo(branch);
    api.commitMultipleOperations()
        .operation(Put.of(third, IcebergTable.of("path3", 42, 42, 42, 42)))
        .commitMeta(CommitMeta.fromMessage("commit 3"))
        .commitTo(branch);
    api.commitMultipleOperations()
        .operation(Put.of(fourth, IcebergTable.of("path4", 42, 42, 42, 42)))
        .commitMeta(CommitMeta.fromMessage("commit 4"))
        .commitTo(branch);

    List<Entry> entries = api.getEntries().refName(branch.getName()).get().getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries = api.getEntries().refName(branch.getName()).get().getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries =
        api.getEntries()
            .refName(branch.getName())
            .filter("entry.namespace.startsWith('a.b')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(2);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a.b"));

    entries =
        api.getEntries()
            .refName(branch.getName())
            .filter("entry.namespace.startsWith('a')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(4);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a"));

    entries =
        api.getEntries()
            .refName(branch.getName())
            .filter("entry.namespace.startsWith('a.b.c.firstTable')")
            .get()
            .getEntries();
    assertThat(entries).isEmpty();

    entries =
        api.getEntries()
            .refName(branch.getName())
            .filter("entry.namespace.startsWith('a.fourthTable')")
            .get()
            .getEntries();
    assertThat(entries).isEmpty();

    api.deleteReference().reference(api.getReference().refName(branch.getName()).get()).delete();
  }

  @Test
  public void filterEntriesByNamespaceAndPrefixDepth() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterEntriesByNamespaceAndPrefixDepth");
    ContentKey first = ContentKey.of("a", "b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b", "c", "secondTable");
    ContentKey third = ContentKey.of("a", "thirdTable");
    ContentKey fourth = ContentKey.of("a", "b", "fourthTable");
    ContentKey fifth = ContentKey.of("a", "boo", "fifthTable");
    List<ContentKey> keys = ImmutableList.of(first, second, third, fourth, fifth);
    for (int i = 0; i < 5; i++) {
      api.commitMultipleOperations()
          .operation(Put.of(keys.get(i), IcebergTable.of("path" + i, 42, 42, 42, 42)))
          .commitMeta(CommitMeta.fromMessage("commit " + i))
          .commitTo(branch);
    }

    List<Entry> entries =
        api.getEntries().refName(branch.getName()).namespaceDepth(0).get().getEntries();
    assertThat(entries).isNotNull().hasSize(5);

    entries =
        api.getEntries()
            .refName(branch.getName())
            .namespaceDepth(0)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).isNotNull().hasSize(5);

    entries =
        api.getEntries()
            .refName(branch.getName())
            .namespaceDepth(1)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(1);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a");

    entries =
        api.getEntries()
            .refName(branch.getName())
            .namespaceDepth(2)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(3);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a.thirdTable", "a.b", "a.boo");

    entries =
        api.getEntries()
            .refName(branch.getName())
            .namespaceDepth(3)
            .filter("entry.namespace.matches('a\\\\.b(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(2);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a.b.c", "a.b.fourthTable");

    entries =
        api.getEntries()
            .refName(branch.getName())
            .namespaceDepth(4)
            .filter("entry.namespace.matches('a\\\\.b\\\\.c(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(2);
    assertThat(entries.stream().map(e -> e.getName().toPathString()))
        .containsExactlyInAnyOrder("a.b.c.firstTable", "a.b.c.secondTable");

    entries =
        api.getEntries()
            .refName(branch.getName())
            .namespaceDepth(5)
            .filter("entry.namespace.matches('(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).isEmpty();

    entries =
        api.getEntries()
            .refName(branch.getName())
            .namespaceDepth(3)
            .filter("entry.namespace.matches('(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(3);
    assertThat(entries.get(2))
        .matches(e -> e.getType().equals(Type.UNKNOWN))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "c")));
    assertThat(entries.get(1))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "fourthTable")));
    assertThat(entries.get(0))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "boo", "fifthTable")));
  }

  @Test
  public void checkCelScriptFailureReporting() {
    assertThatThrownBy(() -> api.getEntries().refName("main").filter("invalid_script").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");

    assertThatThrownBy(() -> api.getCommitLog().refName("main").filter("invalid_script").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");
  }

  @Test
  public void checkSpecialCharacterRoundTrip() throws BaseNessieClientServerException {
    Branch branch = createBranch("specialchar");
    // ContentKey k = ContentKey.of("/%国","国.国");
    ContentKey k = ContentKey.of("a.b", "c.txt");
    IcebergTable ta = IcebergTable.of("path1", 42, 42, 42, 42);
    api.commitMultipleOperations()
        .operation(Put.of(k, ta))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commitTo(branch);

    assertThat(api.getContent().key(k).refName(branch.getName()).get()).containsEntry(k, ta);
    assertEquals(ta, api.getContent().key(k).refName(branch.getName()).get().get(k));
  }

  @Test
  public void checkServerErrorPropagation() throws BaseNessieClientServerException {
    Branch branch = createBranch("bar");

    assertThatThrownBy(() -> api.createReference().sourceRefName("main").createAs(branch))
        .isInstanceOf(NessieReferenceAlreadyExistsException.class)
        .hasMessageContaining("already exists");

    assertThatThrownBy(
            () ->
                api.commitMultipleOperations()
                    .commitMeta(
                        CommitMeta.builder()
                            .author("author")
                            .message("committed-by-test")
                            .committer("disallowed-client-side-committer")
                            .build())
                    .operation(Unchanged.of(ContentKey.of("table")))
                    .commitTo(branch))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Cannot set the committer on the client side.");
  }

  @Test
  public void testInvalidNamedRefs() {
    ContentKey key = ContentKey.of("x");
    String invalidRef = "1234567890123456";

    assertThatThrownBy(() -> api.getCommitLog().refName(invalidRef).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining("getCommitLog.ref: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> api.getEntries().refName(invalidRef).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining("getEntries.refName: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> api.getContent().key(key).refName(invalidRef).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(".ref: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> api.getContent().refName(invalidRef).key(key).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(".ref: " + REF_NAME_MESSAGE);
  }

  @Test
  public void testValidHashesOnValidNamedRefs() throws BaseNessieClientServerException {
    Branch branch = createBranch("testValidHashesOnValidNamedRefs");

    int commits = 10;

    String currentHash = branch.getHash();
    createCommits(branch, 1, commits, currentHash);
    LogResponse entireLog = api.getCommitLog().refName(branch.getName()).get();
    assertThat(entireLog).isNotNull();
    assertThat(entireLog.getLogEntries()).hasSize(commits);

    EntriesResponse allEntries = api.getEntries().refName(branch.getName()).get();
    assertThat(allEntries).isNotNull();
    assertThat(allEntries.getEntries()).hasSize(commits);

    List<ContentKey> keys = new ArrayList<>();
    IntStream.range(0, commits).forEach(i -> keys.add(ContentKey.of("table" + i)));

    // TODO: check where hashOnRef is set
    Map<ContentKey, Content> allContent =
        api.getContent().keys(keys).refName(branch.getName()).get();

    for (int i = 0; i < commits; i++) {
      String hash = entireLog.getLogEntries().get(i).getCommitMeta().getHash();
      LogResponse log = api.getCommitLog().refName(branch.getName()).hashOnRef(hash).get();
      assertThat(log).isNotNull();
      assertThat(log.getLogEntries()).hasSize(commits - i);
      assertThat(ImmutableList.copyOf(entireLog.getLogEntries()).subList(i, commits))
          .containsExactlyElementsOf(log.getLogEntries());

      EntriesResponse entries = api.getEntries().refName(branch.getName()).hashOnRef(hash).get();
      assertThat(entries).isNotNull();
      assertThat(entries.getEntries()).hasSize(commits - i);

      int idx = commits - 1 - i;
      ContentKey key = ContentKey.of("table" + idx);
      Content c =
          api.getContent().key(key).refName(branch.getName()).hashOnRef(hash).get().get(key);
      assertThat(c).isNotNull().isEqualTo(allContent.get(key));
    }
  }

  @Test
  public void testUnknownHashesOnValidNamedRefs() throws BaseNessieClientServerException {
    Branch branch = createBranch("testUnknownHashesOnValidNamedRefs");
    String invalidHash = "1234567890123456";

    int commits = 10;

    String currentHash = branch.getHash();
    createCommits(branch, 1, commits, currentHash);
    assertThatThrownBy(
            () -> api.getCommitLog().refName(branch.getName()).hashOnRef(invalidHash).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));

    assertThatThrownBy(
            () -> api.getEntries().refName(branch.getName()).hashOnRef(invalidHash).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));

    assertThatThrownBy(
            () ->
                api.getContent()
                    .key(ContentKey.of("table0"))
                    .refName(branch.getName())
                    .hashOnRef(invalidHash)
                    .get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));

    assertThatThrownBy(
            () ->
                api.getContent()
                    .key(ContentKey.of("table0"))
                    .refName(branch.getName())
                    .hashOnRef(invalidHash)
                    .get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));
  }

  /** Assigning a branch/tag to a fresh main without any commits didn't work in 0.9.2 */
  @Test
  public void testAssignRefToFreshMain() throws BaseNessieClientServerException {
    Reference main = api.getReference().refName("main").get();
    // make sure main doesn't have any commits
    LogResponse log = api.getCommitLog().refName(main.getName()).get();
    assertThat(log.getLogEntries()).isEmpty();

    Branch testBranch = createBranch("testBranch");
    api.assignReference().reference(testBranch).assignTo(main).assign();
    Reference testBranchRef = api.getReference().refName(testBranch.getName()).get();
    assertThat(testBranchRef.getHash()).isEqualTo(main.getHash());

    String testTag = "testTag";
    Reference testTagRef =
        api.createReference()
            .sourceRefName(main.getName())
            .createAs(Tag.of(testTag, main.getHash()));
    assertThat(testTagRef.getHash()).isNotNull();
    api.assignReference().reference(Tag.of(testTag, testTagRef.getHash())).assignTo(main).assign();
    testTagRef = api.getReference().refName(testTag).get();
    assertThat(testTagRef.getHash()).isEqualTo(main.getHash());
  }

  @Test
  public void testReferencesHaveMetadataProperties() throws BaseNessieClientServerException {
    String branchPrefix = "branchesHaveMetadataProperties";
    String tagPrefix = "tagsHaveMetadataProperties";
    int numBranches = 5;
    int commitsPerBranch = 10;

    for (int i = 0; i < numBranches; i++) {
      Branch r = api.createReference().createAs(Branch.of(branchPrefix + i, null));
      String currentHash = r.getHash();
      currentHash = createCommits(r, 1, commitsPerBranch, currentHash);

      api.createReference().sourceRefName(r.getName()).createAs(Tag.of(tagPrefix + i, currentHash));
    }
    // not fetching additional metadata
    List<Reference> references = api.getAllReferences().get().getReferences();
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
    references = api.getAllReferences().fetch(FetchOption.ALL).get().getReferences();
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

    Branch r = api.createReference().createAs(Branch.of(branchName, null));
    String currentHash = r.getHash();
    currentHash = createCommits(r, 1, numCommits, currentHash);
    api.createReference().sourceRefName(r.getName()).createAs(Tag.of(tagName, currentHash));

    // not fetching additional metadata for a single branch
    Reference ref = api.getReference().refName(branchName).get();
    assertThat(ref).isNotNull().isInstanceOf(Branch.class);
    assertThat(ref).isNotNull().isInstanceOf(Branch.class).extracting("metadata").isNull();

    // not fetching additional metadata for a single tag
    ref = api.getReference().refName(tagName).get();
    assertThat(ref).isNotNull().isInstanceOf(Tag.class).extracting("metadata").isNull();

    // fetching additional metadata for a single branch
    ref = api.getReference().refName(branchName).fetch(FetchOption.ALL).get();
    assertThat(ref).isNotNull().isInstanceOf(Branch.class);
    verifyMetadataProperties(
        numCommits, 0, (Branch) ref, api.getReference().refName("main").get(), numCommits);

    // fetching additional metadata for a single tag
    ref = api.getReference().refName(tagName).fetch(FetchOption.ALL).get();
    assertThat(ref).isNotNull().isInstanceOf(Tag.class);
    verifyMetadataProperties((Tag) ref);
  }

  private void verifyMetadataProperties(
      int expectedCommitsAhead,
      int expectedCommitsBehind,
      Branch branch,
      Reference reference,
      long expectedCommits)
      throws NessieNotFoundException {
    List<LogEntry> commits =
        api.getCommitLog().refName(branch.getName()).maxRecords(1).get().getLogEntries();
    assertThat(commits).hasSize(1);
    CommitMeta commitMeta = commits.get(0).getCommitMeta();

    ReferenceMetadata referenceMetadata = branch.getMetadata();
    assertThat(referenceMetadata)
        .isNotNull()
        .extracting(
            ReferenceMetadata::getNumCommitsAhead,
            ReferenceMetadata::getNumCommitsBehind,
            ReferenceMetadata::getCommitMetaOfHEAD,
            ReferenceMetadata::getCommonAncestorHash,
            ReferenceMetadata::getNumTotalCommits,
            ReferenceMetadata::getExpireAt)
        .containsExactly(
            expectedCommitsAhead,
            expectedCommitsBehind,
            commitMeta,
            reference.getHash(),
            expectedCommits,
            null);
  }

  private void verifyMetadataProperties(Tag tag) throws NessieNotFoundException {
    List<LogEntry> commits =
        api.getCommitLog().refName(tag.getName()).maxRecords(1).get().getLogEntries();
    assertThat(commits).hasSize(1);
    CommitMeta commitMeta = commits.get(0).getCommitMeta();

    ReferenceMetadata referenceMetadata = tag.getMetadata();
    assertThat(referenceMetadata)
        .isNotNull()
        .extracting(
            ReferenceMetadata::getNumCommitsAhead,
            ReferenceMetadata::getNumCommitsBehind,
            ReferenceMetadata::getCommitMetaOfHEAD,
            ReferenceMetadata::getCommonAncestorHash,
            ReferenceMetadata::getNumTotalCommits,
            ReferenceMetadata::getExpireAt)
        .containsExactly(null, null, commitMeta, null, 10L, null);
  }

  @Test
  public void commitLogExtended() throws Exception {
    String branch = "commitLogExtended";
    String firstParent =
        api.createReference().sourceRefName("main").createAs(Branch.of(branch, null)).getHash();

    int numCommits = 10;

    List<String> hashes =
        IntStream.rangeClosed(1, numCommits)
            .mapToObj(
                i -> {
                  try {
                    MutableReference head =
                        (MutableReference) api.getReference().refName(branch).get();
                    return api.commitMultipleOperations()
                        .operation(
                            Put.of(
                                ContentKey.of("k" + i),
                                IcebergTable.of("m" + i, i, i, i, i, "c" + i)))
                        .operation(
                            Put.of(
                                ContentKey.of("key" + i),
                                IcebergTable.of("meta" + i, i, i, i, i, "cid" + i)))
                        .operation(Delete.of(ContentKey.of("delete" + i)))
                        .operation(Unchanged.of(ContentKey.of("key" + i)))
                        .commitMeta(CommitMeta.fromMessage("Commit #" + i))
                        .commitTo(head)
                        .getHash();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    List<String> parentHashes =
        Stream.concat(Stream.of(firstParent), hashes.subList(0, 9).stream())
            .collect(Collectors.toList());

    assertThat(
            Lists.reverse(
                api.getCommitLog().untilHash(firstParent).refName(branch).get().getLogEntries()))
        .allSatisfy(
            c -> {
              assertThat(c.getOperations()).isNull();
              assertThat(c.getParentCommitHash()).isNull();
            })
        .extracting(e -> e.getCommitMeta().getHash())
        .containsExactlyElementsOf(hashes);

    List<LogEntry> commits =
        Lists.reverse(
            api.getCommitLog()
                .fetch(FetchOption.ALL)
                .untilHash(firstParent)
                .refName(branch)
                .get()
                .getLogEntries());
    assertThat(IntStream.rangeClosed(1, numCommits))
        .allSatisfy(
            i -> {
              LogEntry c = commits.get(i - 1);
              assertThat(c)
                  .extracting(
                      e -> e.getCommitMeta().getMessage(),
                      e -> e.getCommitMeta().getHash(),
                      LogEntry::getParentCommitHash,
                      LogEntry::getOperations)
                  .containsExactly(
                      "Commit #" + i,
                      hashes.get(i - 1),
                      parentHashes.get(i - 1),
                      Arrays.asList(
                          Delete.of(ContentKey.of("delete" + i)),
                          Put.of(
                              ContentKey.of("k" + i),
                              IcebergTable.of("m" + i, i, i, i, i, "c" + i)),
                          Put.of(
                              ContentKey.of("key" + i),
                              IcebergTable.of("meta" + i, i, i, i, i, "cid" + i))));
            });
  }

  @Test
  public void testDiff() throws BaseNessieClientServerException {
    int commitsPerBranch = 10;

    Branch fromRef = api.createReference().createAs(Branch.of("testDiffFromRef", null));
    Branch toRef = api.createReference().createAs(Branch.of("testDiffToRef", null));
    String toRefHash = createCommits(toRef, 1, commitsPerBranch, toRef.getHash());

    // we only committed to toRef, the "from" diff should be null
    assertThat(
            api.getDiff()
                .fromRefName(fromRef.getName())
                .toRefName(toRef.getName())
                .get()
                .getDiffs())
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNull();
              assertThat(diff.getTo()).isNotNull();
            });

    // after committing to fromRef, "from/to" diffs should both have data
    createCommits(fromRef, 1, commitsPerBranch, fromRef.getHash());

    assertThat(
            api.getDiff()
                .fromRefName(fromRef.getName())
                .toRefName(toRef.getName())
                .get()
                .getDiffs())
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNotNull();
              assertThat(diff.getTo()).isNotNull();

              // we only have a diff on the ID
              assertThat(diff.getFrom().getId()).isNotEqualTo(diff.getTo().getId());
              Optional<IcebergTable> fromTable = diff.getFrom().unwrap(IcebergTable.class);
              assertThat(fromTable).isPresent();
              Optional<IcebergTable> toTable = diff.getTo().unwrap(IcebergTable.class);
              assertThat(toTable).isPresent();

              assertThat(fromTable.get().getMetadataLocation())
                  .isEqualTo(toTable.get().getMetadataLocation());
              assertThat(fromTable.get().getSchemaId()).isEqualTo(toTable.get().getSchemaId());
              assertThat(fromTable.get().getSnapshotId()).isEqualTo(toTable.get().getSnapshotId());
              assertThat(fromTable.get().getSortOrderId())
                  .isEqualTo(toTable.get().getSortOrderId());
              assertThat(fromTable.get().getSpecId()).isEqualTo(toTable.get().getSpecId());
            });

    List<ContentKey> keys =
        IntStream.rangeClosed(0, commitsPerBranch)
            .mapToObj(i -> ContentKey.of("table" + i))
            .collect(Collectors.toList());
    // request all keys and delete the tables for them on toRef
    Map<ContentKey, Content> map = api.getContent().refName(toRef.getName()).keys(keys).get();
    for (Map.Entry<ContentKey, Content> entry : map.entrySet()) {
      toRef =
          api.commitMultipleOperations()
              .commitMeta(CommitMeta.fromMessage("delete"))
              .operation(Delete.of(entry.getKey()))
              .commitTo(toRef.withHash(toRefHash));
    }

    // now that we deleted all tables on toRef, the diff for "to" should be null
    assertThat(
            api.getDiff()
                .fromRefName(fromRef.getName())
                .toRefName(toRef.getName())
                .get()
                .getDiffs())
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNotNull();
              assertThat(diff.getTo()).isNull();
            });
  }

  @Test
  public void commitLogExtendedForUnchangedOperation() throws Exception {
    String branch = "commitLogExtendedUnchanged";
    api.createReference().sourceRefName("main").createAs(Branch.of(branch, null)).getHash();
    MutableReference head = (MutableReference) api.getReference().refName(branch).get();
    api.commitMultipleOperations()
        .operation(Unchanged.of(ContentKey.of("key1")))
        .commitMeta(CommitMeta.fromMessage("Commit #1"))
        .commitTo(head);

    List<LogEntry> logEntries =
        api.getCommitLog().fetch(FetchOption.ALL).refName(branch).get().getLogEntries();
    assertThat(logEntries.size()).isEqualTo(1);
    assertThat(logEntries.get(0).getCommitMeta().getMessage()).contains("Commit #1");
    assertThat(logEntries.get(0).getOperations()).isNull();
  }

  @Test
  public void testReflog() throws BaseNessieClientServerException {
    String tagName = "tag1_test_reflog";
    String branch1 = "branch1_test_reflog";
    String branch2 = "branch2_test_reflog";
    String branch3 = "branch3_test_reflog";
    String root = "ref_name_test_reflog";

    List<Tuple> expectedEntries = new ArrayList<>(12);

    // reflog 1: creating the default branch0
    Branch branch0 = createBranch(root);
    expectedEntries.add(Tuple.tuple(root, "CREATE_REFERENCE"));

    // reflog 2: create tag1
    Tag createdTag =
        api.createReference()
            .sourceRefName(branch0.getName())
            .createAs(Tag.of(tagName, branch0.getHash()));
    expectedEntries.add(Tuple.tuple(tagName, "CREATE_REFERENCE"));

    // reflog 3: create branch1
    Branch createdBranch1 =
        api.createReference()
            .sourceRefName(branch0.getName())
            .createAs(Branch.of(branch1, branch0.getHash()));
    expectedEntries.add(Tuple.tuple(branch1, "CREATE_REFERENCE"));

    // reflog 4: create branch2
    Branch createdBranch2 =
        api.createReference()
            .sourceRefName(branch0.getName())
            .createAs(Branch.of(branch2, branch0.getHash()));
    expectedEntries.add(Tuple.tuple(branch2, "CREATE_REFERENCE"));

    // reflog 5: create branch2
    Branch createdBranch3 =
        api.createReference()
            .sourceRefName(branch0.getName())
            .createAs(Branch.of(branch3, branch0.getHash()));
    expectedEntries.add(Tuple.tuple(branch3, "CREATE_REFERENCE"));

    // reflog 6: commit on default branch0
    IcebergTable meta = IcebergTable.of("meep", 42, 42, 42, 42);
    branch0 =
        api.commitMultipleOperations()
            .commitMeta(
                CommitMeta.builder()
                    .message("dummy commit log")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Put.of(ContentKey.of("meep"), meta))
            .commitTo(branch0);
    expectedEntries.add(Tuple.tuple(root, "COMMIT"));

    // reflog 7: assign tag
    api.assignReference()
        .reference(Tag.of(tagName, createdTag.getHash()))
        .assignTo(branch0)
        .assign();
    expectedEntries.add(Tuple.tuple(tagName, "ASSIGN_REFERENCE"));

    // reflog 8: assign ref
    api.assignReference()
        .reference(Branch.of(branch1, createdBranch1.getHash()))
        .assignTo(branch0)
        .assign();
    expectedEntries.add(Tuple.tuple(branch1, "ASSIGN_REFERENCE"));

    // reflog 9: merge
    api.mergeRef()
        .reference(createdBranch2)
        .fromRefName(branch1)
        .fromHash(branch0.getHash())
        .merge();
    expectedEntries.add(Tuple.tuple(branch2, "MERGE"));

    // reflog 10: transplant
    api.transplantCommits()
        .hashesToTransplant(ImmutableList.of(Objects.requireNonNull(branch0.getHash())))
        .fromRefName(branch1)
        .reference(createdBranch3)
        .transplant();
    expectedEntries.add(Tuple.tuple(branch3, "TRANSPLANT"));

    // reflog 11: delete branch
    api.deleteReference().reference(Branch.of(branch1, branch0.getHash())).delete();
    expectedEntries.add(Tuple.tuple(branch1, "DELETE_REFERENCE"));

    // reflog 12: delete tag
    api.deleteReference().reference(Tag.of(tagName, branch0.getHash())).delete();
    expectedEntries.add(Tuple.tuple(tagName, "DELETE_REFERENCE"));

    // In the reflog output new entry will be the head. Hence, reverse the expected list
    Collections.reverse(expectedEntries);

    RefLogResponse refLogResponse = api.getRefLog().get();
    // verify reflog entries
    assertThat(refLogResponse.getLogEntries().subList(0, 12))
        .extracting(
            RefLogResponse.RefLogResponseEntry::getRefName,
            RefLogResponse.RefLogResponseEntry::getOperation)
        .isEqualTo(expectedEntries);
    // verify pagination (limit and token)
    RefLogResponse refLogResponse1 = api.getRefLog().maxRecords(2).get();
    assertThat(refLogResponse1.getLogEntries())
        .isEqualTo(refLogResponse.getLogEntries().subList(0, 2));
    assertThat(refLogResponse1.isHasMore()).isTrue();
    RefLogResponse refLogResponse2 = api.getRefLog().pageToken(refLogResponse1.getToken()).get();
    // should start from the token.
    assertThat(refLogResponse2.getLogEntries().get(0).getRefLogId())
        .isEqualTo(refLogResponse1.getToken());
    assertThat(refLogResponse2.getLogEntries().subList(0, 10))
        .isEqualTo(refLogResponse.getLogEntries().subList(2, 12));
    // verify startHash and endHash
    RefLogResponse refLogResponse3 =
        api.getRefLog().fromHash(refLogResponse.getLogEntries().get(10).getRefLogId()).get();
    assertThat(refLogResponse3.getLogEntries().subList(0, 2))
        .isEqualTo(refLogResponse.getLogEntries().subList(10, 12));
    RefLogResponse refLogResponse4 =
        api.getRefLog()
            .fromHash(refLogResponse.getLogEntries().get(3).getRefLogId())
            .untilHash(refLogResponse.getLogEntries().get(5).getRefLogId())
            .get();
    assertThat(refLogResponse4.getLogEntries())
        .isEqualTo(refLogResponse.getLogEntries().subList(3, 6));

    // use invalid reflog id f1234d75178d892a133a410355a5a990cf75d2f33eba25d575943d4df632f3a4
    // computed using Hash.of(
    //    UnsafeByteOperations.unsafeWrap(newHasher().putString("invalid",
    // StandardCharsets.UTF_8).hash().asBytes()));
    assertThatThrownBy(
            () ->
                api.getRefLog()
                    .fromHash("f1234d75178d892a133a410355a5a990cf75d2f33eba25d575943d4df632f3a4")
                    .get())
        .isInstanceOf(NessieRefLogNotFoundException.class)
        .hasMessageContaining(
            "RefLog entry for 'f1234d75178d892a133a410355a5a990cf75d2f33eba25d575943d4df632f3a4' does not exist");
  }
}
