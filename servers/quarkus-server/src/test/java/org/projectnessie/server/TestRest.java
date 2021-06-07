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

package org.projectnessie.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_OR_HASH_MESSAGE;

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.CommitLogParams.Builder;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Hash;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableMerge;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.Tag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class TestRest {

  public static final String COMMA_VALID_HASH_1 = ",1234567890123456789012345678901234567890123456789012345678901234";
  public static final String COMMA_VALID_HASH_2 = ",1234567890123456789012345678901234567890";
  public static final String COMMA_VALID_HASH_3 = ",1234567890123456";

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;
  private HttpClient httpClient;

  @BeforeEach
  void init() {
    URI uri = URI.create("http://localhost:19121/api/v1");
    client = NessieClient.builder().withUri(uri).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));
  }

  @AfterEach
  void closeClient() {
    client.close();
  }

  @Test
  void createReferences() throws NessieNotFoundException {
    String mainHash = tree.getReferenceByName("main").getHash();

    String tagName1 = "createReferences_tag1";
    String tagName2 = "createReferences_tag2";
    String branchName1 = "createReferences_branch1";
    String branchName2 = "createReferences_branch2";
    assertAll(
        // Tag without hash
        () -> assertThatThrownBy(() -> tree.createReference(Tag.of(tagName1, null)))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageStartingWith("Bad Request (HTTP/400): Cannot create an unassigned tag reference"),
        // legit Tag with name + hash
        () -> {
          Reference refTag1 = tree.createReference(Tag.of(tagName2, mainHash));
          assertEquals(Tag.of(tagName2, mainHash), refTag1);
        },
        // Branch without hash
        () -> {
          Reference refBranch1 = tree.createReference(Branch.of(branchName1, null));
          assertEquals(Branch.of(branchName1, mainHash), refBranch1);
        },
        // Branch with name + hash
        () -> {
          Reference refBranch2 = tree.createReference(Branch.of(branchName2, mainHash));
          assertEquals(Branch.of(branchName2, mainHash), refBranch2);
        },
        // Hash
        () -> assertThatThrownBy(() -> tree.createReference(Hash.of("cafebabedeafbeef")))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageStartingWith("Bad Request (HTTP/400): Only tag and branch references can be created"));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "normal",
      "with-no_space",
      "slash/thing"
  })
  void referenceNames(String refNamePart) throws NessieNotFoundException, NessieConflictException {
    String tagName = "tag" + refNamePart;
    String branchName = "branch" + refNamePart;
    String branchName2 = "branch2" + refNamePart;

    Reference main = tree.getReferenceByName("main");
    String someHash = main.getHash();

    Reference createdTag = tree.createReference(Tag.of(tagName, someHash));
    assertEquals(Tag.of(tagName, someHash), createdTag);
    Reference createdBranch1 = tree.createReference(Branch.of(branchName, someHash));
    assertEquals(Branch.of(branchName, someHash), createdBranch1);
    Reference createdBranch2 = tree.createReference(Branch.of(branchName2, someHash));
    assertEquals(Branch.of(branchName2, someHash), createdBranch2);

    Map<String, Reference> references = tree.getAllReferences().stream()
        .filter(r -> "main".equals(r.getName()) || r.getName().endsWith(refNamePart))
        .collect(Collectors.toMap(Reference::getName, Function.identity()));

    assertThat(references).containsAllEntriesOf(
        Map.of(main.getName(), main,
            createdTag.getName(), createdTag,
            createdBranch1.getName(), createdBranch1,
            createdBranch2.getName(), createdBranch2));
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

    assertThat(tree.getReferenceByName(tagName)).isEqualTo(tagRef);
    assertThat(tree.getReferenceByName(branchName)).isEqualTo(branchRef);

    EntriesResponse entries = tree.getEntries(tagName, null, null, Collections.emptyList());
    assertThat(entries).isNotNull();
    entries = tree.getEntries(branchName, null, null, Collections.emptyList());
    assertThat(entries).isNotNull();

    LogResponse log = tree.getCommitLog(tagName, CommitLogParams.empty());
    assertThat(log).isNotNull();
    log = tree.getCommitLog(branchName, CommitLogParams.empty());
    assertThat(log).isNotNull();

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge, delete) will fail
    ImmutablePut op = ImmutablePut.builder().key(ContentsKey.of("some-key")).contents(IcebergTable.of("foo")).build();
    Operations ops = ImmutableOperations.builder().addOperations(op).commitMeta(CommitMeta.fromMessage("One dummy op")).build();
    tree.commitMultipleOperations(branchName, branchHash, ops);
    log = tree.getCommitLog(branchName, CommitLogParams.empty());
    String newHash = log.getOperations().get(0).getHash();

    tree.assignTag(tagName, tagHash, Tag.of(tagName, newHash));
    tree.assignBranch(branchName, newHash, Branch.of(branchName, newHash));

    tree.mergeRefIntoBranch(branchName2, branchHash2, ImmutableMerge.builder().fromHash(newHash).build());

    tree.deleteTag(tagName, newHash);
    tree.deleteBranch(branchName, newHash);
  }

  @Test
  public void filterCommitLogByAuthor() throws NessieNotFoundException, NessieConflictException {
    Reference main = tree.getReferenceByName("main");
    Branch filterCommitLogByAuthor = Branch.of("filterCommitLogByAuthor", main.getHash());
    Reference branch = tree.createReference(filterCommitLogByAuthor);
    assertThat(branch).isEqualTo(filterCommitLogByAuthor);

    int numAuthors = 5;
    int commitsPerAuthor = 10;

    String currentHash = main.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = tree.getCommitLog(branch.getName(), CommitLogParams.empty());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(numAuthors * commitsPerAuthor);

    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().authors(ImmutableList.of("author-3")).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor);
    log.getOperations().forEach(commit -> assertThat(commit.getAuthor()).isEqualTo("author-3"));

    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().authors(ImmutableList.of("author-3"))
        .committers(ImmutableList.of("random-committer")).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();

    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().authors(ImmutableList.of("author-3"))
        .committers(ImmutableList.of("")).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor);
    log.getOperations().forEach(commit -> {
      assertThat(commit.getAuthor()).isEqualTo("author-3");
      assertThat(commit.getCommitter()).isEmpty();
    });

    List<String> authors = ImmutableList.of("author-1", "author-3", "author-4");
    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().authors(authors).committers(ImmutableList.of("")).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor * authors.size());
    log.getOperations().forEach(commit -> {
      assertThat(authors).contains(commit.getAuthor());
      assertThat(commit.getCommitter()).isEmpty();
    });
  }

  @Test
  public void filterCommitLogByTimeRange() throws NessieNotFoundException, NessieConflictException {
    Reference main = tree.getReferenceByName("main");
    Branch filterCommitLogByAuthor = Branch.of("filterCommitLogByTimeRange", main.getHash());
    Reference branch = tree.createReference(filterCommitLogByAuthor);
    assertThat(branch).isEqualTo(filterCommitLogByAuthor);

    int numAuthors = 5;
    int commitsPerAuthor = 10;
    int expectedTotalSize = numAuthors * commitsPerAuthor;

    String currentHash = main.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = tree.getCommitLog(branch.getName(), CommitLogParams.empty());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize);

    Instant initialCommitTime = log.getOperations().get(log.getOperations().size() - 1).getCommitTime();
    assertThat(initialCommitTime).isNotNull();
    Instant lastCommitTime = log.getOperations().get(0).getCommitTime();
    assertThat(lastCommitTime).isNotNull();
    Instant fiveMinLater = initialCommitTime.plus(5, ChronoUnit.MINUTES);


    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().after(initialCommitTime).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize - 1);
    log.getOperations().forEach(commit -> assertThat(commit.getCommitTime()).isAfter(initialCommitTime));

    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().before(fiveMinLater).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize);
    log.getOperations().forEach(commit -> assertThat(commit.getCommitTime()).isBefore(fiveMinLater));

    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().after(initialCommitTime).before(lastCommitTime).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize - 2);
    log.getOperations().forEach(commit -> assertThat(commit.getCommitTime()).isAfter(initialCommitTime).isBefore(lastCommitTime));

    log = tree.getCommitLog(branch.getName(), CommitLogParams.builder().after(fiveMinLater).build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();
  }

  private void createCommits(Reference branch, int numAuthors, int commitsPerAuthor, String currentHash)
      throws NessieNotFoundException, NessieConflictException {
    for (int j = 0; j < numAuthors; j++) {
      String author = "author-" + j;
      for (int i = 0; i < commitsPerAuthor; i++) {
        String nextHash = tree.commitMultipleOperations(branch.getName(), currentHash,
            ImmutableOperations.builder()
                .commitMeta(CommitMeta.builder().author(author)
                    .message("committed-by-" + author)
                    .build())
                .addOperations(Put.of(ContentsKey.of("table"), IcebergTable.of("some-file-" + i)))
                .build()).getHash();
        assertThat(currentHash).isNotEqualTo(nextHash);
        currentHash = nextHash;
      }
    }
  }

  @Test
  void commitLogPagingAndFilteringByAuthor() throws NessieNotFoundException, NessieConflictException {
    String someHash = tree.getReferenceByName("main").getHash();
    String branchName = "commitLogPagingAndFiltering";
    Branch branch = Branch.of(branchName, someHash);
    tree.createReference(branch);

    int numAuthors = 3;
    int commits = 45;
    int pageSizeHint = 10;
    int expectedTotalSize = numAuthors * commits;

    createCommits(branch, numAuthors, commits, someHash);
    LogResponse log = tree.getCommitLog(branch.getName(), CommitLogParams.empty());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize);

    String author = "author-1";
    List<String> messagesOfAuthorOne = log.getOperations()
        .stream()
        .filter(c -> author.equals(c.getAuthor()))
        .map(CommitMeta::getMessage)
        .collect(Collectors.toList());
    verifyPaging(branchName, commits, pageSizeHint, messagesOfAuthorOne, author);

    List<String> allMessages = log.getOperations().stream().map(CommitMeta::getMessage).collect(Collectors.toList());
    List<CommitMeta> completeLog = StreamingUtil.getCommitLogStream(tree, branchName, CommitLogParams.builder()
        .maxRecords(pageSizeHint)
        .build())
        .collect(Collectors.toList());
    assertThat(completeLog.stream().map(CommitMeta::getMessage)).containsExactlyElementsOf(allMessages);
  }

  @Test
  void commitLogPaging() throws NessieNotFoundException, NessieConflictException {
    String someHash = tree.getReferenceByName("main").getHash();
    String branchName = "commitLogPaging";
    Branch branch = Branch.of(branchName, someHash);
    tree.createReference(branch);

    int commits = 95;
    int pageSizeHint = 10;

    String currentHash = someHash;
    List<String> allMessages = new ArrayList<>();
    for (int i = 0; i < commits; i++) {
      String msg = "message-for-" + i;
      allMessages.add(msg);
      String nextHash = tree.commitMultipleOperations(branchName, currentHash,
          ImmutableOperations.builder()
              .commitMeta(CommitMeta.fromMessage(msg))
              .addOperations(Put.of(ContentsKey.of("table"), IcebergTable.of("some-file-" + i)))
              .build()).getHash();
      assertNotEquals(currentHash, nextHash);
      currentHash = nextHash;
    }
    Collections.reverse(allMessages);

    verifyPaging(branchName, commits, pageSizeHint, allMessages, null);

    List<CommitMeta> completeLog = StreamingUtil.getCommitLogStream(tree, branchName, CommitLogParams.builder()
        .maxRecords(pageSizeHint)
        .build())
        .collect(Collectors.toList());
    assertEquals(
        completeLog.stream().map(CommitMeta::getMessage).collect(Collectors.toList()),
        allMessages
    );
  }

  private void verifyPaging(String branchName, int commits, int pageSizeHint, List<String> commitMessages, String filterByAuthor)
      throws NessieNotFoundException {
    String pageToken = null;
    for (int pos = 0; pos < commits; pos += pageSizeHint) {
      Builder builder = CommitLogParams.builder().maxRecords(pageSizeHint).pageToken(pageToken);
      if (null != filterByAuthor) {
        builder = builder.authors(ImmutableList.of(filterByAuthor));
      }
      CommitLogParams commitLogParams = builder.build();
      LogResponse response = tree.getCommitLog(branchName, commitLogParams);
      if (pos + pageSizeHint <= commits) {
        assertTrue(response.hasMore());
        assertNotNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, pos + pageSizeHint),
            response.getOperations().stream().map(CommitMeta::getMessage).collect(Collectors.toList())
        );
        pageToken = response.getToken();
      } else {
        assertFalse(response.hasMore());
        assertNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, commitMessages.size()),
            response.getOperations().stream().map(CommitMeta::getMessage).collect(Collectors.toList())
        );
        break;
      }
    }
  }

  @Test
  void multiget() throws NessieNotFoundException, NessieConflictException {
    final String branch = "foo";
    Reference r = tree.createReference(Branch.of(branch, null));
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable ta = IcebergTable.of("path1");
    IcebergTable tb = IcebergTable.of("path2");
    contents.setContents(a, branch, r.getHash(), "commit 1", ta);
    contents.setContents(b, branch, r.getHash(), "commit 2", tb);
    List<ContentsWithKey> keys =
        contents.getMultipleContents("foo", MultiGetContentsRequest.of(a, b, ContentsKey.of("noexist"))).getContents();
    List<ContentsWithKey> expected = Arrays.asList(ContentsWithKey.of(a, ta), ContentsWithKey.of(b, tb));
    assertThat(keys).containsExactlyInAnyOrderElementsOf(expected);
    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void filters() throws NessieNotFoundException, NessieConflictException {
    final String branch = "filterTypes";
    Reference r = tree.createReference(Branch.of(branch, null));
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable ta = IcebergTable.of("path1");
    SqlView tb = ImmutableSqlView.builder().sqlText("select * from table")
        .dialect(SqlView.Dialect.DREMIO).build();
    contents.setContents(a, branch, r.getHash(), "commit 1", ta);
    contents.setContents(b, branch, r.getHash(), "commit 2", tb);
    List<EntriesResponse.Entry> entries = tree.getEntries(branch, null, null, Collections.emptyList()).getEntries();
    List<EntriesResponse.Entry> expected = Arrays.asList(
        EntriesResponse.Entry.builder().name(a).type(Contents.Type.ICEBERG_TABLE).build(),
        EntriesResponse.Entry.builder().name(b).type(Contents.Type.VIEW).build());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);
    entries = tree.getEntries(branch, null, null, ImmutableList.of(Contents.Type.ICEBERG_TABLE.name())).getEntries();
    assertEquals(Collections.singletonList(expected.get(0)), entries);

    entries = tree.getEntries(branch, null, null, ImmutableList.of(Contents.Type.VIEW.name())).getEntries();
    assertEquals(Collections.singletonList(expected.get(1)), entries);

    entries = tree.getEntries(branch, null, null, ImmutableList.of(Contents.Type.VIEW.name(),
        Contents.Type.ICEBERG_TABLE.name())).getEntries();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);

    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void checkSpecialCharacterRoundTrip() throws NessieNotFoundException, NessieConflictException {
    final String branch = "specialchar";
    Reference r = tree.createReference(Branch.of(branch, null));
    //ContentsKey k = ContentsKey.of("/%国","国.国");
    ContentsKey k = ContentsKey.of("a.b", "c.d");
    IcebergTable ta = IcebergTable.of("path1");
    contents.setContents(k, branch, r.getHash(), "commit 1", ta);
    assertEquals(ContentsWithKey.of(k, ta), contents.getMultipleContents(branch, MultiGetContentsRequest.of(k)).getContents().get(0));
    assertEquals(ta, contents.getContents(k, branch));
    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void checkServerErrorPropagation() throws NessieNotFoundException, NessieConflictException {
    final String branch = "bar";
    tree.createReference(Branch.of(branch, null));
    assertThatThrownBy(() -> tree.createReference(Branch.of(branch, null)))
        .isInstanceOf(NessieConflictException.class)
        .hasMessageContaining("already exists");
  }

  @ParameterizedTest
  @CsvSource({
      "x/" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidBranchNames(String invalidBranchName, String validHash) {
    Operations ops = ImmutableOperations.builder().commitMeta(CommitMeta.fromMessage("")).build();
    ContentsKey key = ContentsKey.of("x");
    Contents cts = IcebergTable.of("moo");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    Tag tag = Tag.of("valid", validHash);

    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): commitMultipleOperations.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.commitMultipleOperations(invalidBranchName, validHash, ops)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteBranch.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteBranch(invalidBranchName, validHash)).getMessage()),
        () ->  assertEquals("Bad Request (HTTP/400): getCommitLog.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getCommitLog(invalidBranchName, CommitLogParams.empty())).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getEntries.refName: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getEntries(invalidBranchName, null, null, Collections.emptyList())).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getReferenceByName.refName: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getReferenceByName(invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): assignTag.tagName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.assignTag(invalidBranchName, validHash, tag)).getMessage()),
        () -> assertThatThrownBy(() -> tree.mergeRefIntoBranch(invalidBranchName, validHash, null))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageContaining("Bad Request (HTTP/400): ")
            .hasMessageContaining("mergeRefIntoBranch.branchName: " + REF_NAME_MESSAGE)
            .hasMessageContaining("mergeRefIntoBranch.merge: must not be null"),
        () -> assertEquals("Bad Request (HTTP/400): deleteTag.tagName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteTag(invalidBranchName, validHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): transplantCommitsIntoBranch.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.transplantCommitsIntoBranch(invalidBranchName, validHash, null, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): setContents.branch: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.setContents(key, invalidBranchName, validHash, null, cts)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteContents.branch: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.deleteContents(key, invalidBranchName, validHash, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getContents.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.getContents(key, invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getMultipleContents.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.getMultipleContents(invalidBranchName, mgReq)).getMessage())
    );
  }

  @ParameterizedTest
  @CsvSource({
      "" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidHashes(String invalidHashIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidHash = invalidHashIn != null ? invalidHashIn : "";

    String validBranchName = "hello";
    Operations ops = ImmutableOperations.builder().commitMeta(CommitMeta.fromMessage("")).build();
    ContentsKey key = ContentsKey.of("x");
    Contents cts = IcebergTable.of("moo");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    Tag tag = Tag.of("valid", validHash);

    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): commitMultipleOperations.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.commitMultipleOperations(validBranchName, invalidHash, ops)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteBranch.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteBranch(validBranchName, invalidHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): assignTag.oldHash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.assignTag(validBranchName, invalidHash, tag)).getMessage()),
        () -> assertThatThrownBy(() -> tree.mergeRefIntoBranch(validBranchName, invalidHash, null))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageContaining("Bad Request (HTTP/400): ")
            .hasMessageContaining("mergeRefIntoBranch.merge: must not be null")
            .hasMessageContaining("mergeRefIntoBranch.hash: " + HASH_MESSAGE),
        () -> assertEquals("Bad Request (HTTP/400): deleteTag.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteTag(validBranchName, invalidHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): transplantCommitsIntoBranch.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.transplantCommitsIntoBranch(validBranchName, invalidHash, null, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): setContents.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.setContents(key, validBranchName, invalidHash, null, cts)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteContents.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.deleteContents(key, validBranchName, invalidHash, null)).getMessage()),
        () -> assertThatThrownBy(() -> contents.getMultipleContents(invalidHash, null))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageContaining("Bad Request (HTTP/400): ")
            .hasMessageContaining("getMultipleContents.request: must not be null")
            .hasMessageContaining("getMultipleContents.ref: " + REF_NAME_OR_HASH_MESSAGE)
    );
  }

  @ParameterizedTest
  @CsvSource({
      "" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidTags(String invalidTagNameIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidTagName = invalidTagNameIn != null ? invalidTagNameIn : "";

    String validBranchName = "hello";
    ContentsKey key = ContentsKey.of("x");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    // Need the string-ified JSON representation of `Tag` here, because `Tag` itself performs
    // validation.
    String tag = "{\"type\": \"TAG\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    String branch = "{\"type\": \"BRANCH\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    String different = "{\"type\": \"FOOBAR\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    assertAll(
        () -> assertThatThrownBy(() -> unwrap(() ->
            httpClient.newRequest().path("trees/tag/{tagName}")
                .resolveTemplate("tagName", validBranchName)
                .queryParam("expectedHash", validHash)
                .put(null)))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessage("Bad Request (HTTP/400): assignTag.tag: must not be null"),
        () -> assertThatThrownBy(() -> unwrap(() ->
            httpClient.newRequest().path("trees/tag/{tagName}")
                .resolveTemplate("tagName", validBranchName)
                .queryParam("expectedHash", validHash)
                .put(tag)))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageStartingWith("Bad Request (HTTP/400): Cannot construct instance of "
                + "`org.projectnessie.model.ImmutableTag`, problem: "
                + REF_NAME_MESSAGE + " - but was: " + invalidTagName + "\n"),
        () -> assertThatThrownBy(() -> unwrap(() ->
            httpClient.newRequest().path("trees/tag/{tagName}")
                .resolveTemplate("tagName", validBranchName)
                .queryParam("expectedHash", validHash)
                .put(branch)))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageStartingWith("Bad Request (HTTP/400): Could not resolve type id 'BRANCH' as a subtype of "
                + "`org.projectnessie.model.Tag`: Class `org.projectnessie.model.Branch` "
                + "not subtype of `org.projectnessie.model.Tag`\n"),
        () -> assertThatThrownBy(() -> unwrap(() ->
            httpClient.newRequest().path("trees/tag/{tagName}")
                .resolveTemplate("tagName", validBranchName)
                .queryParam("expectedHash", validHash)
                .put(different)))
            .isInstanceOf(NessieBadRequestException.class)
            .hasMessageStartingWith("Bad Request (HTTP/400): Could not resolve type id 'FOOBAR' as a subtype of "
                + "`org.projectnessie.model.Tag`: known type ids = []\n")
    );
  }

  void unwrap(Executable exec) throws Throwable {
    try {
      exec.execute();
    } catch (Throwable targetException) {
      if (targetException instanceof HttpClientException) {
        if (targetException.getCause() instanceof NessieNotFoundException
            || targetException.getCause() instanceof NessieConflictException) {
          throw targetException.getCause();
        }
      }

      throw targetException;
    }
  }
}
