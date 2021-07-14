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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_OR_HASH_MESSAGE;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
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
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.Hash;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableEntry;
import org.projectnessie.model.ImmutableHiveDatabase;
import org.projectnessie.model.ImmutableHiveTable;
import org.projectnessie.model.ImmutableMerge;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.SqlView.Dialect;
import org.projectnessie.model.Tag;

public abstract class AbstractTestRest {
  public static final String COMMA_VALID_HASH_1 =
      ",1234567890123456789012345678901234567890123456789012345678901234";
  public static final String COMMA_VALID_HASH_2 = ",1234567890123456789012345678901234567890";
  public static final String COMMA_VALID_HASH_3 = ",1234567890123456";

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;
  private HttpClient httpClient;

  protected void init(URI uri) {
    client = NessieClient.builder().withUri(uri).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    ObjectMapper mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));
  }

  @BeforeEach
  public void setUp() throws Exception {}

  @AfterEach
  public void tearDown() throws Exception {
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
        () ->
            assertThatThrownBy(() -> tree.createReference(Tag.of(tagName1, null)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Cannot create an unassigned tag reference"),
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
        () ->
            assertThatThrownBy(() -> tree.createReference(Hash.of("cafebabedeafbeef")))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Only tag and branch references can be created"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"normal", "with-no_space", "slash/thing"})
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

    Map<String, Reference> references =
        tree.getAllReferences().stream()
            .filter(r -> "main".equals(r.getName()) || r.getName().endsWith(refNamePart))
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

    assertThat(tree.getReferenceByName(tagName)).isEqualTo(tagRef);
    assertThat(tree.getReferenceByName(branchName)).isEqualTo(branchRef);

    EntriesResponse entries = tree.getEntries(tagName, EntriesParams.empty());
    assertThat(entries).isNotNull();
    entries = tree.getEntries(branchName, EntriesParams.empty());
    assertThat(entries).isNotNull();

    LogResponse log = tree.getCommitLog(tagName, CommitLogParams.empty());
    assertThat(log).isNotNull();
    log = tree.getCommitLog(branchName, CommitLogParams.empty());
    assertThat(log).isNotNull();

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge,
    // delete) will fail
    ImmutablePut op =
        ImmutablePut.builder()
            .key(ContentsKey.of("some-key"))
            .contents(IcebergTable.of("foo"))
            .build();
    Operations ops =
        ImmutableOperations.builder()
            .addOperations(op)
            .commitMeta(CommitMeta.fromMessage("One dummy op"))
            .build();
    tree.commitMultipleOperations(branchName, branchHash, ops);
    log = tree.getCommitLog(branchName, CommitLogParams.empty());
    String newHash = log.getOperations().get(0).getHash();

    tree.assignTag(tagName, tagHash, Tag.of(tagName, newHash));
    tree.assignBranch(branchName, newHash, Branch.of(branchName, newHash));

    tree.mergeRefIntoBranch(
        branchName2, branchHash2, ImmutableMerge.builder().fromHash(newHash).build());

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

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder().expression("commit.author == 'author-3'").build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor);
    log.getOperations().forEach(commit -> assertThat(commit.getAuthor()).isEqualTo("author-3"));

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression("commit.author == 'author-3' && commit.committer == 'random-committer'")
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression("commit.author == 'author-3' && commit.committer == ''")
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor);
    log.getOperations()
        .forEach(
            commit -> {
              assertThat(commit.getAuthor()).isEqualTo("author-3");
              assertThat(commit.getCommitter()).isEmpty();
            });

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression("commit.author in ['author-1', 'author-3', 'author-4']")
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor * 3);
    log.getOperations()
        .forEach(
            commit -> {
              assertThat(ImmutableList.of("author-1", "author-3", "author-4"))
                  .contains(commit.getAuthor());
              assertThat(commit.getCommitter()).isEmpty();
            });

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression("!(commit.author in ['author-1', 'author-0'])")
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor * 3);
    log.getOperations()
        .forEach(
            commit -> {
              assertThat(ImmutableList.of("author-2", "author-3", "author-4"))
                  .contains(commit.getAuthor());
              assertThat(commit.getCommitter()).isEmpty();
            });

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder().expression("commit.author.matches('au.*-(2|4)')").build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(commitsPerAuthor * 2);
    log.getOperations()
        .forEach(
            commit -> {
              assertThat(ImmutableList.of("author-2", "author-4")).contains(commit.getAuthor());
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

    Instant initialCommitTime =
        log.getOperations().get(log.getOperations().size() - 1).getCommitTime();
    assertThat(initialCommitTime).isNotNull();
    Instant lastCommitTime = log.getOperations().get(0).getCommitTime();
    assertThat(lastCommitTime).isNotNull();
    Instant fiveMinLater = initialCommitTime.plus(5, ChronoUnit.MINUTES);

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression(
                    String.format(
                        "timestamp(commit.commitTime) > timestamp('%s')", initialCommitTime))
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize - 1);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getCommitTime()).isAfter(initialCommitTime));

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression(
                    String.format("timestamp(commit.commitTime) < timestamp('%s')", fiveMinLater))
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getCommitTime()).isBefore(fiveMinLater));

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression(
                    String.format(
                        "timestamp(commit.commitTime) > timestamp('%s') && timestamp(commit.commitTime) < timestamp('%s')",
                        initialCommitTime, lastCommitTime))
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(expectedTotalSize - 2);
    log.getOperations()
        .forEach(
            commit ->
                assertThat(commit.getCommitTime())
                    .isAfter(initialCommitTime)
                    .isBefore(lastCommitTime));

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder()
                .expression(
                    String.format("timestamp(commit.commitTime) > timestamp('%s')", fiveMinLater))
                .build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();
  }

  @Test
  public void filterCommitLogByProperties()
      throws NessieNotFoundException, NessieConflictException {
    Reference main = tree.getReferenceByName("main");
    Branch filterCommitLogByAuthor = Branch.of("filterCommitLogByProperties", main.getHash());
    Reference branch = tree.createReference(filterCommitLogByAuthor);
    assertThat(branch).isEqualTo(filterCommitLogByAuthor);

    int numAuthors = 5;
    int commitsPerAuthor = 10;

    String currentHash = main.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    LogResponse log = tree.getCommitLog(branch.getName(), CommitLogParams.empty());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(numAuthors * commitsPerAuthor);

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder().expression("commit.properties['prop1'] == 'val1'").build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).hasSize(numAuthors * commitsPerAuthor);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getProperties().get("prop1")).isEqualTo("val1"));

    log =
        tree.getCommitLog(
            branch.getName(),
            CommitLogParams.builder().expression("commit.properties['prop1'] == 'val3'").build());
    assertThat(log).isNotNull();
    assertThat(log.getOperations()).isEmpty();
  }

  private void createCommits(
      Reference branch, int numAuthors, int commitsPerAuthor, String currentHash)
      throws NessieNotFoundException, NessieConflictException {
    for (int j = 0; j < numAuthors; j++) {
      String author = "author-" + j;
      for (int i = 0; i < commitsPerAuthor; i++) {
        String nextHash =
            tree.commitMultipleOperations(
                    branch.getName(),
                    currentHash,
                    ImmutableOperations.builder()
                        .commitMeta(
                            CommitMeta.builder()
                                .author(author)
                                .message("committed-by-" + author)
                                .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                                .build())
                        .addOperations(
                            Put.of(ContentsKey.of("table" + i), IcebergTable.of("some-file-" + i)))
                        .build())
                .getHash();
        assertThat(currentHash).isNotEqualTo(nextHash);
        currentHash = nextHash;
      }
    }
  }

  @Test
  void commitLogPagingAndFilteringByAuthor()
      throws NessieNotFoundException, NessieConflictException {
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
    List<String> messagesOfAuthorOne =
        log.getOperations().stream()
            .filter(c -> author.equals(c.getAuthor()))
            .map(CommitMeta::getMessage)
            .collect(Collectors.toList());
    verifyPaging(branchName, commits, pageSizeHint, messagesOfAuthorOne, author);

    List<String> allMessages =
        log.getOperations().stream().map(CommitMeta::getMessage).collect(Collectors.toList());
    List<CommitMeta> completeLog =
        StreamingUtil.getCommitLogStream(
                tree, branchName, CommitLogParams.builder().maxRecords(pageSizeHint).build())
            .collect(Collectors.toList());
    assertThat(completeLog.stream().map(CommitMeta::getMessage))
        .containsExactlyElementsOf(allMessages);
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
      String nextHash =
          tree.commitMultipleOperations(
                  branchName,
                  currentHash,
                  ImmutableOperations.builder()
                      .commitMeta(CommitMeta.fromMessage(msg))
                      .addOperations(
                          Put.of(ContentsKey.of("table"), IcebergTable.of("some-file-" + i)))
                      .build())
              .getHash();
      assertNotEquals(currentHash, nextHash);
      currentHash = nextHash;
    }
    Collections.reverse(allMessages);

    verifyPaging(branchName, commits, pageSizeHint, allMessages, null);

    List<CommitMeta> completeLog =
        StreamingUtil.getCommitLogStream(
                tree, branchName, CommitLogParams.builder().maxRecords(pageSizeHint).build())
            .collect(Collectors.toList());
    assertEquals(
        completeLog.stream().map(CommitMeta::getMessage).collect(Collectors.toList()), allMessages);
  }

  private void verifyPaging(
      String branchName,
      int commits,
      int pageSizeHint,
      List<String> commitMessages,
      String filterByAuthor)
      throws NessieNotFoundException {
    String pageToken = null;
    for (int pos = 0; pos < commits; pos += pageSizeHint) {
      String queryExpression = null;
      if (null != filterByAuthor) {
        queryExpression = String.format("commit.author=='%s'", filterByAuthor);
      }
      LogResponse response =
          tree.getCommitLog(
              branchName,
              CommitLogParams.builder()
                  .maxRecords(pageSizeHint)
                  .pageToken(pageToken)
                  .expression(queryExpression)
                  .build());
      if (pos + pageSizeHint <= commits) {
        assertTrue(response.hasMore());
        assertNotNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, pos + pageSizeHint),
            response.getOperations().stream()
                .map(CommitMeta::getMessage)
                .collect(Collectors.toList()));
        pageToken = response.getToken();
      } else {
        assertFalse(response.hasMore());
        assertNull(response.getToken());
        assertEquals(
            commitMessages.subList(pos, commitMessages.size()),
            response.getOperations().stream()
                .map(CommitMeta::getMessage)
                .collect(Collectors.toList()));
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
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(a).contents(ta).build())
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .build());
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(b).contents(tb).build())
            .commitMeta(CommitMeta.fromMessage("commit 2"))
            .build());
    List<ContentsWithKey> keys =
        contents
            .getMultipleContents(
                "foo", null, MultiGetContentsRequest.of(a, b, ContentsKey.of("noexist")))
            .getContents();
    List<ContentsWithKey> expected = asList(ContentsWithKey.of(a, ta), ContentsWithKey.of(b, tb));
    assertThat(keys).containsExactlyInAnyOrderElementsOf(expected);
    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  private static final class ContentAndOperationType {
    final Type type;
    final Operation operation;

    ContentAndOperationType(Type type, Operation operation) {
      this.type = type;
      this.operation = operation;
    }

    @Override
    public String toString() {
      String s;
      if (operation instanceof Put) {
        s = "Put_" + ((Put) operation).getContents().getClass().getSimpleName();
      } else {
        s = operation.getClass().getSimpleName();
      }
      return s + "_" + operation.getKey().toPathString();
    }
  }

  static Stream<ContentAndOperationType> contentAndOperationTypes() {
    return Stream.of(
        new ContentAndOperationType(
            Type.ICEBERG_TABLE,
            Put.of(ContentsKey.of("iceberg"), IcebergTable.of("/iceberg/table"))),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentsKey.of("view_dremio"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.DREMIO)
                    .sqlText("SELECT foo FROM dremio")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentsKey.of("view_hive"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.HIVE)
                    .sqlText("SELECT foo FROM hive")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentsKey.of("view_presto"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.PRESTO)
                    .sqlText("SELECT foo FROM presto")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentsKey.of("view_spark"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.SPARK)
                    .sqlText("SELECT foo FROM spark")
                    .build())),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE,
            Put.of(
                ContentsKey.of("delta"),
                ImmutableDeltaLakeTable.builder()
                    .addCheckpointLocationHistory("checkpoint")
                    .addMetadataLocationHistory("metadata")
                    .build())),
        new ContentAndOperationType(
            Type.HIVE_DATABASE,
            Put.of(
                ContentsKey.of("hivedb"),
                ImmutableHiveDatabase.builder()
                    .databaseDefinition((byte) 1, (byte) 2, (byte) 3)
                    .build())),
        new ContentAndOperationType(
            Type.HIVE_TABLE,
            Put.of(
                ContentsKey.of("hivetable"),
                ImmutableHiveTable.builder()
                    .tableDefinition((byte) 1, (byte) 2, (byte) 3)
                    .build())),
        new ContentAndOperationType(
            Type.ICEBERG_TABLE, Delete.of(ContentsKey.of("iceberg_delete"))),
        new ContentAndOperationType(
            Type.ICEBERG_TABLE, Unchanged.of(ContentsKey.of("iceberg_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentsKey.of("view_dremio_delete"))),
        new ContentAndOperationType(
            Type.VIEW, Unchanged.of(ContentsKey.of("view_dremio_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentsKey.of("view_spark_delete"))),
        new ContentAndOperationType(
            Type.VIEW, Unchanged.of(ContentsKey.of("view_spark_unchanged"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Delete.of(ContentsKey.of("delta_delete"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Unchanged.of(ContentsKey.of("delta_unchanged"))),
        new ContentAndOperationType(Type.HIVE_DATABASE, Delete.of(ContentsKey.of("hivedb_delete"))),
        new ContentAndOperationType(
            Type.HIVE_DATABASE, Unchanged.of(ContentsKey.of("hivedb_unchanged"))),
        new ContentAndOperationType(Type.HIVE_TABLE, Delete.of(ContentsKey.of("hivetable_delete"))),
        new ContentAndOperationType(
            Type.HIVE_TABLE, Unchanged.of(ContentsKey.of("hivetable_unchanged"))));
  }

  @Test
  void veriryAllContentAndOperationTypes() throws NessieNotFoundException, NessieConflictException {
    String branchName = "contentAndOperationAll";
    Reference r = tree.createReference(Branch.of(branchName, null));
    tree.commitMultipleOperations(
        branchName,
        r.getHash(),
        ImmutableOperations.builder()
            .addAllOperations(
                contentAndOperationTypes().map(c -> c.operation).collect(Collectors.toList()))
            .commitMeta(CommitMeta.fromMessage("verifyAllContentAndOperationTypes"))
            .build());
    List<Entry> entries = tree.getEntries(branchName, EntriesParams.empty()).getEntries();
    List<ImmutableEntry> expect =
        contentAndOperationTypes()
            .filter(c -> c.operation instanceof Put)
            .map(c -> Entry.builder().type(c.type).name(c.operation.getKey()).build())
            .collect(Collectors.toList());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
  }

  @ParameterizedTest
  @MethodSource("contentAndOperationTypes")
  void verifyContentAndOperationTypesIndividually(ContentAndOperationType contentAndOperationType)
      throws NessieNotFoundException, NessieConflictException {
    String branchName = "contentAndOperation_" + contentAndOperationType;
    Reference r = tree.createReference(Branch.of(branchName, null));
    tree.commitMultipleOperations(
        branchName,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(contentAndOperationType.operation)
            .commitMeta(CommitMeta.fromMessage("commit " + contentAndOperationType))
            .build());
    List<Entry> entries = tree.getEntries(branchName, EntriesParams.empty()).getEntries();
    // Oh, yea - this is weird. The property ContentAndOperationType.operation.key.namespace is null
    // (!!!)
    // here, because somehow JUnit @MethodSource implementation re-constructs the objects returned
    // from
    // the source-method contentAndOperationTypes.
    ContentsKey fixedContentKey =
        ContentsKey.of(contentAndOperationType.operation.getKey().getElements());
    List<Entry> expect =
        contentAndOperationType.operation instanceof Put
            ? singletonList(
                Entry.builder().name(fixedContentKey).type(contentAndOperationType.type).build())
            : emptyList();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
  }

  @Test
  void filterEntriesByType() throws NessieNotFoundException, NessieConflictException {
    final String branch = "filterTypes";
    Reference r = tree.createReference(Branch.of(branch, null));
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable ta = IcebergTable.of("path1");
    SqlView tb =
        ImmutableSqlView.builder().sqlText("select * from table").dialect(Dialect.DREMIO).build();
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(a).contents(ta).build())
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .build());
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(b).contents(tb).build())
            .commitMeta(CommitMeta.fromMessage("commit 2"))
            .build());
    List<Entry> entries = tree.getEntries(branch, EntriesParams.empty()).getEntries();
    List<Entry> expected =
        asList(
            Entry.builder().name(a).type(Type.ICEBERG_TABLE).build(),
            Entry.builder().name(b).type(Type.VIEW).build());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);

    entries =
        tree.getEntries(
                branch,
                EntriesParams.builder().expression("entry.contentType=='ICEBERG_TABLE'").build())
            .getEntries();
    assertEquals(singletonList(expected.get(0)), entries);

    entries =
        tree.getEntries(
                branch, EntriesParams.builder().expression("entry.contentType=='VIEW'").build())
            .getEntries();
    assertEquals(singletonList(expected.get(1)), entries);

    entries =
        tree.getEntries(
                branch,
                EntriesParams.builder()
                    .expression("entry.contentType in ['ICEBERG_TABLE', 'VIEW']")
                    .build())
            .getEntries();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);

    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  public void filterEntriesByNamespace() throws NessieConflictException, NessieNotFoundException {
    final String branch = "filterEntriesByNamespace";
    Reference r = tree.createReference(Branch.of(branch, null));
    ContentsKey first = ContentsKey.of("a", "b", "c", "firstTable");
    ContentsKey second = ContentsKey.of("a", "b", "c", "secondTable");
    ContentsKey third = ContentsKey.of("a", "thirdTable");
    ContentsKey fourth = ContentsKey.of("a", "fourthTable");
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(
                ImmutablePut.builder().key(first).contents(IcebergTable.of("path1")).build())
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .build());
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(
                ImmutablePut.builder().key(second).contents(IcebergTable.of("path2")).build())
            .commitMeta(CommitMeta.fromMessage("commit 2"))
            .build());
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(
                ImmutablePut.builder().key(third).contents(IcebergTable.of("path3")).build())
            .commitMeta(CommitMeta.fromMessage("commit 3"))
            .build());
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(
                ImmutablePut.builder().key(fourth).contents(IcebergTable.of("path4")).build())
            .commitMeta(CommitMeta.fromMessage("commit 4"))
            .build());

    List<Entry> entries = tree.getEntries(branch, EntriesParams.empty()).getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries = tree.getEntries(branch, EntriesParams.empty()).getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries =
        tree.getEntries(
                branch,
                EntriesParams.builder().expression("entry.namespace.startsWith('a.b')").build())
            .getEntries();
    assertThat(entries).hasSize(2);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a.b"));

    entries =
        tree.getEntries(
                branch,
                EntriesParams.builder().expression("entry.namespace.startsWith('a')").build())
            .getEntries();
    assertThat(entries).hasSize(4);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a"));

    entries =
        tree.getEntries(
                branch,
                EntriesParams.builder()
                    .expression("entry.namespace.startsWith('a.b.c.firstTable')")
                    .build())
            .getEntries();
    assertThat(entries).isEmpty();

    entries =
        tree.getEntries(
                branch,
                EntriesParams.builder()
                    .expression("entry.namespace.startsWith('a.fourthTable')")
                    .build())
            .getEntries();
    assertThat(entries).isEmpty();

    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  public void checkCelScriptFailureReporting() {
    assertThatThrownBy(
            () ->
                tree.getEntries(
                    "main", EntriesParams.builder().expression("invalid_script").build()))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");

    assertThatThrownBy(
            () ->
                tree.getCommitLog(
                    "main", CommitLogParams.builder().expression("invalid_script").build()))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");
  }

  @Test
  void checkSpecialCharacterRoundTrip() throws NessieNotFoundException, NessieConflictException {
    final String branch = "specialchar";
    Reference r = tree.createReference(Branch.of(branch, null));
    // ContentsKey k = ContentsKey.of("/%国","国.国");
    ContentsKey k = ContentsKey.of("a.b", "c.d");
    IcebergTable ta = IcebergTable.of("path1");
    tree.commitMultipleOperations(
        branch,
        r.getHash(),
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(k).contents(ta).build())
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .build());
    assertEquals(
        ContentsWithKey.of(k, ta),
        contents
            .getMultipleContents(branch, null, MultiGetContentsRequest.of(k))
            .getContents()
            .get(0));
    assertEquals(ta, contents.getContents(k, branch, null));
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

    String opsCountMsg =
        "commitMultipleOperations.operations.operations: size must be between 1 and 2147483647";

    assertAll(
        () ->
            assertThat(
                    assertThrows(
                            NessieBadRequestException.class,
                            () -> tree.commitMultipleOperations(invalidBranchName, validHash, ops))
                        .getMessage())
                .contains("Bad Request (HTTP/400): ")
                .contains("commitMultipleOperations.branchName: " + REF_NAME_MESSAGE)
                .contains(opsCountMsg),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): deleteBranch.branchName: " + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> tree.deleteBranch(invalidBranchName, validHash))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): getCommitLog.ref: " + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () ->
                            tree.getCommitLog(
                                invalidBranchName,
                                CommitLogParams.builder().hashOnRef(validHash).build()))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): getEntries.refName: " + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () ->
                            tree.getEntries(
                                invalidBranchName,
                                EntriesParams.builder().hashOnRef(validHash).build()))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): getReferenceByName.refName: " + REF_NAME_OR_HASH_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> tree.getReferenceByName(invalidBranchName))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): assignTag.tagName: " + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> tree.assignTag(invalidBranchName, validHash, tag))
                    .getMessage()),
        () ->
            assertThatThrownBy(() -> tree.mergeRefIntoBranch(invalidBranchName, validHash, null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400): ")
                .hasMessageContaining("mergeRefIntoBranch.branchName: " + REF_NAME_MESSAGE)
                .hasMessageContaining("mergeRefIntoBranch.merge: must not be null"),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): deleteTag.tagName: " + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> tree.deleteTag(invalidBranchName, validHash))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): transplantCommitsIntoBranch.branchName: "
                    + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () ->
                            tree.transplantCommitsIntoBranch(
                                invalidBranchName, validHash, null, null))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): getContents.ref: " + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> contents.getContents(key, invalidBranchName, validHash))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): getMultipleContents.ref: " + REF_NAME_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> contents.getMultipleContents(invalidBranchName, validHash, mgReq))
                    .getMessage()));
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
    Tag tag = Tag.of("valid", validHash);

    String opsCountMsg =
        "commitMultipleOperations.operations.operations: size must be between 1 and 2147483647";

    assertAll(
        () ->
            assertThat(
                    assertThrows(
                            NessieBadRequestException.class,
                            () -> tree.commitMultipleOperations(validBranchName, invalidHash, ops))
                        .getMessage())
                .contains("Bad Request (HTTP/400): ")
                .contains("commitMultipleOperations.hash: " + HASH_MESSAGE)
                .contains(opsCountMsg),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): deleteBranch.hash: " + HASH_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> tree.deleteBranch(validBranchName, invalidHash))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): assignTag.oldHash: " + HASH_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> tree.assignTag(validBranchName, invalidHash, tag))
                    .getMessage()),
        () ->
            assertThatThrownBy(() -> tree.mergeRefIntoBranch(validBranchName, invalidHash, null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400): ")
                .hasMessageContaining("mergeRefIntoBranch.merge: must not be null")
                .hasMessageContaining("mergeRefIntoBranch.hash: " + HASH_MESSAGE),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): deleteTag.hash: " + HASH_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () -> tree.deleteTag(validBranchName, invalidHash))
                    .getMessage()),
        () ->
            assertEquals(
                "Bad Request (HTTP/400): transplantCommitsIntoBranch.hash: " + HASH_MESSAGE,
                assertThrows(
                        NessieBadRequestException.class,
                        () ->
                            tree.transplantCommitsIntoBranch(
                                validBranchName, invalidHash, null, null))
                    .getMessage()),
        () ->
            assertThatThrownBy(() -> contents.getMultipleContents(invalidHash, null, null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400): ")
                .hasMessageContaining("getMultipleContents.request: must not be null")
                .hasMessageContaining("getMultipleContents.ref: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> contents.getMultipleContents(validBranchName, invalidHash, null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400): ")
                .hasMessageContaining("getMultipleContents.hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(() -> contents.getContents(key, validBranchName, invalidHash))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400): ")
                .hasMessageContaining("getContents.hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        tree.getCommitLog(
                            validBranchName,
                            CommitLogParams.builder().hashOnRef(invalidHash).build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400): ")
                .hasMessageContaining("getCommitLog.params.hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        tree.getEntries(
                            validBranchName,
                            EntriesParams.builder().hashOnRef(invalidHash).build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400): ")
                .hasMessageContaining("getEntries.params.hashOnRef: " + HASH_MESSAGE));
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
    // Need the string-ified JSON representation of `Tag` here, because `Tag` itself performs
    // validation.
    String tag =
        "{\"type\": \"TAG\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String branch =
        "{\"type\": \"BRANCH\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String different =
        "{\"type\": \"FOOBAR\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(null)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessage("Bad Request (HTTP/400): assignTag.tag: must not be null"),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(tag)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Cannot construct instance of "
                        + "`org.projectnessie.model.ImmutableTag`, problem: "
                        + REF_NAME_MESSAGE
                        + " - but was: "
                        + invalidTagName
                        + "\n"),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(branch)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Could not resolve type id 'BRANCH' as a subtype of "
                        + "`org.projectnessie.model.Tag`: Class `org.projectnessie.model.Branch` "
                        + "not subtype of `org.projectnessie.model.Tag`\n"),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                httpClient
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(different)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Could not resolve type id 'FOOBAR' as a subtype of "
                        + "`org.projectnessie.model.Tag`: known type ids = []\n"));
  }

  @Test
  public void testInvalidNamedRefs() {
    ContentsKey key = ContentsKey.of("x");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    String invalidRef = "1234567890123456";

    assertThatThrownBy(() -> tree.getCommitLog(invalidRef, CommitLogParams.empty()))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith("Bad Request (HTTP/400): getCommitLog.ref: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> tree.getEntries(invalidRef, EntriesParams.empty()))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith("Bad Request (HTTP/400): getEntries.refName: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> contents.getContents(key, invalidRef, null))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith("Bad Request (HTTP/400): getContents.ref: " + REF_NAME_MESSAGE);

    assertThatThrownBy(() -> contents.getMultipleContents(invalidRef, null, mgReq))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith(
            "Bad Request (HTTP/400): getMultipleContents.ref: " + REF_NAME_MESSAGE);
  }

  @Test
  public void testValidHashesOnValidNamedRefs()
      throws NessieNotFoundException, NessieConflictException {
    Reference main = tree.getReferenceByName("main");
    Branch b = Branch.of("testValidHashesOnValidNamedRefs", main.getHash());
    Reference branch = tree.createReference(b);
    assertThat(branch).isEqualTo(b);

    int commits = 10;

    String currentHash = main.getHash();
    createCommits(branch, 1, commits, currentHash);
    LogResponse entireLog = tree.getCommitLog(branch.getName(), CommitLogParams.empty());
    assertThat(entireLog).isNotNull();
    assertThat(entireLog.getOperations()).hasSize(commits);

    EntriesResponse allEntries = tree.getEntries(branch.getName(), EntriesParams.empty());
    assertThat(allEntries).isNotNull();
    assertThat(allEntries.getEntries()).hasSize(commits);

    List<ContentsKey> keys = new ArrayList<>();
    IntStream.range(0, commits).forEach(i -> keys.add(ContentsKey.of("table" + i)));

    // TODO: check where hashOnRef is set
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(keys);
    List<ContentsWithKey> allContents =
        contents.getMultipleContents(branch.getName(), null, mgReq).getContents();

    for (int i = 0; i < commits; i++) {
      String hash = entireLog.getOperations().get(i).getHash();
      LogResponse log =
          tree.getCommitLog(branch.getName(), CommitLogParams.builder().hashOnRef(hash).build());
      assertThat(log).isNotNull();
      assertThat(log.getOperations()).hasSize(commits - i);
      assertThat(ImmutableList.copyOf(entireLog.getOperations()).subList(i, commits))
          .containsExactlyElementsOf(log.getOperations());

      EntriesResponse entries =
          tree.getEntries(branch.getName(), EntriesParams.builder().hashOnRef(hash).build());
      assertThat(entries).isNotNull();
      assertThat(entries.getEntries()).hasSize(commits - i);

      int idx = commits - 1 - i;
      Contents c = this.contents.getContents(ContentsKey.of("table" + idx), branch.getName(), hash);
      assertThat(c).isNotNull().isEqualTo(allContents.get(idx).getContents());
    }
  }

  @Test
  public void testUnknownHashesOnValidNamedRefs()
      throws NessieNotFoundException, NessieConflictException {
    Reference main = tree.getReferenceByName("main");
    Branch b = Branch.of("testUnknownHashesOnValidNamedRefs", main.getHash());
    Reference branch = tree.createReference(b);
    assertThat(branch).isEqualTo(b);
    String invalidHash = "1234567890123456";

    int commits = 10;

    String currentHash = main.getHash();
    createCommits(branch, 1, commits, currentHash);
    assertThatThrownBy(
            () ->
                tree.getCommitLog(
                    branch.getName(), CommitLogParams.builder().hashOnRef(invalidHash).build()))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format("Hash %s on Ref %s could not be found", invalidHash, b.getName()));

    assertThatThrownBy(
            () ->
                tree.getEntries(
                    branch.getName(), EntriesParams.builder().hashOnRef(invalidHash).build()))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format("Hash %s on Ref %s could not be found", invalidHash, b.getName()));

    assertThatThrownBy(
            () ->
                contents
                    .getMultipleContents(
                        branch.getName(),
                        invalidHash,
                        MultiGetContentsRequest.of(ContentsKey.of("table0")))
                    .getContents())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format("Hash %s on Ref %s could not be found", invalidHash, b.getName()));

    assertThatThrownBy(
            () -> contents.getContents(ContentsKey.of("table0"), branch.getName(), invalidHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format("Hash %s on Ref %s could not be found", invalidHash, b.getName()));
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
