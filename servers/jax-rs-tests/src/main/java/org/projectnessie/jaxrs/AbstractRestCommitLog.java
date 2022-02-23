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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestCommitLog extends AbstractRestAssign {

  @Test
  public void filterCommitLogOperations() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogOperations");

    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("some awkward message"))
            .operation(
                Put.of(
                    ContentKey.of("hello", "world", "BaseTable"),
                    IcebergView.of("path1", 1, 1, "Spark", "SELECT ALL THE THINGS")))
            .operation(
                Put.of(
                    ContentKey.of("dlrow", "olleh", "BaseTable"),
                    IcebergView.of("path2", 1, 1, "Spark", "SELECT ALL THE THINGS")))
            .commit();

    assertThat(
            getApi()
                .getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.type == 'PUT')")
                .get()
                .getLogEntries())
        .hasSize(1);
    assertThat(
            getApi()
                .getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.key.startsWith('hello.world.'))")
                .get()
                .getLogEntries())
        .hasSize(1);
    assertThat(
            getApi()
                .getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.key.startsWith('not.there.'))")
                .get()
                .getLogEntries())
        .isEmpty();
    assertThat(
            getApi()
                .getCommitLog()
                .refName(branch.getName())
                .fetch(FetchOption.ALL)
                .filter("operations.exists(op, op.name == 'BaseTable')")
                .get()
                .getLogEntries())
        .hasSize(1);
    assertThat(
            getApi()
                .getCommitLog()
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
    LogResponse log = getApi().getCommitLog().refName(branch.getName()).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(numAuthors * commitsPerAuthor);

    log =
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .filter("commit.author == 'author-3'")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(commitsPerAuthor);
    log.getLogEntries()
        .forEach(commit -> assertThat(commit.getCommitMeta().getAuthor()).isEqualTo("author-3"));

    log =
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .filter("commit.author == 'author-3' && commit.committer == 'random-committer'")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).isEmpty();

    log =
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .filter("commit.author == 'author-3' && commit.committer == ''")
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(commitsPerAuthor);
    log.getLogEntries()
        .forEach(commit -> assertThat(commit.getCommitMeta().getAuthor()).isEqualTo("author-3"));

    log =
        getApi()
            .getCommitLog()
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
        getApi()
            .getCommitLog()
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
        getApi()
            .getCommitLog()
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
    LogResponse log = getApi().getCommitLog().refName(branch.getName()).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(expectedTotalSize);

    Instant initialCommitTime =
        log.getLogEntries().get(log.getLogEntries().size() - 1).getCommitMeta().getCommitTime();
    assertThat(initialCommitTime).isNotNull();
    Instant lastCommitTime = log.getLogEntries().get(0).getCommitMeta().getCommitTime();
    assertThat(lastCommitTime).isNotNull();
    Instant fiveMinLater = initialCommitTime.plus(5, ChronoUnit.MINUTES);

    log =
        getApi()
            .getCommitLog()
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
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .filter(String.format("timestamp(commit.commitTime) < timestamp('%s')", fiveMinLater))
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(expectedTotalSize);
    log.getLogEntries()
        .forEach(
            commit -> assertThat(commit.getCommitMeta().getCommitTime()).isBefore(fiveMinLater));

    log =
        getApi()
            .getCommitLog()
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
        getApi()
            .getCommitLog()
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
    LogResponse log = getApi().getCommitLog().refName(branch.getName()).get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(numAuthors * commitsPerAuthor);

    log =
        getApi()
            .getCommitLog()
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
        getApi()
            .getCommitLog()
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
    LogResponse entireLog = getApi().getCommitLog().refName(branch.getName()).get();
    assertThat(entireLog).isNotNull();
    assertThat(entireLog.getLogEntries()).hasSize(numCommits);

    // if startHash > endHash, then we return all commits starting from startHash
    String startHash = entireLog.getLogEntries().get(numCommits / 2).getCommitMeta().getHash();
    String endHash = entireLog.getLogEntries().get(0).getCommitMeta().getHash();
    LogResponse log =
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .hashOnRef(endHash)
            .untilHash(startHash)
            .get();
    assertThat(log).isNotNull();
    assertThat(log.getLogEntries()).hasSize(numCommits / 2 + 1);

    for (int i = 0, j = numCommits - 1; i < j; i++, j--) {
      startHash = entireLog.getLogEntries().get(j).getCommitMeta().getHash();
      endHash = entireLog.getLogEntries().get(i).getCommitMeta().getHash();
      log =
          getApi()
              .getCommitLog()
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

  @Test
  public void commitLogPagingAndFilteringByAuthor() throws BaseNessieClientServerException {
    Branch branch = createBranch("commitLogPagingAndFiltering");

    int numAuthors = 3;
    int commits = 45;
    int pageSizeHint = 10;
    int expectedTotalSize = numAuthors * commits;

    createCommits(branch, numAuthors, commits, branch.getHash());
    LogResponse log = getApi().getCommitLog().refName(branch.getName()).get();
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
                getApi(), branch.getName(), (c) -> {}, OptionalInt.of(pageSizeHint), false)
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

    String currentHash = branch.getHash();
    List<String> allMessages = new ArrayList<>();
    for (int i = 0; i < commits; i++) {
      String msg = "message-for-" + i;
      allMessages.add(msg);
      IcebergTable tableMeta = IcebergTable.of("some-file-" + i, 42, 42, 42, 42);
      String nextHash =
          getApi()
              .commitMultipleOperations()
              .branchName(branch.getName())
              .hash(currentHash)
              .commitMeta(CommitMeta.fromMessage(msg))
              .operation(Put.of(ContentKey.of("table"), tableMeta))
              .commit()
              .getHash();
      assertNotEquals(currentHash, nextHash);
      currentHash = nextHash;
    }
    Collections.reverse(allMessages);

    verifyPaging(branch.getName(), commits, pageSizeHint, allMessages, null);

    List<CommitMeta> completeLog =
        StreamingUtil.getCommitLogStream(
                getApi(), branch.getName(), (c) -> {}, OptionalInt.of(pageSizeHint), false)
            .map(LogEntry::getCommitMeta)
            .collect(Collectors.toList());
    assertEquals(
        completeLog.stream().map(CommitMeta::getMessage).collect(Collectors.toList()), allMessages);
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void commitLogExtended(ReferenceMode refMode) throws Exception {
    String branch = "commitLogExtended";
    String firstParent =
        getApi()
            .createReference()
            .sourceRefName("main")
            .reference(Branch.of(branch, null))
            .create()
            .getHash();

    int numCommits = 10;

    // Hack for tests running via Quarkus :(
    IntFunction<String> c1 = i -> refMode.name() + "-c1-" + i;
    IntFunction<String> c2 = i -> refMode.name() + "-c2-" + i;

    List<String> hashes =
        IntStream.rangeClosed(1, numCommits)
            .mapToObj(
                i -> {
                  try {
                    String head = getApi().getReference().refName(branch).get().getHash();
                    return getApi()
                        .commitMultipleOperations()
                        .operation(
                            Put.of(
                                ContentKey.of("k" + i),
                                IcebergTable.of("m" + i, i, i, i, i, c1.apply(i))))
                        .operation(
                            Put.of(
                                ContentKey.of("key" + i),
                                IcebergTable.of("meta" + i, i, i, i, i, c2.apply(i))))
                        .operation(Delete.of(ContentKey.of("delete" + i)))
                        .operation(Unchanged.of(ContentKey.of("key" + i)))
                        .commitMeta(CommitMeta.fromMessage("Commit #" + i))
                        .branchName(branch)
                        .hash(head)
                        .commit()
                        .getHash();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    List<String> parentHashes =
        Stream.concat(Stream.of(firstParent), hashes.subList(0, 9).stream())
            .collect(Collectors.toList());

    Reference branchRef = getApi().getReference().refName(branch).get();

    assertThat(
            Lists.reverse(
                getApi()
                    .getCommitLog()
                    .untilHash(firstParent)
                    .reference(refMode.transform(branchRef))
                    .get()
                    .getLogEntries()))
        .allSatisfy(
            c -> {
              assertThat(c.getOperations()).isNull();
              assertThat(c.getParentCommitHash()).isNull();
            })
        .extracting(e -> e.getCommitMeta().getHash())
        .containsExactlyElementsOf(hashes);

    List<LogEntry> commits =
        Lists.reverse(
            getApi()
                .getCommitLog()
                .fetch(FetchOption.ALL)
                .reference(refMode.transform(branchRef))
                .untilHash(firstParent)
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
                              IcebergTable.of("m" + i, i, i, i, i, c1.apply(i))),
                          Put.of(
                              ContentKey.of("key" + i),
                              IcebergTable.of("meta" + i, i, i, i, i, c2.apply(i)))));
            });
  }

  @Test
  public void commitLogExtendedForUnchangedOperation() throws Exception {
    String branch = "commitLogExtendedUnchanged";
    getApi()
        .createReference()
        .sourceRefName("main")
        .reference(Branch.of(branch, null))
        .create()
        .getHash();
    String head = getApi().getReference().refName(branch).get().getHash();
    getApi()
        .commitMultipleOperations()
        .operation(Unchanged.of(ContentKey.of("key1")))
        .commitMeta(CommitMeta.fromMessage("Commit #1"))
        .branchName(branch)
        .hash(head)
        .commit();

    List<LogEntry> logEntries =
        getApi().getCommitLog().fetch(FetchOption.ALL).refName(branch).get().getLogEntries();
    assertThat(logEntries.size()).isEqualTo(1);
    assertThat(logEntries.get(0).getCommitMeta().getMessage()).contains("Commit #1");
    assertThat(logEntries.get(0).getOperations()).isNull();
  }

  void verifyPaging(
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
          getApi()
              .getCommitLog()
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
}
