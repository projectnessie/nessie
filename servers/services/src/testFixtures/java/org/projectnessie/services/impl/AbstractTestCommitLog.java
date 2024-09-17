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

import static com.google.common.collect.Lists.reverse;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.ALL;
import static org.projectnessie.model.FetchOption.MINIMAL;
import static org.projectnessie.versioned.RequestMeta.API_READ;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.CommitResponse.AddedContent;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Reference;
import org.projectnessie.versioned.DetachedRef;

public abstract class AbstractTestCommitLog extends BaseTestServiceImpl {

  @Test
  public void commitResponse() throws BaseNessieClientServerException {
    Branch branch = createBranch("commitResponse");
    ContentKey key1 = ContentKey.of("test");
    ContentKey key2 = ContentKey.of("testFoo");
    CommitResponse response =
        commit(
            branch,
            fromMessage("test"),
            Put.of(key1, IcebergTable.of("loc", 1, 2, 3, 4)),
            Put.of(key2, IcebergTable.of("blah", 1, 2, 3, 4)));
    soft.assertThat(response).isNotNull();
    soft.assertThat(response.getAddedContents())
        .isNotNull()
        .hasSize(2)
        .allSatisfy(ac -> assertThat(ac.contentId()).isNotNull())
        .extracting(AddedContent::getKey)
        .containsExactlyInAnyOrder(key1, key2);
    soft.assertThat(response.getTargetBranch()).isEqualTo(getReference(branch.getName()));

    Map<ContentKey, String> contentIds =
        requireNonNull(response.getAddedContents()).stream()
            .collect(Collectors.toMap(AddedContent::getKey, AddedContent::contentId));

    Map<ContentKey, Content> contents = contents(response.getTargetBranch(), key1, key2);

    soft.assertThat(contentIds.keySet()).isEqualTo(contents.keySet());
    soft.assertThat(contentIds.get(key1)).isEqualTo(contents.get(key1).getId());
    soft.assertThat(contentIds.get(key2)).isEqualTo(contents.get(key2).getId());

    response =
        commit(
            response.getTargetBranch(),
            fromMessage("test"),
            Put.of(key1, IcebergTable.of("loc", 1, 2, 3, 4, contentIds.get(key1))),
            Put.of(key2, IcebergTable.of("blah", 1, 2, 3, 4, contentIds.get(key2))));
    soft.assertThat(response.getAddedContents()).isNull();
  }

  @Test
  public void filterCommitLogOperations() throws BaseNessieClientServerException {
    ContentKey hello = ContentKey.of("hello", "world", "BaseTable");
    ContentKey ollah = ContentKey.of("dlrow", "olleh", "BaseTable");

    Branch branch =
        ensureNamespacesForKeysExist(createBranch("filterCommitLogOperations"), hello, ollah);

    branch =
        commit(
                branch,
                fromMessage("some awkward message"),
                Put.of(hello, IcebergView.of("path1", 1, 1)),
                Put.of(ollah, IcebergView.of("path2", 1, 1)))
            .getTargetBranch();

    soft.assertThat(
            commitLog(
                branch.getName(),
                ALL,
                "operations.exists(op, op.type == 'PUT') && commit.message != 'create namespaces'"))
        .hasSize(1);
    soft.assertThat(
            commitLog(
                branch.getName(),
                ALL,
                "operations.exists(op, op.key.startsWith('hello.world.')) && commit.message != 'create namespaces'"))
        .hasSize(1);
    soft.assertThat(
            commitLog(
                branch.getName(),
                ALL,
                "operations.exists(op, op.key.startsWith('not.there.')) && commit.message != 'create namespaces'"))
        .isEmpty();
    soft.assertThat(
            commitLog(
                branch.getName(),
                ALL,
                "operations.exists(op, op.name == 'BaseTable') && commit.message != 'create namespaces'"))
        .hasSize(1);
    soft.assertThat(
            commitLog(
                branch.getName(),
                ALL,
                "operations.exists(op, op.name == 'ThereIsNoSuchTable') && commit.message != 'create namespaces'"))
        .isEmpty();
  }

  @Test
  public void filterCommitLogByAuthor() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByAuthor");

    int numAuthors = 5;
    int commitsPerAuthor = 3;

    String currentHash = branch.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    List<LogEntry> log = commitLog(branch.getName());
    assertThat(log).hasSize(numAuthors * commitsPerAuthor);

    log = commitLog(branch.getName(), null, "commit.author == 'author-3'");
    soft.assertThat(log)
        .hasSize(commitsPerAuthor)
        .allSatisfy(commit -> assertThat(commit.getCommitMeta().getAuthor()).isEqualTo("author-3"));

    log =
        commitLog(
            branch.getName(),
            null,
            "commit.author == 'author-3' && commit.committer == 'random-committer'");
    soft.assertThat(log).isEmpty();

    log = commitLog(branch.getName(), null, "commit.author == 'author-3'");
    soft.assertThat(log)
        .hasSize(commitsPerAuthor)
        .allSatisfy(commit -> assertThat(commit.getCommitMeta().getAuthor()).isEqualTo("author-3"));

    log =
        commitLog(branch.getName(), null, "commit.author in ['author-1', 'author-3', 'author-4']");
    soft.assertThat(log)
        .hasSize(commitsPerAuthor * 3)
        .allSatisfy(
            commit ->
                assertThat(ImmutableList.of("author-1", "author-3", "author-4"))
                    .contains(commit.getCommitMeta().getAuthor()));

    log = commitLog(branch.getName(), null, "!(commit.author in ['author-1', 'author-0'])");
    soft.assertThat(log)
        .hasSize(commitsPerAuthor * 3)
        .allSatisfy(
            commit ->
                assertThat(ImmutableList.of("author-2", "author-3", "author-4"))
                    .contains(commit.getCommitMeta().getAuthor()));

    log = commitLog(branch.getName(), null, "commit.author.matches('au.*-(2|4)')");
    soft.assertThat(log)
        .hasSize(commitsPerAuthor * 2)
        .allSatisfy(
            commit ->
                assertThat(ImmutableList.of("author-2", "author-4"))
                    .contains(commit.getCommitMeta().getAuthor()));
  }

  @Test
  @DisabledOnOs(OS.WINDOWS) // missing req'd time accuracy
  public void filterCommitLogByTimeRange() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByTimeRange");

    int numAuthors = 3;
    int commitsPerAuthor = 3;
    int expectedTotalSize = numAuthors * commitsPerAuthor;

    String currentHash = branch.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    List<LogEntry> log = commitLog(branch.getName());
    soft.assertThat(log).hasSize(expectedTotalSize);

    Instant initialCommitTime = log.get(log.size() - 1).getCommitMeta().getCommitTime();
    soft.assertThat(initialCommitTime).isNotNull();
    Instant lastCommitTime = log.get(0).getCommitMeta().getCommitTime();
    soft.assertThat(lastCommitTime).isNotNull();
    Instant fiveMinLater = requireNonNull(initialCommitTime).plus(5, ChronoUnit.MINUTES);

    log =
        commitLog(
            branch.getName(),
            null,
            String.format("timestamp(commit.commitTime) > timestamp('%s')", initialCommitTime));
    soft.assertThat(log)
        .hasSize(expectedTotalSize - 1)
        .allSatisfy(
            commit ->
                assertThat(commit.getCommitMeta().getCommitTime()).isAfter(initialCommitTime));

    log =
        commitLog(
            branch.getName(),
            null,
            String.format("timestamp(commit.commitTime) < timestamp('%s')", fiveMinLater));
    soft.assertThat(log)
        .hasSize(expectedTotalSize)
        .allSatisfy(
            commit -> assertThat(commit.getCommitMeta().getCommitTime()).isBefore(fiveMinLater));

    log =
        commitLog(
            branch.getName(),
            null,
            String.format(
                "timestamp(commit.commitTime) > timestamp('%s') && timestamp(commit.commitTime) < timestamp('%s')",
                initialCommitTime, lastCommitTime));
    soft.assertThat(log)
        .hasSize(expectedTotalSize - 2)
        .allSatisfy(
            commit ->
                assertThat(commit.getCommitMeta().getCommitTime())
                    .isAfter(initialCommitTime)
                    .isBefore(lastCommitTime));

    log =
        commitLog(
            branch.getName(),
            null,
            String.format("timestamp(commit.commitTime) > timestamp('%s')", fiveMinLater));
    soft.assertThat(log).isEmpty();
  }

  @Test
  public void filterCommitLogByProperties() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByProperties");

    int numAuthors = 3;
    int commitsPerAuthor = 3;

    String currentHash = branch.getHash();
    createCommits(branch, numAuthors, commitsPerAuthor, currentHash);
    List<LogEntry> log = commitLog(branch.getName());
    soft.assertThat(log).hasSize(numAuthors * commitsPerAuthor);

    log = commitLog(branch.getName(), null, "commit.properties['prop1'] == 'val1'");
    soft.assertThat(log)
        .hasSize(numAuthors * commitsPerAuthor)
        .allSatisfy(
            commit ->
                assertThat(commit.getCommitMeta().getProperties().get("prop1")).isEqualTo("val1"));

    log = commitLog(branch.getName(), null, "commit.properties['prop1'] == 'val3'");
    soft.assertThat(log).isEmpty();
  }

  @Test
  public void filterCommitLogByCommitRange() throws BaseNessieClientServerException {
    Branch branch = createBranch("filterCommitLogByCommitRange");

    int numCommits = 3;

    String currentHash = branch.getHash();
    createCommits(branch, 1, numCommits, currentHash);
    List<LogEntry> entireLog = commitLog(branch.getName());
    soft.assertThat(entireLog).hasSize(numCommits);

    // if startHash > endHash, then we return all commits starting from startHash
    String startHash = entireLog.get(numCommits / 2).getCommitMeta().getHash();
    String endHash = entireLog.get(0).getCommitMeta().getHash();
    List<LogEntry> log =
        treeApi()
            .getCommitLog(
                branch.getName(),
                MINIMAL,
                startHash,
                endHash,
                null,
                null,
                new UnlimitedListResponseHandler<>());
    soft.assertThat(log).hasSize(numCommits / 2 + 1);

    for (int i = 0, j = numCommits - 1; i < j; i++, j--) {
      startHash = entireLog.get(j).getCommitMeta().getHash();
      endHash = entireLog.get(i).getCommitMeta().getHash();
      log =
          treeApi()
              .getCommitLog(
                  branch.getName(),
                  MINIMAL,
                  startHash,
                  endHash,
                  null,
                  null,
                  new UnlimitedListResponseHandler<>());
      soft.assertThat(log).hasSize(numCommits - (i * 2));
      soft.assertThat(ImmutableList.copyOf(entireLog).subList(i, j + 1))
          .containsExactlyElementsOf(log);
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
    List<LogEntry> log = commitLog(branch.getName());
    soft.assertThat(log).hasSize(expectedTotalSize);

    String author = "author-1";
    List<String> messagesOfAuthorOne =
        log.stream()
            .map(LogEntry::getCommitMeta)
            .filter(c -> author.equals(c.getAuthor()))
            .map(CommitMeta::getMessage)
            .collect(Collectors.toList());
    soft.assertThat(
            pagedCommitLog(
                    branch.getName(),
                    MINIMAL,
                    String.format("commit.author=='%s'", author),
                    pageSizeHint,
                    commits)
                .stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getMessage))
        .containsAnyElementsOf(messagesOfAuthorOne);

    List<String> allMessages =
        log.stream()
            .map(LogEntry::getCommitMeta)
            .map(CommitMeta::getMessage)
            .collect(Collectors.toList());

    List<LogEntry> completeLog =
        pagedCommitLog(branch.getName(), MINIMAL, null, pageSizeHint, expectedTotalSize);

    soft.assertThat(completeLog.stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactlyElementsOf(allMessages);
  }

  @Test
  public void commitLogPaging() throws BaseNessieClientServerException {
    Branch branch = createBranch("commitLogPaging");

    int commits = 25;
    int pageSizeHint = 4;

    String currentHash = branch.getHash();
    List<String> allMessages = new ArrayList<>();
    ContentKey key = ContentKey.of("table");
    for (int i = 0; i < commits; i++) {
      String msg = "message-for-" + i;
      allMessages.add(msg);

      Put op;
      try {
        Content existing =
            contentApi()
                .getContent(key, branch.getName(), currentHash, false, API_READ)
                .getContent();
        op = Put.of(key, IcebergTable.of("some-file-" + i, 42, 42, 42, 42, existing.getId()));
      } catch (NessieNotFoundException notFound) {
        op = Put.of(key, IcebergTable.of("some-file-" + i, 42, 42, 42, 42));
      }

      String nextHash =
          commit(branch.getName(), currentHash, fromMessage(msg), op).getTargetBranch().getHash();
      soft.assertThat(nextHash).isNotEqualTo(currentHash);
      currentHash = nextHash;
    }
    soft.assertAll();
    Collections.reverse(allMessages);

    soft.assertThat(
            pagedCommitLog(branch.getName(), MINIMAL, null, pageSizeHint, commits).stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getMessage))
        .containsExactlyElementsOf(allMessages);

    soft.assertThat(
            commitLog(branch.getName()).stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getMessage))
        .containsExactlyElementsOf(allMessages);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void commitLogExtended() throws Exception {
    String branch = "commitLogExtended";
    Branch branchInitial = createBranch(branch, treeApi().getDefaultBranch());

    int numCommits = 10;

    Operation[] operations =
        IntStream.rangeClosed(1, numCommits)
            .boxed()
            .flatMap(
                i ->
                    Stream.of(
                        Put.of(
                            ContentKey.of("delete" + i),
                            IcebergTable.of("delete-" + i, i, i, i, i)),
                        Put.of(
                            ContentKey.of("unchanged" + i),
                            IcebergTable.of("unchanged-" + i, i, i, i, i))))
            .toArray(Operation[]::new);
    String initialHash =
        commit(branchInitial, fromMessage("Initial commit"), operations)
            .getTargetBranch()
            .getHash();

    List<String> hashes =
        IntStream.rangeClosed(1, numCommits)
            .mapToObj(
                i -> {
                  try {
                    Branch currentBranch = (Branch) getReference(branch);
                    return commit(
                            currentBranch,
                            fromMessage("Commit #" + i),
                            Put.of(ContentKey.of("k" + i), IcebergTable.of("m" + i, i, i, i, i)),
                            Put.of(
                                ContentKey.of("key" + i), IcebergTable.of("meta" + i, i, i, i, i)),
                            Delete.of(ContentKey.of("delete" + i)),
                            Unchanged.of(ContentKey.of("unchanged" + i)))
                        .getTargetBranch()
                        .getHash();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    List<String> parentHashes =
        Stream.concat(Stream.of(initialHash), hashes.subList(0, 9).stream())
            .collect(Collectors.toList());

    Reference branchRef = getReference(branch);

    soft.assertThat(reverse(commitLog(branchRef.getName(), MINIMAL, hashes.get(0), null, null)))
        .allSatisfy(
            c -> {
              assertThat(c.getOperations()).isNull();
              assertThat(c.getParentCommitHash()).isNotNull();
              assertThat(c.getAdditionalParents()).isEmpty();
              assertThat(c.getCommitMeta().getProperties()).isEmpty();
            })
        .extracting(e -> e.getCommitMeta().getHash())
        .containsExactlyElementsOf(hashes);

    List<LogEntry> commits =
        reverse(commitLog(branchRef.getName(), ALL, hashes.get(0), null, null));
    soft.assertThat(IntStream.rangeClosed(1, numCommits))
        .allSatisfy(
            i -> {
              LogEntry c = commits.get(i - 1);
              assertThat(c)
                  .extracting(
                      e -> e.getCommitMeta().getMessage(),
                      e -> e.getCommitMeta().getHash(),
                      LogEntry::getParentCommitHash,
                      e -> operationsWithoutContentId(e.getOperations()))
                  .containsExactly(
                      "Commit #" + i,
                      hashes.get(i - 1),
                      parentHashes.get(i - 1),
                      Arrays.asList(
                          Delete.of(ContentKey.of("delete" + i)),
                          Put.of(ContentKey.of("k" + i), IcebergTable.of("m" + i, i, i, i, i)),
                          Put.of(
                              ContentKey.of("key" + i), IcebergTable.of("meta" + i, i, i, i, i))));
            });
  }

  @Test
  public void commitLogExtendedForUnchangedOperation() throws Exception {
    String branch = "commitLogExtendedUnchanged";
    String head = createBranch(branch, treeApi().getDefaultBranch()).getHash();
    commit(branch, head, fromMessage("Commit #1"), Unchanged.of(ContentKey.of("key1")));

    List<LogEntry> logEntries = commitLog(branch, ALL, null);
    soft.assertThat(logEntries.size()).isEqualTo(1);
    soft.assertThat(logEntries.get(0).getCommitMeta().getMessage()).contains("Commit #1");
    soft.assertThat(logEntries.get(0).getOperations()).isNull();
  }

  @Test
  public void commitLogForNamelessReference() throws BaseNessieClientServerException {
    Branch branch = createBranch("commitLogForNamelessReference");
    String head = createCommits(branch, 1, 5, branch.getHash());
    List<LogEntry> log = commitLog(DetachedRef.REF_NAME, MINIMAL, null, head, null);
    // Verifying size is sufficient to make sure the right log was retrieved
    assertThat(log).hasSize(5);
  }
}
