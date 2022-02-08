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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.projectnessie.versioned.testworker.CommitMessage.commitMessage;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;

public abstract class AbstractCommitLog extends AbstractNestedVersionStore {
  protected AbstractCommitLog(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  @Test
  public void commitLogPaging() throws Exception {
    BranchName branch = BranchName.of("commitLogPaging");
    Hash createHash = store().create(branch, Optional.empty());

    int commits = 95; // this should be enough
    Hash[] commitHashes = new Hash[commits];
    List<CommitMessage> messages = new ArrayList<>(commits);
    for (int i = 0; i < commits; i++) {
      CommitMessage msg = commitMessage(String.format("commit#%05d", i));
      messages.add(msg);
      commitHashes[i] =
          store()
              .commit(
                  branch,
                  Optional.of(i == 0 ? createHash : commitHashes[i - 1]),
                  msg,
                  ImmutableList.of(
                      Put.of(Key.of("table"), newOnRef(String.format("value#%05d", i)))));
    }
    Collections.reverse(messages);

    List<CommitMessage> justTwo =
        commitsList(branch, s -> s.limit(2).map(Commit::getCommitMeta), false);
    assertEquals(messages.subList(0, 2), justTwo);
    List<CommitMessage> justTen =
        commitsList(branch, s -> s.limit(10).map(Commit::getCommitMeta), false);
    assertEquals(messages.subList(0, 10), justTen);

    int pageSize = 10;

    // Test parameter sanity check. Want the last page to be smaller than the page-size.
    assertNotEquals(0, commits % (pageSize - 1));

    Hash lastHash = null;
    for (int offset = 0; ; ) {
      List<Commit<CommitMessage, BaseContent>> logPage =
          commitsList(lastHash == null ? branch : lastHash, s -> s.limit(pageSize), false);

      assertEquals(
          messages.subList(offset, Math.min(offset + pageSize, commits)),
          logPage.stream().map(Commit::getCommitMeta).collect(Collectors.toList()));

      lastHash = logPage.get(logPage.size() - 1).getHash();

      offset += pageSize - 1;
      if (offset >= commits) {
        // The "next after last page" should always return just a single commit, that's basically
        // the "end of commit-log"-condition.
        logPage = commitsList(lastHash, s -> s.limit(pageSize), false);
        assertEquals(
            Collections.singletonList(messages.get(commits - 1)),
            logPage.stream().map(Commit::getCommitMeta).collect(Collectors.toList()));
        break;
      }
    }
  }

  @Test
  public void commitLogExtendedNoGlobalState() throws Exception {
    BranchName branch = BranchName.of("commitLogExtended");
    Hash firstParent = store().create(branch, Optional.empty());

    int numCommits = 10;

    List<Hash> hashes =
        IntStream.rangeClosed(1, numCommits)
            .mapToObj(
                i -> {
                  try {
                    return commit("Commit #" + i)
                        .put("k" + i, onRef("v" + i, "c" + i))
                        .put("key" + i, onRef("value" + i, "cid" + i))
                        .delete("delete" + i)
                        .toBranch(branch);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    List<Hash> parentHashes =
        Stream.concat(Stream.of(firstParent), hashes.subList(0, 9).stream())
            .collect(Collectors.toList());

    assertThat(Lists.reverse(commitsList(branch, false)))
        .allSatisfy(
            c -> {
              assertThat(c.getOperations()).isNull();
              assertThat(c.getParentHash()).isNull();
            })
        .extracting(Commit::getHash)
        .containsExactlyElementsOf(hashes);

    List<Commit<CommitMessage, BaseContent>> commits = Lists.reverse(commitsList(branch, true));
    assertThat(IntStream.rangeClosed(1, numCommits))
        .allSatisfy(
            i -> {
              Commit<CommitMessage, BaseContent> c = commits.get(i - 1);
              assertThat(c)
                  .extracting(
                      Commit::getCommitMeta,
                      Commit::getHash,
                      Commit::getParentHash,
                      Commit::getOperations)
                  .containsExactly(
                      commitMessage("Commit #" + i),
                      hashes.get(i - 1),
                      parentHashes.get(i - 1),
                      Arrays.asList(
                          Delete.of(Key.of("delete" + i)),
                          Put.of(Key.of("k" + i), onRef("v" + i, "c" + i)),
                          Put.of(Key.of("key" + i), onRef("value" + i, "cid" + i))));
            });
  }
}
