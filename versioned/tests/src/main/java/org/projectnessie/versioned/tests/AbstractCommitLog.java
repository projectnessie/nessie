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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractCommitLog extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractCommitLog(VersionStore store) {
    super(store);
  }

  @Test
  public void commitLogPaging() throws Exception {
    BranchName branch = BranchName.of("commitLogPaging");
    Hash createHash = store().create(branch, Optional.empty()).getHash();

    int commits = 95; // this should be enough
    Hash[] commitHashes = new Hash[commits];
    List<CommitMeta> messages = new ArrayList<>(commits);
    for (int i = 0; i < commits; i++) {
      String str = String.format("commit#%05d", i);
      ImmutableCommitMeta.Builder msg = CommitMeta.builder().message(str);

      ContentKey key = ContentKey.of("table");
      Hash parent = i == 0 ? createHash : commitHashes[i - 1];
      ContentResult value =
          store()
              .getValue(
                  store().hashOnReference(branch, Optional.of(parent), emptyList()), key, false);
      Put op =
          value != null
              ? Put.of(key, onRef(str, requireNonNull(value.content()).getId()))
              : Put.of(key, newOnRef(str));

      commitHashes[i] =
          store()
              .commit(branch, Optional.of(parent), msg.build(), ImmutableList.of(op))
              .getCommitHash();

      messages.add(
          msg.hash(commitHashes[i].asString()).addParentCommitHashes(parent.asString()).build());
    }
    Collections.reverse(messages);

    List<CommitMeta> justTwo = commitsListMap(branch, 2, Commit::getCommitMeta);
    soft.assertThat(justTwo).isEqualTo(messages.subList(0, 2));
    List<CommitMeta> justTen = commitsListMap(branch, 10, Commit::getCommitMeta);
    soft.assertThat(justTen).isEqualTo(messages.subList(0, 10));

    int pageSize = 10;

    // Test parameter sanity check. Want the last page to be smaller than the page-size.
    soft.assertThat(commits % (pageSize - 1)).isNotEqualTo(0);

    Hash lastHash = null;
    for (int offset = 0; ; ) {
      List<Commit> logPage =
          commitsListMap(lastHash == null ? branch : lastHash, pageSize, Function.identity());

      soft.assertThat(logPage.stream().map(Commit::getCommitMeta).collect(Collectors.toList()))
          .isEqualTo(messages.subList(offset, Math.min(offset + pageSize, commits)));

      lastHash = logPage.get(logPage.size() - 1).getHash();

      offset += pageSize - 1;
      if (offset >= commits) {
        // The "next after last page" should always return just a single commit, that's basically
        // the "end of commit-log"-condition.
        logPage = commitsListMap(lastHash, pageSize, Function.identity());
        soft.assertThat(logPage.stream().map(Commit::getCommitMeta).collect(Collectors.toList()))
            .isEqualTo(Collections.singletonList(messages.get(commits - 1)));
        break;
      }
    }
  }

  @Test
  public void commitLogExtended() throws Exception {
    BranchName branch = BranchName.of("commitLogExtended");
    Hash firstParent = store().create(branch, Optional.empty()).getHash();

    int numCommits = 10;

    CommitBuilder init = commit("initial");
    List<Hash> hashes = new ArrayList<>();

    for (int i = 1; i <= numCommits; i++) {
      init = init.put("delete" + i, newOnRef("to delete " + i));
    }
    hashes.add(init.toBranch(branch));

    for (int i = 1; i <= numCommits; i++) {
      Hash head =
          commit("Commit #" + i)
              .put("k" + i, newOnRef("v" + i))
              .put("key" + i, newOnRef("value" + i))
              .delete("delete" + i)
              .toBranch(branch);
      hashes.add(head);
    }

    List<Hash> parentHashes =
        Stream.concat(Stream.of(firstParent), hashes.stream()).collect(Collectors.toList());

    soft.assertThat(Lists.reverse(commitsList(branch, false)))
        .allSatisfy(
            c -> {
              assertThat(c.getOperations()).isNull();
              assertThat(c.getParentHash()).isNotNull();
            })
        .extracting(Commit::getHash)
        .containsExactlyElementsOf(hashes);

    List<Commit> commits = Lists.reverse(commitsList(branch, true));
    for (int i = 1; i <= numCommits; i++) {
      Commit c = commits.get(i);
      soft.assertThat(c.getCommitMeta())
          .describedAs("Commit number %s", i)
          .satisfiesAnyOf(
              cm -> assertThat(cm.getHash()).isNull(),
              cm -> assertThat(cm.getHash()).isEqualTo(c.getHash().asString()));
      soft.assertThat(c)
          .describedAs("Commit number %s", i)
          .extracting(
              Commit::getHash,
              Commit::getParentHash,
              commit -> operationsWithoutContentId(commit.getOperations()))
          .containsExactly(
              hashes.get(i),
              parentHashes.get(i),
              Arrays.asList(
                  Delete.of(ContentKey.of("delete" + i)),
                  Put.of(ContentKey.of("k" + i), newOnRef("v" + i)),
                  Put.of(ContentKey.of("key" + i), newOnRef("value" + i))));
    }
  }
}
