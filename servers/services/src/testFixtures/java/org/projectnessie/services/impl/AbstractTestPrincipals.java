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

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.model.MergeBehavior.NORMAL;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Put;

public abstract class AbstractTestPrincipals extends BaseTestServiceImpl {

  private Branch makeCommits(Branch head) throws NessieConflictException, NessieNotFoundException {
    IcebergTable meta1 = IcebergTable.of("meep", 42, 42, 42, 42);
    IcebergTable meta2 = IcebergTable.of("meep_meep", 42, 42, 42, 42);

    head =
        commit(
                head,
                CommitMeta.builder().message("no security context").build(),
                Put.of(ContentKey.of("meep"), meta1))
            .getTargetBranch();
    assertThat(commitLog(head.getName()).stream().limit(1))
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(tuple("", "", "no security context"));

    setPrincipal(() -> "ThatNessieGuy");

    head =
        commit(
                head,
                CommitMeta.builder().message("with security").build(),
                Put.of(ContentKey.of("meep_meep"), meta2))
            .getTargetBranch();
    assertThat(commitLog(head.getName()).stream().limit(2).collect(Collectors.toList()))
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("ThatNessieGuy", "ThatNessieGuy", "with security"),
            tuple("", "", "no security context"));

    return head;
  }

  @Test
  public void committerAndAuthorMerge() throws Exception {
    Branch root = createBranch("root");

    root =
        commit(
                root,
                CommitMeta.fromMessage("root"),
                Put.of(ContentKey.of("other"), IcebergTable.of("/dev/null", 42, 42, 42, 42)))
            .getTargetBranch();

    Branch main = makeCommits(createBranch("committerAndAuthorMerge_main", root));
    Branch merge = createBranch("committerAndAuthorMerge_target", root);

    setPrincipal(() -> "NessieHerself");

    treeApi()
        .mergeRefIntoBranch(
            merge.getName(),
            merge.getHash(),
            main.getName(),
            main.getHash(),
            null,
            emptyList(),
            NORMAL,
            false,
            false,
            false);

    Branch merged = (Branch) getReference(merge.getName());

    assertThat(commitLog(merge.getName()).stream().limit(1).collect(Collectors.toList()))
        .first()
        .extracting(LogEntry::getCommitMeta)
        .extracting(
            CommitMeta::getCommitter,
            CommitMeta::getAuthor,
            CommitMeta::getMessage,
            CommitMeta::getHash)
        .containsExactly(
            "NessieHerself",
            "NessieHerself",
            format(
                "Merged %s at %s into %s at %s",
                main.getName(), main.getHash(), merge.getName(), merge.getHash()),
            merged.getHash());
  }

  @Test
  public void committerAndAuthorTransplant() throws Exception {
    Branch main = makeCommits(createBranch("committerAndAuthorTransplant_main"));
    Branch transplant = createBranch("committerAndAuthorTransplant_target");

    List<String> hashesToTransplant =
        commitLog(main.getName()).stream()
            .limit(2)
            .map(logEntry -> logEntry.getCommitMeta().getHash())
            .collect(Collectors.toList());
    Collections.reverse(hashesToTransplant); // transplant in the original commit order

    setPrincipal(() -> "NessieHerself");

    treeApi()
        .transplantCommitsIntoBranch(
            transplant.getName(),
            transplant.getHash(),
            null,
            hashesToTransplant,
            main.getName(),
            emptyList(),
            NORMAL,
            false,
            false,
            false);

    transplant = (Branch) getReference(transplant.getName());

    assertThat(commitLog(transplant.getName()).stream().limit(2).collect(Collectors.toList()))
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("NessieHerself", "ThatNessieGuy", "with security"),
            tuple("NessieHerself", "NessieHerself", "no security context"));
  }
}
