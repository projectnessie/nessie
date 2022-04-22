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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestMergeTransplant extends AbstractRestInvalidWithHttp {

  @ParameterizedTest
  @CsvSource(
      value = {"true,true", "true,false", "false,true", "false,false"}) // merge requires the hash
  public void transplant(boolean withDetachedCommit, boolean keepIndividualCommits)
      throws BaseNessieClientServerException {
    mergeTransplant(
        keepIndividualCommits,
        t -> {
          List<Object> objects = t.toList();
          Branch base = (Branch) objects.get(0);
          Branch branch = (Branch) objects.get(1);
          Reference committed1 = (Reference) objects.get(2);
          Reference committed2 = (Reference) objects.get(3);
          getApi()
              .transplantCommitsIntoBranch()
              .hashesToTransplant(ImmutableList.of(committed1.getHash(), committed2.getHash()))
              .fromRefName(maybeAsDetachedName(withDetachedCommit, branch))
              .branch(base)
              .keepIndividualCommits(keepIndividualCommits)
              .transplant();
        });
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "UNCHANGED,true",
        "UNCHANGED,false",
        "DETACHED,true",
        "DETACHED,false"
      }) // merge requires the hash
  public void merge(ReferenceMode refMode, boolean keepIndividualCommits)
      throws BaseNessieClientServerException {
    mergeTransplant(
        keepIndividualCommits,
        t -> {
          List<Object> objects = t.toList();
          Branch base = (Branch) objects.get(0);
          Reference committed2 = (Reference) objects.get(3);
          getApi()
              .mergeRefIntoBranch()
              .branch(base)
              .fromRef(refMode.transform(committed2))
              .keepIndividualCommits(keepIndividualCommits)
              .merge();
        });
  }

  private void mergeTransplant(boolean keepIndividualCommits, ThrowingConsumer<Tuple> actor)
      throws BaseNessieClientServerException {
    Branch base = createBranch("base");
    Branch branch = createBranch("branch");

    ContentKey key1 = ContentKey.of("key1");
    IcebergTable table1 = IcebergTable.of("table1", 42, 42, 42, 42);
    ContentKey key2 = ContentKey.of("key2");
    IcebergTable table2 = IcebergTable.of("table2", 43, 43, 43, 43);

    Branch committed1 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(branch.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch1"))
            .operation(Put.of(key1, table1))
            .commit();
    assertThat(committed1.getHash()).isNotNull();

    table1 =
        getApi()
            .getContent()
            .reference(committed1)
            .key(key1)
            .get()
            .get(key1)
            .unwrap(IcebergTable.class)
            .get();

    Branch committed2 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(committed1.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch2"))
            .operation(Put.of(key1, table1, table1))
            .commit();
    assertThat(committed2.getHash()).isNotNull();

    int commitCount = 2;

    LogResponse logBranch =
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .untilHash(branch.getHash())
            .maxRecords(commitCount)
            .get();

    getApi()
        .commitMultipleOperations()
        .branchName(base.getName())
        .hash(base.getHash())
        .commitMeta(CommitMeta.fromMessage("test-main"))
        .operation(Put.of(key2, table2))
        .commit();

    actor.accept(Tuple.tuple(base, branch, committed1, committed2));

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    if (keepIndividualCommits) {
      assertThat(
              log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .hasSize(3)
          .containsExactly("test-branch2", "test-branch1", "test-main");
    } else {
      assertThat(
              log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .hasSize(2)
          .first(InstanceOfAssertFactories.STRING)
          .contains("test-branch2")
          .contains("test-branch1");
    }

    // Verify that the commit-timestamp was updated
    LogResponse logOfMerged =
        getApi().getCommitLog().refName(base.getName()).maxRecords(commitCount).get();
    assertThat(
            logOfMerged.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime))
        .isNotEqualTo(
            logBranch.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime));

    assertThat(
            getApi().getEntries().refName(base.getName()).get().getEntries().stream()
                .map(e -> e.getName().getName()))
        .containsExactlyInAnyOrder("key1", "key2");
  }

  @ParameterizedTest
  @EnumSource(
      value = ReferenceMode.class,
      mode = Mode.EXCLUDE,
      names = "NAME_ONLY") // merge requires the hash
  public void mergeWithNamespaces(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch base = createBranch("merge-base");
    Branch branch = createBranch("merge-branch");
    Namespace ns = Namespace.parse("a.b.c");
    // create the same namespace on both branches
    getApi().createNamespace().namespace(ns).refName(branch.getName()).create();
    getApi().createNamespace().namespace(ns).refName(base.getName()).create();

    IcebergTable table1 = IcebergTable.of("table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("table2", 43, 43, 43, 43);

    ContentKey key1 = ContentKey.of(ns, "key1");
    ContentKey key2 = ContentKey.of(ns, "key2");
    Branch committed1 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(branch.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch1"))
            .operation(Put.of(key1, table1))
            .commit();
    assertThat(committed1.getHash()).isNotNull();

    table1 =
        getApi()
            .getContent()
            .reference(committed1)
            .key(key1)
            .get()
            .get(key1)
            .unwrap(IcebergTable.class)
            .get();

    Branch committed2 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(committed1.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch2"))
            .operation(Put.of(key1, table1, table1))
            .commit();
    assertThat(committed2.getHash()).isNotNull();

    getApi()
        .commitMultipleOperations()
        .branchName(base.getName())
        .hash(base.getHash())
        .commitMeta(CommitMeta.fromMessage("test-main"))
        .operation(Put.of(key2, table2))
        .commit();

    getApi()
        .mergeRefIntoBranch()
        .branch(base)
        .fromRef(refMode.transform(committed2))
        .keepIndividualCommits(true)
        .merge();

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly(
            "test-branch2",
            "test-branch1",
            "create namespace a.b.c",
            "test-main",
            "create namespace a.b.c");

    assertThat(
            getApi().getEntries().refName(base.getName()).get().getEntries().stream()
                .map(Entry::getName))
        .containsExactlyInAnyOrder(key1, key2, ContentKey.of(ns.getElements()));

    assertThat(getApi().getNamespace().refName(base.getName()).namespace(ns).get()).isNotNull();
  }
}
