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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.ValueSource;
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

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestMergeTransplant extends AbstractRestInvalidWithHttp {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void transplant(boolean withDetachedCommit) throws BaseNessieClientServerException {
    Branch base = createBranch("transplant-base");
    Branch branch = createBranch("transplant-branch");

    IcebergTable table1 = IcebergTable.of("transplant-table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("transplant-table2", 43, 43, 43, 43);

    Branch committed1 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(branch.getHash())
            .commitMeta(CommitMeta.fromMessage("test-transplant-branch1"))
            .operation(Put.of(ContentKey.of("key1"), table1))
            .commit();
    assertThat(committed1.getHash()).isNotNull();

    Branch committed2 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(committed1.getHash())
            .commitMeta(CommitMeta.fromMessage("test-transplant-branch2"))
            .operation(Put.of(ContentKey.of("key1"), table1, table1))
            .commit();
    assertThat(committed2.getHash()).isNotNull();

    int commitsToTransplant = 2;

    LogResponse logBranch =
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .untilHash(branch.getHash())
            .maxRecords(commitsToTransplant)
            .get();

    getApi()
        .commitMultipleOperations()
        .branchName(base.getName())
        .hash(base.getHash())
        .commitMeta(CommitMeta.fromMessage("test-transplant-main"))
        .operation(Put.of(ContentKey.of("key2"), table2))
        .commit();

    getApi()
        .transplantCommitsIntoBranch()
        .hashesToTransplant(ImmutableList.of(committed1.getHash(), committed2.getHash()))
        .fromRefName(maybeAsDetachedName(withDetachedCommit, branch))
        .branch(base)
        .transplant();

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly(
            "test-transplant-branch2", "test-transplant-branch1", "test-transplant-main");

    // Verify that the commit-timestamp was updated
    LogResponse logOfTransplanted =
        getApi().getCommitLog().refName(base.getName()).maxRecords(commitsToTransplant).get();
    assertThat(
            logOfTransplanted.getLogEntries().stream()
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
  public void merge(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch base = createBranch("merge-base");
    Branch branch = createBranch("merge-branch");

    IcebergTable table1 = IcebergTable.of("merge-table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("merge-table2", 43, 43, 43, 43);

    Branch committed1 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(branch.getHash())
            .commitMeta(CommitMeta.fromMessage("test-merge-branch1"))
            .operation(Put.of(ContentKey.of("key1"), table1))
            .commit();
    assertThat(committed1.getHash()).isNotNull();

    Branch committed2 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(committed1.getHash())
            .commitMeta(CommitMeta.fromMessage("test-merge-branch2"))
            .operation(Put.of(ContentKey.of("key1"), table1, table1))
            .commit();
    assertThat(committed2.getHash()).isNotNull();

    int commitsToMerge = 2;

    LogResponse logBranch =
        getApi()
            .getCommitLog()
            .refName(branch.getName())
            .untilHash(branch.getHash())
            .maxRecords(commitsToMerge)
            .get();

    getApi()
        .commitMultipleOperations()
        .branchName(base.getName())
        .hash(base.getHash())
        .commitMeta(CommitMeta.fromMessage("test-merge-main"))
        .operation(Put.of(ContentKey.of("key2"), table2))
        .commit();

    getApi().mergeRefIntoBranch().branch(base).fromRef(refMode.transform(committed2)).merge();

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly("test-merge-branch2", "test-merge-branch1", "test-merge-main");

    // Verify that the commit-timestamp was updated
    LogResponse logOfMerged =
        getApi().getCommitLog().refName(base.getName()).maxRecords(commitsToMerge).get();
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

    IcebergTable table1 = IcebergTable.of("merge-table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("merge-table2", 43, 43, 43, 43);

    ContentKey key1 = ContentKey.of(ns, "key1");
    ContentKey key2 = ContentKey.of(ns, "key2");
    Branch committed1 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(branch.getHash())
            .commitMeta(CommitMeta.fromMessage("test-merge-branch1"))
            .operation(Put.of(key1, table1))
            .commit();
    assertThat(committed1.getHash()).isNotNull();

    Branch committed2 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(committed1.getHash())
            .commitMeta(CommitMeta.fromMessage("test-merge-branch2"))
            .operation(Put.of(key1, table1, table1))
            .commit();
    assertThat(committed2.getHash()).isNotNull();

    getApi()
        .commitMultipleOperations()
        .branchName(base.getName())
        .hash(base.getHash())
        .commitMeta(CommitMeta.fromMessage("test-merge-main"))
        .operation(Put.of(key2, table2))
        .commit();

    getApi().mergeRefIntoBranch().branch(base).fromRef(refMode.transform(committed2)).merge();

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly(
            "test-merge-branch2",
            "test-merge-branch1",
            "create namespace a.b.c",
            "test-merge-main",
            "create namespace a.b.c");

    assertThat(
            getApi().getEntries().refName(base.getName()).get().getEntries().stream()
                .map(Entry::getName))
        .containsExactlyInAnyOrder(key1, key2, ContentKey.of(ns.getElements()));

    assertThat(getApi().getNamespace().refName(base.getName()).namespace(ns).get()).isNotNull();
  }
}
