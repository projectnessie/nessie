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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
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
public abstract class AbstractRestNamespace extends AbstractRestRefLog {
  @Test
  public void testNamespaces() throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespaces");
    Namespace ns = Namespace.parse("a.b.c");
    Namespace namespace =
        getApi().createNamespace().refName(branch.getName()).namespace(ns).create();

    assertThat(namespace).isNotNull().isEqualTo(ns);
    assertThat(getApi().getNamespace().refName(branch.getName()).namespace(ns).get())
        .isEqualTo(namespace);

    assertThatThrownBy(
            () -> getApi().createNamespace().refName(branch.getName()).namespace(ns).create())
        .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
        .hasMessage("Namespace 'a.b.c' already exists");

    getApi().deleteNamespace().refName(branch.getName()).namespace(ns).delete();
    assertThatThrownBy(
            () -> getApi().deleteNamespace().refName(branch.getName()).namespace(ns).delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'a.b.c' does not exist");

    assertThatThrownBy(() -> getApi().getNamespace().refName(branch.getName()).namespace(ns).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'a.b.c' does not exist");

    assertThatThrownBy(
            () ->
                getApi()
                    .deleteNamespace()
                    .refName(branch.getName())
                    .namespace(Namespace.parse("nonexisting"))
                    .delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'nonexisting' does not exist");
  }

  @Test
  public void testNamespacesRetrieval() throws BaseNessieClientServerException {
    Branch branch = createBranch("namespace");
    Namespace one = Namespace.parse("a.b.c");
    Namespace two = Namespace.parse("a.b.d");
    Namespace three = Namespace.parse("x.y.z");
    Namespace four = Namespace.parse("one.two");
    for (Namespace namespace : Arrays.asList(one, two, three, four)) {
      assertThat(getApi().createNamespace().refName(branch.getName()).namespace(namespace).create())
          .isNotNull();
    }

    assertThat(getApi().getMultipleNamespaces().refName(branch.getName()).get().getNamespaces())
        .containsExactlyInAnyOrder(one, two, three, four);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace(Namespace.EMPTY)
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(one, two, three, four);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace("a")
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(one, two);
    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace("a.b")
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(one, two);
    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace("a.b.c")
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(one);
    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace("a.b.d")
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(two);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace("x")
                .get()
                .getNamespaces())
        .containsExactly(three);
    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace("z")
                .get()
                .getNamespaces())
        .isEmpty();
    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace("on")
                .get()
                .getNamespaces())
        .containsExactly(four);
  }

  @Test
  public void testNamespaceDeletion() throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespaceDeletion");

    CommitMultipleOperationsBuilder commit =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("verifyAllContentAndOperationTypes"));
    contentAndOperationTypes()
        .flatMap(
            c ->
                c.globalOperation == null
                    ? Stream.of(c.operation)
                    : Stream.of(c.operation, c.globalOperation))
        .forEach(commit::operation);
    commit.commit();

    List<Entry> entries =
        contentAndOperationTypes()
            .filter(c -> c.operation instanceof Put)
            .map(c -> Entry.builder().type(c.type).name(c.operation.getKey()).build())
            .collect(Collectors.toList());

    for (Entry e : entries) {
      Namespace namespace = e.getName().getNamespace();
      assertThat(getApi().getNamespace().refName(branch.getName()).namespace(namespace).get())
          .isEqualTo(namespace);

      assertThatThrownBy(
              () ->
                  getApi()
                      .deleteNamespace()
                      .refName(branch.getName())
                      .namespace(namespace)
                      .delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class)
          .hasMessage(String.format("Namespace '%s' is not empty", namespace));
    }
  }

  @Test
  public void testNamespaceMerge() throws BaseNessieClientServerException {
    Branch base = createBranch("merge-base");
    Branch branch = createBranch("merge-branch");
    Namespace ns = Namespace.parse("a.b.c");
    // create the same namespace on both branches
    getApi().createNamespace().namespace(ns).refName(branch.getName()).create();
    getApi().createNamespace().namespace(ns).refName(base.getName()).create();

    base = (Branch) getApi().getReference().refName(base.getName()).get();
    branch = (Branch) getApi().getReference().refName(branch.getName()).get();
    getApi().mergeRefIntoBranch().branch(base).fromRef(branch).merge();

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    String expectedCommitMsg = "create namespace a.b.c";
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly(expectedCommitMsg, expectedCommitMsg);

    assertThat(
            getApi().getEntries().refName(base.getName()).get().getEntries().stream()
                .map(Entry::getName))
        .containsExactly(ContentKey.of(ns.getElements()));

    assertThat(getApi().getNamespace().refName(base.getName()).namespace(ns).get()).isNotNull();
  }

  @Test
  public void testNamespaceMergeWithConflict() throws BaseNessieClientServerException {
    Branch base = createBranch("merge-base");
    Branch branch = createBranch("merge-branch");
    Namespace ns = Namespace.parse("a.b.c");
    // create a namespace on the base branch
    getApi().createNamespace().namespace(ns).refName(base.getName()).create();
    base = (Branch) getApi().getReference().refName(base.getName()).get();

    // create a table with the same name on the other branch
    IcebergTable table = IcebergTable.of("merge-table1", 42, 42, 42, 42);
    branch =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(branch.getHash())
            .commitMeta(CommitMeta.fromMessage("test-merge-branch1"))
            .operation(Put.of(ContentKey.of("a", "b", "c"), table))
            .commit();
    Branch finalBase = base;
    Branch finalBranch = branch;
    assertThatThrownBy(
            () -> getApi().mergeRefIntoBranch().branch(finalBase).fromRef(finalBranch).merge())
        .isInstanceOf(NessieReferenceConflictException.class)
        .hasMessage("The following keys have been changed in conflict: 'a.b.c'");

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    // merging should not have been possible ("test-merge-branch1" shouldn't be in the commits)
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly("create namespace a.b.c");

    List<Entry> entries = getApi().getEntries().refName(base.getName()).get().getEntries();
    assertThat(entries.stream().map(Entry::getName))
        .containsExactly(ContentKey.of(ns.getElements()));

    assertThat(getApi().getNamespace().refName(base.getName()).namespace(ns).get()).isNotNull();
  }
}
