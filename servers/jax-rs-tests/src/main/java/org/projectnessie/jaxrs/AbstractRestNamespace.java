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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.iterable.ThrowingExtractor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

  @ParameterizedTest
  @ValueSource(strings = {"a.b.c", "a.b\u001Dc.d", "a.b.c.d", "a.b\u0000c.d"})
  public void testNamespaces(String namespaceName) throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespaces");
    Namespace ns = Namespace.parse(namespaceName);
    Namespace namespace =
        getApi().createNamespace().refName(branch.getName()).namespace(ns).create();

    assertThat(namespace)
        .isNotNull()
        .extracting(Namespace::getElements, Namespace::toPathString)
        .containsExactly(ns.getElements(), ns.toPathString());

    Namespace got = getApi().getNamespace().refName(branch.getName()).namespace(ns).get();
    assertThat(got).isEqualTo(namespace);

    // the namespace in the error message will contain the representation with u001D
    String namespaceInErrorMsg = namespaceName.replace("\u0000", "\u001D");

    assertThatThrownBy(
            () -> getApi().createNamespace().refName(branch.getName()).namespace(ns).create())
        .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
        .hasMessage(String.format("Namespace '%s' already exists", namespaceInErrorMsg));

    getApi().deleteNamespace().refName(branch.getName()).namespace(ns).delete();
    assertThatThrownBy(
            () -> getApi().deleteNamespace().refName(branch.getName()).namespace(ns).delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage(String.format("Namespace '%s' does not exist", namespaceInErrorMsg));

    assertThatThrownBy(() -> getApi().getNamespace().refName(branch.getName()).namespace(ns).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage(String.format("Namespace '%s' does not exist", namespaceInErrorMsg));

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

    ThrowingExtractor<String, Namespace, ?> createNamespace =
        identifier ->
            getApi()
                .createNamespace()
                .refName(branch.getName())
                .namespace(Namespace.parse(identifier))
                .create();

    Namespace one = createNamespace.apply("a.b.c");
    Namespace two = createNamespace.apply("a.b.d");
    Namespace three = createNamespace.apply("x.y.z");
    Namespace four = createNamespace.apply("one.two");
    for (Namespace namespace : Arrays.asList(one, two, three, four)) {
      assertThat(namespace).isNotNull();
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
                .namespace("one")
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

  @Test
  public void testNamespaceConflictWithOtherContent() throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespaceConflictWithOtherContent");
    IcebergTable icebergTable = IcebergTable.of("icebergTable", 42, 42, 42, 42);

    List<String> elements = Arrays.asList("a", "b", "c");
    ContentKey key = ContentKey.of(elements);
    getApi()
        .commitMultipleOperations()
        .branchName(branch.getName())
        .hash(branch.getHash())
        .commitMeta(CommitMeta.fromMessage("add table"))
        .operation(Put.of(key, icebergTable))
        .commit();

    Namespace ns = Namespace.of(elements);
    assertThatThrownBy(
            () -> getApi().createNamespace().refName(branch.getName()).namespace(ns).create())
        .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
        .hasMessage("Another content object with name 'a.b.c' already exists");

    assertThatThrownBy(() -> getApi().getNamespace().refName(branch.getName()).namespace(ns).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'a.b.c' does not exist");

    assertThatThrownBy(
            () -> getApi().deleteNamespace().refName(branch.getName()).namespace(ns).delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'a.b.c' does not exist");

    // it should only contain the parent namespace of the "a.b.c" table
    assertThat(getApi().getMultipleNamespaces().refName(branch.getName()).get().getNamespaces())
        .containsExactly(Namespace.parse("a.b"));
  }

  @Test
  public void testNamespacesWithAndWithoutZeroBytes() throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespacesWithAndWithoutZeroBytes");
    String firstName = "a.b\u0000c.d";
    String secondName = "a.b.c.d";

    // perform creation and retrieval
    ThrowingExtractor<String, Namespace, ?> creator =
        identifier -> {
          Namespace namespace = Namespace.parse(identifier);

          Namespace created =
              getApi().createNamespace().refName(branch.getName()).namespace(namespace).create();
          assertThat(created)
              .isNotNull()
              .extracting(Namespace::getElements, Namespace::toPathString)
              .containsExactly(namespace.getElements(), namespace.toPathString());

          assertThat(getApi().getNamespace().refName(branch.getName()).namespace(namespace).get())
              .isEqualTo(created);

          assertThatThrownBy(
                  () ->
                      getApi()
                          .createNamespace()
                          .refName(branch.getName())
                          .namespace(namespace)
                          .create())
              .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
              .hasMessage(String.format("Namespace '%s' already exists", namespace.name()));

          return created;
        };

    Namespace first = creator.apply(firstName);
    Namespace second = creator.apply(secondName);
    List<Namespace> namespaces = Arrays.asList(first, second);

    // retrieval by prefix
    assertThat(getApi().getMultipleNamespaces().refName(branch.getName()).get().getNamespaces())
        .containsExactlyInAnyOrderElementsOf(namespaces);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .namespace("a")
                .refName(branch.getName())
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrderElementsOf(namespaces);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .namespace("a.b")
                .refName(branch.getName())
                .get()
                .getNamespaces())
        .containsExactly(second);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .namespace("a.b\u001Dc")
                .refName(branch.getName())
                .get()
                .getNamespaces())
        .containsExactly(first);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .namespace("a.b\u0000c")
                .refName(branch.getName())
                .get()
                .getNamespaces())
        .containsExactly(first);

    assertThat(
            getApi()
                .getMultipleNamespaces()
                .namespace("a.b.c")
                .refName(branch.getName())
                .get()
                .getNamespaces())
        .containsExactly(second);

    // deletion
    for (Namespace namespace : namespaces) {
      getApi().deleteNamespace().refName(branch.getName()).namespace(namespace).delete();

      assertThatThrownBy(
              () ->
                  getApi()
                      .deleteNamespace()
                      .refName(branch.getName())
                      .namespace(namespace)
                      .delete())
          .isInstanceOf(NessieNamespaceNotFoundException.class)
          .hasMessage(String.format("Namespace '%s' does not exist", namespace.name()));
    }

    assertThat(getApi().getMultipleNamespaces().refName(branch.getName()).get().getNamespaces())
        .isEmpty();
  }

  @Test
  public void testEmptyNamespace() throws BaseNessieClientServerException {
    Branch branch = createBranch("emptyNamespace");
    // can't create/fetch/delete an empty namespace due to empty REST path
    assertThatThrownBy(
            () ->
                getApi()
                    .createNamespace()
                    .refName(branch.getName())
                    .namespace(Namespace.EMPTY)
                    .create())
        .isInstanceOf(Exception.class);

    assertThatThrownBy(
            () ->
                getApi().getNamespace().refName(branch.getName()).namespace(Namespace.EMPTY).get())
        .isInstanceOf(Exception.class);

    assertThatThrownBy(
            () ->
                getApi()
                    .deleteNamespace()
                    .refName(branch.getName())
                    .namespace(Namespace.EMPTY)
                    .delete())
        .isInstanceOf(Exception.class);

    assertThat(getApi().getMultipleNamespaces().refName(branch.getName()).get().getNamespaces())
        .isEmpty();

    ContentKey keyWithoutNamespace = ContentKey.of("icebergTable");
    getApi()
        .commitMultipleOperations()
        .branchName(branch.getName())
        .hash(branch.getHash())
        .commitMeta(CommitMeta.fromMessage("add table"))
        .operation(Put.of(keyWithoutNamespace, IcebergTable.of("icebergTable", 42, 42, 42, 42)))
        .commit();

    assertThat(getApi().getMultipleNamespaces().refName(branch.getName()).get().getNamespaces())
        .isEmpty();
    assertThat(
            getApi()
                .getMultipleNamespaces()
                .refName(branch.getName())
                .namespace(Namespace.EMPTY)
                .get()
                .getNamespaces())
        .isEmpty();
  }

  @Test
  public void testNamespaceWithProperties() throws BaseNessieClientServerException {
    Branch branch = createBranch("namespaceWithProperties");
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    Namespace namespace = Namespace.of(properties, "a", "b", "c");

    Namespace ns =
        getApi()
            .createNamespace()
            .namespace(namespace)
            .properties(properties)
            .reference(branch)
            .create();
    assertThat(ns.getProperties()).isEqualTo(properties);

    assertThatThrownBy(
            () ->
                getApi()
                    .updateProperties()
                    .reference(branch)
                    .namespace("non-existing")
                    .updateProperties(properties)
                    .update())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'non-existing' does not exist");

    // Re-run with invalid name, but different parameters to ensure that missing parameters do not
    // fail the request before the name is validated.
    assertThatThrownBy(
            () ->
                getApi()
                    .updateProperties()
                    .reference(branch)
                    .namespace("non-existing")
                    .removeProperties(properties.keySet())
                    .update())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'non-existing' does not exist");

    getApi()
        .updateProperties()
        .reference(branch)
        .namespace(namespace)
        .updateProperties(properties)
        .update();

    ns = getApi().getNamespace().reference(branch).namespace(namespace).get();
    assertThat(ns.getProperties()).isEqualTo(properties);

    getApi()
        .updateProperties()
        .reference(branch)
        .namespace(namespace)
        .updateProperties(ImmutableMap.of("key3", "val3", "key1", "xyz"))
        .removeProperties(ImmutableSet.of("key2", "key5"))
        .update();
    ns = getApi().getNamespace().reference(branch).namespace(namespace).get();
    assertThat(ns.getProperties()).isEqualTo(ImmutableMap.of("key1", "xyz", "key3", "val3"));
  }
}
