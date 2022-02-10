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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestEntries extends AbstractRestDiff {

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByType(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch branch = createBranch("filterTypes");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable tam = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergView tb = IcebergView.of("pathx", 1, 1, "select * from table", "Dremio");
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(a, tam))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commit();
    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(b, tb))
            .commitMeta(CommitMeta.fromMessage("commit 2"))
            .commit();
    List<Entry> entries =
        getApi().getEntries().reference(refMode.transform(branch)).get().getEntries();
    List<Entry> expected =
        asList(
            Entry.builder().name(a).type(Type.ICEBERG_TABLE).build(),
            Entry.builder().name(b).type(Type.ICEBERG_VIEW).build());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.contentType=='ICEBERG_TABLE'")
            .get()
            .getEntries();
    assertEquals(singletonList(expected.get(0)), entries);

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.contentType=='ICEBERG_VIEW'")
            .get()
            .getEntries();
    assertEquals(singletonList(expected.get(1)), entries);

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.contentType in ['ICEBERG_TABLE', 'ICEBERG_VIEW']")
            .get()
            .getEntries();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByNamespace(ReferenceMode refMode)
      throws BaseNessieClientServerException {
    Branch branch = createBranch("filterEntriesByNamespace");
    ContentKey first = ContentKey.of("a", "b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b", "c", "secondTable");
    ContentKey third = ContentKey.of("a", "thirdTable");
    ContentKey fourth = ContentKey.of("a", "fourthTable");
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commit();
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .commit();
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(third, IcebergTable.of("path3", 42, 42, 42, 42)))
        .commitMeta(CommitMeta.fromMessage("commit 3"))
        .commit();
    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(fourth, IcebergTable.of("path4", 42, 42, 42, 42)))
            .commitMeta(CommitMeta.fromMessage("commit 4"))
            .commit();

    List<Entry> entries =
        getApi().getEntries().reference(refMode.transform(branch)).get().getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries = getApi().getEntries().reference(refMode.transform(branch)).get().getEntries();
    assertThat(entries).isNotNull().hasSize(4);

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a.b')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(2);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a.b"));

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(4);
    entries.forEach(e -> assertThat(e.getName().getNamespace().name()).startsWith("a"));

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a.b.c.firstTable')")
            .get()
            .getEntries();
    assertThat(entries).isEmpty();

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a.fourthTable')")
            .get()
            .getEntries();
    assertThat(entries).isEmpty();

    getApi()
        .deleteBranch()
        .branchName(branch.getName())
        .hash(getApi().getReference().refName(branch.getName()).get().getHash())
        .delete();
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByNamespaceAndPrefixDepth(ReferenceMode refMode)
      throws BaseNessieClientServerException {
    Branch branch = createBranch("filterEntriesByNamespaceAndPrefixDepth");
    ContentKey first = ContentKey.of("a", "b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b", "c", "secondTable");
    ContentKey third = ContentKey.of("a", "thirdTable");
    ContentKey fourth = ContentKey.of("a", "b", "fourthTable");
    ContentKey fifth = ContentKey.of("a", "boo", "fifthTable");
    ContentKey withoutNamespace = ContentKey.of("withoutNamespace");
    List<ContentKey> keys = ImmutableList.of(first, second, third, fourth, fifth, withoutNamespace);
    for (int i = 0; i < keys.size(); i++) {
      getApi()
          .commitMultipleOperations()
          .branch(branch)
          .operation(Put.of(keys.get(i), IcebergTable.of("path" + i, 42, 42, 42, 42)))
          .commitMeta(CommitMeta.fromMessage("commit " + i))
          .commit();
    }
    branch = (Branch) getApi().getReference().refName(branch.getName()).get();

    Reference reference = refMode.transform(branch);
    List<Entry> entries =
        getApi().getEntries().reference(reference).namespaceDepth(0).get().getEntries();
    assertThat(entries).isNotNull().hasSize(6);

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(0)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).isNotNull().hasSize(5);

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(1)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0))
        .matches(e -> e.getType().equals(Type.NAMESPACE))
        .matches(e -> e.getName().equals(ContentKey.of("a")));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(2)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(3);
    assertThat(entries.get(2))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "thirdTable")));
    assertThat(entries.get(1))
        .matches(e -> e.getType().equals(Type.NAMESPACE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b")));
    assertThat(entries.get(0))
        .matches(e -> e.getType().equals(Type.NAMESPACE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "boo")));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(3)
            .filter("entry.namespace.matches('a\\\\.b(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(2);
    assertThat(entries.get(1))
        .matches(e -> e.getType().equals(Type.NAMESPACE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "c")));
    assertThat(entries.get(0))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "fourthTable")));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(4)
            .filter("entry.namespace.matches('a\\\\.b\\\\.c(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(2);
    assertThat(entries.get(1))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "c", "firstTable")));
    assertThat(entries.get(0))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "c", "secondTable")));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(5)
            .filter("entry.namespace.matches('(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).isEmpty();

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(3)
            .filter("entry.namespace.matches('(\\\\.|$)')")
            .get()
            .getEntries();
    assertThat(entries).hasSize(3);
    assertThat(entries.get(2))
        .matches(e -> e.getType().equals(Type.NAMESPACE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "c")));
    assertThat(entries.get(1))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "b", "fourthTable")));
    assertThat(entries.get(0))
        .matches(e -> e.getType().equals(Type.ICEBERG_TABLE))
        .matches(e -> e.getName().equals(ContentKey.of("a", "boo", "fifthTable")));

    assumeTrue(ReferenceMode.DETACHED != refMode);
    // check that implicit namespaces are properly detected
    checkNamespaces(
        reference,
        Arrays.asList("a", "a.b", "a.boo", "a.b.c"),
        Arrays.asList(first, second, third, fourth, fifth));
  }

  private void checkNamespaces(
      Reference reference, List<String> knownNamespaces, List<ContentKey> knownContentKeys)
      throws NessieReferenceNotFoundException, NessieNamespaceNotFoundException {

    assertThat(getApi().getNamespaces().reference(reference).namespace("a").get().getNamespaces())
        .hasSize(4);
    for (String namespace : knownNamespaces) {
      Namespace ns = Namespace.parse(namespace);
      assertThat(getApi().getNamespace().reference(reference).namespace(ns).get()).isNotNull();

      assertThatThrownBy(
              () -> getApi().createNamespace().reference(reference).namespace(ns).create())
          .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
          .hasMessage(String.format("Namespace '%s' already exists", namespace));

      assertThatThrownBy(
              () -> getApi().deleteNamespace().reference(reference).namespace(ns).delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class)
          .hasMessage(String.format("Namespace '%s' is not empty", namespace));
    }

    // unknown in the sense that these are actual tables and not namespaces
    List<String> unknownNamespaces =
        knownContentKeys.stream().map(ContentKey::toString).collect(Collectors.toList());
    for (String namespace : unknownNamespaces) {
      assertThatThrownBy(
              () ->
                  getApi()
                      .getNamespace()
                      .reference(reference)
                      .namespace(Namespace.parse(namespace))
                      .get())
          .isInstanceOf(NessieNamespaceNotFoundException.class)
          .hasMessage(String.format("Namespace '%s' does not exist", namespace));
    }
  }
}
