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
package org.projectnessie.jaxrs.tests;

import static com.google.common.collect.Maps.immutableEntry;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assumptions.abort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestEntries extends AbstractRestDiff {

  @NessieApiVersions(versions = NessieApiVersion.V2)
  @ParameterizedTest
  @ValueSource(ints = {0, 20, 22})
  public void entriesPaging(int numKeys) throws BaseNessieClientServerException {
    Branch branch = createBranch("entriesPaging");
    try {
      getApi().getEntries().pageToken("Zm9v").reference(branch).get();
    } catch (NessieBadRequestException e) {
      if (!e.getMessage().contains("Paging not supported")) {
        throw e;
      }
      abort("DatabaseAdapter implementations / PersistVersionStore do not support paging");
    }

    IntFunction<ContentKey> contentKey = i -> ContentKey.of("key", Integer.toString(i));
    IntFunction<IcebergTable> table = i -> IcebergTable.of("meta" + i, 1, 2, 3, 4);
    int pageSize = 5;

    if (numKeys > 0) {
      CommitMultipleOperationsBuilder commit =
          getApi()
              .commitMultipleOperations()
              .branch(branch)
              .commitMeta(CommitMeta.fromMessage("commit"));
      for (int i = 0; i < numKeys; i++) {
        commit.operation(Put.of(contentKey.apply(i), table.apply(i)));
      }
      branch = commit.commit();
    }

    Set<ContentKey> contents = new HashSet<>();
    String token = null;
    for (int i = 0; ; i += pageSize) {
      EntriesResponse response =
          getApi().getEntries().reference(branch).maxRecords(pageSize).pageToken(token).get();

      for (EntriesResponse.Entry entry : response.getEntries()) {
        soft.assertThat(contents.add(entry.getName()))
            .describedAs("offset: %d , entry: %s", i, entry)
            .isTrue();
      }
      soft.assertThat(contents).hasSize(Math.min(i + pageSize, numKeys));
      if (i + pageSize < numKeys) {
        soft.assertThat(response.getToken())
            .describedAs("offset: %d", i)
            .isNotEmpty()
            .isNotEqualTo(token);
        soft.assertThat(response.isHasMore()).describedAs("offset: %d", i).isTrue();
        token = response.getToken();
      } else {
        soft.assertThat(response.getToken()).describedAs("offset: %d", i).isNull();
        soft.assertThat(response.isHasMore()).describedAs("offset: %d", i).isFalse();
        break;
      }
    }

    soft.assertThat(contents)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, numKeys).mapToObj(contentKey).collect(toSet()));
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByType(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch branch = createBranch("filterTypes");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable tam = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergView tb = IcebergView.of("pathx", 1, 1, "select * from table", "Dremio");
    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(a, tam))
            .operation(Put.of(b, tb))
            .commitMeta(CommitMeta.fromMessage("commit"))
            .commit();
    List<Entry> entries =
        getApi().getEntries().reference(refMode.transform(branch)).get().getEntries();
    Map<ContentKey, Content.Type> expect =
        ImmutableMap.of(a, Content.Type.ICEBERG_TABLE, b, Content.Type.ICEBERG_VIEW);
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrderElementsOf(expect.entrySet());

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.contentType=='ICEBERG_TABLE'")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactly(immutableEntry(a, Content.Type.ICEBERG_TABLE));

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.contentType=='ICEBERG_VIEW'")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactly(immutableEntry(b, Content.Type.ICEBERG_VIEW));

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.contentType in ['ICEBERG_TABLE', 'ICEBERG_VIEW']")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrderElementsOf(expect.entrySet());
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByName(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch branch = createBranch("filterEntriesByName");
    ContentKey first = ContentKey.of("a", "b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b", "c", "secondTable");
    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)))
            .operation(Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)))
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .commit();

    List<Entry> entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.name.startsWith('first')")
            .get()
            .getEntries();
    soft.assertThat(entries.stream().map(Entry::getName)).containsExactly(first);

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.name.endsWith('Table')")
            .get()
            .getEntries();
    soft.assertThat(entries.stream().map(Entry::getName)).containsExactlyInAnyOrder(first, second);

    getApi()
        .deleteBranch()
        .branchName(branch.getName())
        .hash(getApi().getReference().refName(branch.getName()).get().getHash())
        .delete();
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByFullKeyName(ReferenceMode refMode)
      throws BaseNessieClientServerException {
    Branch branch = createBranch("filterEntriesByFullKeyName");
    ContentKey first = ContentKey.of("a", "b", "c", "table");
    ContentKey second = ContentKey.of("d", "b", "c", "table");
    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)))
            .operation(Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)))
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .commit();

    List<Entry> entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.key == 'a.b.c.table'")
            .get()
            .getEntries();
    soft.assertThat(entries.stream().map(Entry::getName)).containsExactly(first);

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.key.endsWith('.b.c.table')")
            .get()
            .getEntries();
    soft.assertThat(entries.stream().map(Entry::getName)).containsExactlyInAnyOrder(first, second);

    getApi()
        .deleteBranch()
        .branchName(branch.getName())
        .hash(getApi().getReference().refName(branch.getName()).get().getHash())
        .delete();
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
    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)))
            .operation(Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)))
            .operation(Put.of(third, IcebergTable.of("path3", 42, 42, 42, 42)))
            .operation(Put.of(fourth, IcebergTable.of("path4", 42, 42, 42, 42)))
            .commitMeta(CommitMeta.fromMessage("commit"))
            .commit();

    List<Entry> entries =
        getApi().getEntries().reference(refMode.transform(branch)).get().getEntries();
    soft.assertThat(entries).isNotNull().hasSize(4);

    entries = getApi().getEntries().reference(refMode.transform(branch)).get().getEntries();
    soft.assertThat(entries).isNotNull().hasSize(4);

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a.b')")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .hasSize(2)
        .map(e -> e.getName().getNamespace().name())
        .allMatch(n -> n.startsWith("a.b"));

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a')")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .hasSize(4)
        .map(e -> e.getName().getNamespace().name())
        .allMatch(n -> n.startsWith("a"));

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a.b.c.firstTable')")
            .get()
            .getEntries();
    soft.assertThat(entries).isEmpty();

    entries =
        getApi()
            .getEntries()
            .reference(refMode.transform(branch))
            .filter("entry.namespace.startsWith('a.fourthTable')")
            .get()
            .getEntries();
    soft.assertThat(entries).isEmpty();

    getApi()
        .deleteBranch()
        .branchName(branch.getName())
        .hash(getApi().getReference().refName(branch.getName()).get().getHash())
        .delete();
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  @NessieApiVersions(versions = NessieApiVersion.V1)
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
    CommitMultipleOperationsBuilder commit = getApi().commitMultipleOperations().branch(branch);
    for (int i = 0; i < keys.size(); i++) {
      commit.operation(Put.of(keys.get(i), IcebergTable.of("path" + i, 42, 42, 42, 42)));
    }
    commit.commitMeta(CommitMeta.fromMessage("commit")).commit();
    branch = (Branch) getApi().getReference().refName(branch.getName()).get();

    Reference reference = refMode.transform(branch);
    List<Entry> entries =
        getApi().getEntries().reference(reference).namespaceDepth(0).get().getEntries();
    soft.assertThat(entries).isNotNull().hasSize(6);

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(0)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    soft.assertThat(entries).isNotNull().hasSize(5);

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(1)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .hasSize(1)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactly(immutableEntry(ContentKey.of("a"), Content.Type.NAMESPACE));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(2)
            .filter("entry.namespace.matches('a(\\\\.|$)')")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .hasSize(3)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(ContentKey.of("a", "boo"), Content.Type.NAMESPACE),
            immutableEntry(ContentKey.of("a", "b"), Content.Type.NAMESPACE),
            immutableEntry(ContentKey.of("a", "thirdTable"), Content.Type.ICEBERG_TABLE));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(3)
            .filter("entry.namespace.matches('a\\\\.b(\\\\.|$)')")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .hasSize(2)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(ContentKey.of("a", "b", "fourthTable"), Content.Type.ICEBERG_TABLE),
            immutableEntry(ContentKey.of("a", "b", "c"), Content.Type.NAMESPACE));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(4)
            .filter("entry.namespace.matches('a\\\\.b\\\\.c(\\\\.|$)')")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .hasSize(2)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(ContentKey.of("a", "b", "c", "secondTable"), Content.Type.ICEBERG_TABLE),
            immutableEntry(ContentKey.of("a", "b", "c", "firstTable"), Content.Type.ICEBERG_TABLE));

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(5)
            .filter("entry.namespace.matches('(\\\\.|$)')")
            .get()
            .getEntries();
    soft.assertThat(entries).isEmpty();

    entries =
        getApi()
            .getEntries()
            .reference(reference)
            .namespaceDepth(3)
            .filter("entry.namespace.matches('(\\\\.|$)')")
            .get()
            .getEntries();
    soft.assertThat(entries)
        .hasSize(3)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(ContentKey.of("a", "boo", "fifthTable"), Content.Type.ICEBERG_TABLE),
            immutableEntry(ContentKey.of("a", "b", "fourthTable"), Content.Type.ICEBERG_TABLE),
            immutableEntry(ContentKey.of("a", "b", "c"), Content.Type.NAMESPACE));

    if (ReferenceMode.DETACHED != refMode) {
      // check that implicit namespaces are properly detected
      checkNamespaces(
          reference,
          Arrays.asList("a", "a.b", "a.boo", "a.b.c"),
          Arrays.asList(first, second, third, fourth, fifth));
    }
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void fetchEntriesByNamelessReference() throws BaseNessieClientServerException {
    Branch branch = createBranch("fetchEntriesByNamelessReference");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable ta = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergView tb = IcebergView.of("pathx", 1, 1, "select * from table", "Dremio");
    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(a, ta))
            .operation(Put.of(b, tb))
            .commitMeta(CommitMeta.fromMessage("commit 1"))
            .commit();
    List<Entry> entries = getApi().getEntries().hashOnRef(branch.getHash()).get().getEntries();
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(a, Content.Type.ICEBERG_TABLE),
            immutableEntry(b, Content.Type.ICEBERG_VIEW));
  }

  private void checkNamespaces(
      Reference reference, List<String> knownNamespaces, List<ContentKey> knownContentKeys)
      throws NessieReferenceNotFoundException, NessieNamespaceNotFoundException {

    soft.assertThat(
            getApi()
                .getMultipleNamespaces()
                .reference(reference)
                .namespace("a")
                .get()
                .getNamespaces())
        .hasSize(4);
    for (String namespace : knownNamespaces) {
      Namespace ns = Namespace.parse(namespace);
      soft.assertThat(getApi().getNamespace().reference(reference).namespace(ns).get()).isNotNull();

      soft.assertThatThrownBy(
              () -> getApi().createNamespace().reference(reference).namespace(ns).create())
          .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
          .hasMessage(String.format("Namespace '%s' already exists", namespace));

      soft.assertThatThrownBy(
              () -> getApi().deleteNamespace().reference(reference).namespace(ns).delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class)
          .hasMessage(String.format("Namespace '%s' is not empty", namespace));
    }

    // unknown in the sense that these are actual tables and not namespaces
    List<String> unknownNamespaces =
        knownContentKeys.stream().map(ContentKey::toString).collect(Collectors.toList());
    for (String namespace : unknownNamespaces) {
      soft.assertThatThrownBy(
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
