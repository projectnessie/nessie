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

import static com.google.common.collect.Maps.immutableEntry;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

public abstract class AbstractTestEntries extends BaseTestServiceImpl {

  @ParameterizedTest
  @ValueSource(ints = {0, 20, 22})
  public void entriesPaging(int numKeys) throws BaseNessieClientServerException {
    Branch branch =
        ensureNamespacesForKeysExist(createBranch("entriesPaging"), ContentKey.of("key", "0"));

    IntFunction<ContentKey> contentKey = i -> ContentKey.of("key", Integer.toString(i));
    IntFunction<IcebergTable> table = i -> IcebergTable.of("meta" + i, 1, 2, 3, 4);
    int pageSize = 5;

    if (numKeys > 0) {
      branch =
          commit(
                  branch,
                  fromMessage("commit"),
                  IntStream.range(0, numKeys)
                      .mapToObj(i -> Put.of(contentKey.apply(i), table.apply(i)))
                      .toArray(Operation[]::new))
              .getTargetBranch();
    }

    AtomicReference<Reference> effectiveReference = new AtomicReference<>();
    List<EntriesResponse.Entry> contents =
        withoutNamespaces(
            pagedEntries(branch, null, pageSize, numKeys, effectiveReference::set, true));

    soft.assertThat(effectiveReference).hasValue(branch);

    soft.assertThat(contents)
        .extracting(
            EntriesResponse.Entry::getName,
            e -> IcebergTable.builder().from(e.getContent()).id(null).build())
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, numKeys)
                .mapToObj(i -> tuple(contentKey.apply(i), table.apply(i)))
                .collect(toSet()));
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByType(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch branch = createBranch("filterTypes");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable tam = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergView tb = IcebergView.of("pathx", 1, 1);
    branch = commit(branch, fromMessage("commit"), Put.of(a, tam), Put.of(b, tb)).getTargetBranch();
    List<EntriesResponse.Entry> entries = entries(refMode.transform(branch));
    Map<ContentKey, Content.Type> expect =
        ImmutableMap.of(a, Content.Type.ICEBERG_TABLE, b, Content.Type.ICEBERG_VIEW);
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrderElementsOf(expect.entrySet());

    entries = entries(refMode.transform(branch), null, "entry.contentType=='ICEBERG_TABLE'");
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactly(immutableEntry(a, Content.Type.ICEBERG_TABLE));

    entries = entries(refMode.transform(branch), null, "entry.contentType=='ICEBERG_VIEW'");
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactly(immutableEntry(b, Content.Type.ICEBERG_VIEW));

    entries =
        entries(
            refMode.transform(branch),
            null,
            "entry.contentType in ['ICEBERG_TABLE', 'ICEBERG_VIEW']");
    soft.assertThat(entries)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrderElementsOf(expect.entrySet());
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByName(ReferenceMode refMode) throws BaseNessieClientServerException {
    ContentKey first = ContentKey.of("a", "b.b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b.b", "c", "secondTable");
    ContentKey fourth = ContentKey.of("a", "b.bbb", "Foo");
    ContentKey abb = ContentKey.of("a", "b.b");
    ContentKey abbc = ContentKey.of("a", "b.b", "c");
    ContentKey abbbb = ContentKey.of("a", "b.bbb");
    Branch branch =
        ensureNamespacesForKeysExist(createBranch("filterEntriesByName"), first, second, fourth);
    branch =
        commit(
                branch,
                fromMessage("commit 1"),
                Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)),
                Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)),
                Put.of(fourth, IcebergTable.of("path4", 42, 42, 42, 42)))
            .getTargetBranch();

    List<EntriesResponse.Entry> entries =
        entries(refMode.transform(branch), null, "entry.name.startsWith('first')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName)).containsExactly(first);

    entries = entries(refMode.transform(branch), null, "entry.name.endsWith('Table')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(first, second);

    entries = entries(refMode.transform(branch), null, "entry.namespace.startsWith('a')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(first, second, abb, abbc, abbbb, fourth);

    entries = entries(refMode.transform(branch), null, "entry.namespace.startsWith('a.b\u001db.')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(first, second);

    entries =
        entries(refMode.transform(branch), null, "entry.encodedKey.startsWith('a.b\u001dbbb.')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(fourth);

    entries =
        entries(refMode.transform(branch), null, "entry.namespace.startsWith('a.b\u001dbbb.')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName)).isEmpty();

    entries = entries(refMode.transform(branch), null, "entry.namespace.startsWith('a.b\u001db.')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(first, second);

    entries =
        entries(
            refMode.transform(branch),
            null,
            "entry.encodedKey == 'a.b\u001dbbb' || entry.encodedKey.startsWith('a.b\u001dbbb.')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(abbbb, fourth);
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByFullKeyName(ReferenceMode refMode)
      throws BaseNessieClientServerException {
    ContentKey first = ContentKey.of("a", "b", "c", "table");
    ContentKey second = ContentKey.of("d", "b", "c", "table");
    Branch branch =
        ensureNamespacesForKeysExist(createBranch("filterEntriesByFullKeyName"), first, second);
    branch =
        commit(
                branch,
                fromMessage("commit 1"),
                Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)),
                Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)))
            .getTargetBranch();

    List<EntriesResponse.Entry> entries =
        entries(refMode.transform(branch), null, "entry.key == 'a.b.c.table'");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName)).containsExactly(first);

    entries = entries(refMode.transform(branch), null, "entry.key.endsWith('.b.c.table')");
    soft.assertThat(entries.stream().map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(first, second);
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByNamespace(ReferenceMode refMode)
      throws BaseNessieClientServerException {
    ContentKey first = ContentKey.of("a", "b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b", "c", "secondTable");
    ContentKey third = ContentKey.of("a", "thirdTable");
    ContentKey fourth = ContentKey.of("a", "fourthTable");
    Branch branch =
        ensureNamespacesForKeysExist(
            createBranch("filterEntriesByNamespace"), first, second, third, fourth);
    branch =
        commit(
                branch,
                fromMessage("commit"),
                Put.of(first, IcebergTable.of("path1", 42, 42, 42, 42)),
                Put.of(second, IcebergTable.of("path2", 42, 42, 42, 42)),
                Put.of(third, IcebergTable.of("path3", 42, 42, 42, 42)),
                Put.of(fourth, IcebergTable.of("path4", 42, 42, 42, 42)))
            .getTargetBranch();

    List<EntriesResponse.Entry> entries = withoutNamespaces(entries(refMode.transform(branch)));
    soft.assertThat(entries).isNotNull().hasSize(4);

    entries = withoutNamespaces(entries(refMode.transform(branch)));
    soft.assertThat(entries).isNotNull().hasSize(4);

    entries =
        withoutNamespaces(
            entries(refMode.transform(branch), null, "entry.namespace.startsWith('a.b')"));
    soft.assertThat(entries)
        .hasSize(2)
        .map(e -> e.getName().getNamespace().name())
        .allMatch(n -> n.startsWith("a.b"));

    entries =
        withoutNamespaces(
            entries(refMode.transform(branch), null, "entry.namespace.startsWith('a')"));
    soft.assertThat(entries)
        .hasSize(4)
        .map(e -> e.getName().getNamespace().name())
        .allMatch(n -> n.startsWith("a"));

    entries =
        withoutNamespaces(
            entries(
                refMode.transform(branch), null, "entry.namespace.startsWith('a.b.c.firstTable')"));
    soft.assertThat(entries).isEmpty();

    entries =
        withoutNamespaces(
            entries(
                refMode.transform(branch), null, "entry.namespace.startsWith('a.fourthTable')"));
    soft.assertThat(entries).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void filterEntriesByNamespaceAndPrefixDepth(ReferenceMode refMode)
      throws BaseNessieClientServerException {
    ContentKey first = ContentKey.of("a", "b", "c", "firstTable");
    ContentKey second = ContentKey.of("a", "b", "c", "secondTable");
    ContentKey third = ContentKey.of("a", "thirdTable");
    ContentKey fourth = ContentKey.of("a", "b", "fourthTable");
    ContentKey fifth = ContentKey.of("a", "boo", "fifthTable");
    ContentKey withoutNamespace = ContentKey.of("withoutNamespace");
    Branch branch =
        ensureNamespacesForKeysExist(
            createBranch("filterEntriesByNamespaceAndPrefixDepth"),
            first,
            second,
            third,
            fourth,
            fifth);
    List<ContentKey> keys = ImmutableList.of(first, second, third, fourth, fifth, withoutNamespace);
    branch =
        commit(
                branch,
                fromMessage("commit"),
                IntStream.range(0, keys.size())
                    .mapToObj(i -> Put.of(keys.get(i), IcebergTable.of("path" + i, 42, 42, 42, 42)))
                    .toArray(Operation[]::new))
            .getTargetBranch();

    Reference reference = refMode.transform(branch);
    List<EntriesResponse.Entry> entries = withoutNamespaces(entries(reference, 0, null));
    soft.assertThat(entries).isNotNull().hasSize(6);

    entries = withoutNamespaces(entries(reference, 0, "entry.namespace.matches('a(\\\\.|$)')"));
    soft.assertThat(entries).isNotNull().hasSize(5);

    entries = entries(reference, 1, "entry.namespace.matches('a(\\\\.|$)')");
    soft.assertThat(entries)
        .hasSize(1)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactly(immutableEntry(ContentKey.of("a"), Content.Type.NAMESPACE));

    entries = entries(reference, 2, "entry.namespace.matches('a(\\\\.|$)')");
    soft.assertThat(entries)
        .hasSize(3)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(ContentKey.of("a", "boo"), Content.Type.NAMESPACE),
            immutableEntry(ContentKey.of("a", "b"), Content.Type.NAMESPACE),
            immutableEntry(ContentKey.of("a", "thirdTable"), Content.Type.ICEBERG_TABLE));

    entries = entries(reference, 3, "entry.namespace.matches('a\\\\.b(\\\\.|$)')");
    soft.assertThat(entries)
        .hasSize(2)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(ContentKey.of("a", "b", "fourthTable"), Content.Type.ICEBERG_TABLE),
            immutableEntry(ContentKey.of("a", "b", "c"), Content.Type.NAMESPACE));

    String filterThatFindsTwoTables = "entry.namespace.matches('a\\\\.b\\\\.c(\\\\.|$)')";
    entries = entries(reference, 4, filterThatFindsTwoTables);
    soft.assertThat(entries)
        .hasSize(2)
        .map(e -> immutableEntry(e.getName(), e.getType()))
        .containsExactlyInAnyOrder(
            immutableEntry(ContentKey.of("a", "b", "c", "secondTable"), Content.Type.ICEBERG_TABLE),
            immutableEntry(ContentKey.of("a", "b", "c", "firstTable"), Content.Type.ICEBERG_TABLE));

    // namespaceDepth filter always returns contentId of real entries
    entries = entries(reference.getName(), reference.getHash(), 4, filterThatFindsTwoTables, false);
    soft.assertThat(entries)
        .hasSize(2)
        .allSatisfy(
            entry -> {
              assertThat(entry.getType()).isEqualTo(Content.Type.ICEBERG_TABLE);
              assertThat(entry.getContentId()).isNotNull();
              assertThat(entry.getContent()).isNull();
            });

    // namespaceDepth filter returns content of real entries if requested
    entries = entries(reference.getName(), reference.getHash(), 4, filterThatFindsTwoTables, true);
    soft.assertThat(entries)
        .hasSize(2)
        .allSatisfy(
            entry -> {
              assertThat(entry.getType()).isEqualTo(Content.Type.ICEBERG_TABLE);
              assertThat(entry.getContentId()).isNotNull();
              assertThat(entry.getContent()).isNotNull();
            });

    entries = entries(reference, 5, "entry.namespace.matches('(\\\\.|$)')");
    soft.assertThat(entries).isEmpty();

    entries = entries(reference, 3, "entry.namespace.matches('(\\\\.|$)')");
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
  public void fetchEntriesByNamelessReference() throws BaseNessieClientServerException {
    Branch branch = createBranch("fetchEntriesByNamelessReference");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable ta = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergView tb = IcebergView.of("pathx", 1, 1);
    branch =
        commit(branch, fromMessage("commit 1"), Put.of(a, ta), Put.of(b, tb)).getTargetBranch();
    List<EntriesResponse.Entry> entries = entries(Detached.of(branch.getHash()));
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
            namespaceApi()
                .getNamespaces(reference.getName(), reference.getHash(), Namespace.of("a"))
                .getNamespaces())
        .hasSize(4);
    for (String namespace : knownNamespaces) {
      Namespace ns = Namespace.parse(namespace);
      soft.assertThat(namespaceApi().getNamespace(reference.getName(), reference.getHash(), ns))
          .isNotNull();

      soft.assertThatThrownBy(
              () -> namespaceApi().createNamespace(reference.getName(), ns, API_WRITE))
          .cause()
          .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
          .hasMessage(String.format("Namespace '%s' already exists", namespace));

      soft.assertThatThrownBy(() -> namespaceApi().deleteNamespace(reference.getName(), ns))
          .cause()
          .isInstanceOf(NessieNamespaceNotEmptyException.class)
          .hasMessage(String.format("Namespace '%s' is not empty", namespace));
    }

    // unknown in the sense that these are actual tables and not namespaces
    List<String> unknownNamespaces =
        knownContentKeys.stream().map(ContentKey::toString).collect(Collectors.toList());
    for (String namespace : unknownNamespaces) {
      soft.assertThatThrownBy(
              () ->
                  namespaceApi()
                      .getNamespace(
                          reference.getName(), reference.getHash(), Namespace.parse(namespace)))
          .isInstanceOf(NessieNamespaceNotFoundException.class)
          .hasMessage(String.format("Namespace '%s' does not exist", namespace));
    }
  }
}
