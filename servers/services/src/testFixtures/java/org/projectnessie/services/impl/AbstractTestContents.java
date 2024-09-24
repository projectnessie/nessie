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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.ALL;
import static org.projectnessie.versioned.RequestMeta.API_READ;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.UDF;

public abstract class AbstractTestContents extends BaseTestServiceImpl {

  public static final class ContentAndOperationType {
    final Content.Type type;
    final Operation operation;
    final Put prepare;

    public ContentAndOperationType(Content.Type type, Operation operation) {
      this(type, operation, null);
    }

    public ContentAndOperationType(Content.Type type, Operation operation, Put prepare) {
      this.type = type;
      this.operation = operation;
      this.prepare = prepare;
    }

    @Override
    public String toString() {
      String s = opString(operation);
      return s + "_" + operation.getKey().toPathString();
    }

    private static String opString(Operation operation) {
      if (operation instanceof Put) {
        return "Put_" + ((Put) operation).getContent().getClass().getSimpleName();
      } else {
        return operation.getClass().getSimpleName();
      }
    }
  }

  public static Stream<ContentAndOperationType> contentAndOperationTypes() {
    return Stream.of(
        new ContentAndOperationType(
            Content.Type.ICEBERG_TABLE,
            Put.of(
                ContentKey.of("a", "iceberg"), IcebergTable.of("/iceberg/table", 42, 42, 42, 42))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW,
            Put.of(ContentKey.of("a", "view"), IcebergView.of("/iceberg/view", 1, 1))),
        new ContentAndOperationType(
            Content.Type.UDF, Put.of(ContentKey.of("a", "udf"), UDF.udf("/udf-meta", "42", "666"))),
        new ContentAndOperationType(
            Content.Type.DELTA_LAKE_TABLE,
            Put.of(
                ContentKey.of("c", "delta"),
                ImmutableDeltaLakeTable.builder()
                    .addCheckpointLocationHistory("checkpoint")
                    .addMetadataLocationHistory("metadata")
                    .build())),
        new ContentAndOperationType(
            Content.Type.ICEBERG_TABLE,
            Delete.of(ContentKey.of("a", "iceberg_delete")),
            Put.of(
                ContentKey.of("a", "iceberg_delete"),
                IcebergTable.of("/iceberg/table", 42, 42, 42, 42))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_TABLE,
            Unchanged.of(ContentKey.of("a", "iceberg_unchanged")),
            Put.of(
                ContentKey.of("a", "iceberg_unchanged"),
                IcebergTable.of("/iceberg/table", 42, 42, 42, 42))),
        new ContentAndOperationType(
            Content.Type.UDF,
            Delete.of(ContentKey.of("a", "udf_delete")),
            Put.of(ContentKey.of("a", "udf_delete"), UDF.udf("/udf-metadata", "42", "666"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW,
            Delete.of(ContentKey.of("a", "view_delete")),
            Put.of(ContentKey.of("a", "view_delete"), IcebergView.of("/iceberg/view", 42, 42))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW,
            Unchanged.of(ContentKey.of("a", "view_unchanged")),
            Put.of(ContentKey.of("a", "view_unchanged"), IcebergView.of("/iceberg/view", 42, 42))),
        new ContentAndOperationType(
            Content.Type.UDF,
            Unchanged.of(ContentKey.of("a", "udf_unchanged")),
            Put.of(ContentKey.of("a", "udf_unchanged"), UDF.udf("/udf-meta-data", "42", "666"))),
        new ContentAndOperationType(
            Content.Type.DELTA_LAKE_TABLE,
            Delete.of(ContentKey.of("a", "delta_delete")),
            Put.of(
                ContentKey.of("a", "delta_delete"),
                ImmutableDeltaLakeTable.builder()
                    .addMetadataLocationHistory("/delta/table")
                    .addCheckpointLocationHistory("/delta/history")
                    .lastCheckpoint("/delta/check")
                    .build())),
        new ContentAndOperationType(
            Content.Type.DELTA_LAKE_TABLE,
            Unchanged.of(ContentKey.of("a", "delta_unchanged")),
            Put.of(
                ContentKey.of("a", "delta_unchanged"),
                ImmutableDeltaLakeTable.builder()
                    .addMetadataLocationHistory("/delta/table")
                    .addCheckpointLocationHistory("/delta/history")
                    .lastCheckpoint("/delta/check")
                    .build())));
  }

  @Test
  public void verifyAllContentAndOperationTypes() throws BaseNessieClientServerException {
    Branch branch =
        ensureNamespacesForKeysExist(
            createBranch("contentAndOperationAll"),
            contentAndOperationTypes().map(co -> co.operation.getKey()).toArray(ContentKey[]::new));

    List<ContentAndOperationType> contentAndOps =
        contentAndOperationTypes().collect(Collectors.toList());

    branch =
        commit(
                branch,
                fromMessage("verifyAllContentAndOperationTypes prepare"),
                contentAndOps.stream()
                    .filter(co -> co.prepare != null)
                    .map(co -> co.prepare)
                    .toArray(Operation[]::new))
            .getTargetBranch();

    Branch committed =
        commit(
                branch,
                fromMessage("verifyAllContentAndOperationTypes"),
                contentAndOps.stream()
                    .map(contentAndOp -> contentAndOp.operation)
                    .toArray(Operation[]::new))
            .getTargetBranch();

    List<EntriesResponse.Entry> entries = entries(branch.getName(), null);
    Map<ContentKey, Content.Type> expect =
        contentAndOps.stream()
            .filter(c -> c.operation instanceof Put)
            .collect(Collectors.toMap(c -> c.operation.getKey(), c -> c.type));
    Map<ContentKey, Content.Type> notExpect =
        contentAndOps.stream()
            .filter(c -> c.operation instanceof Delete)
            .collect(Collectors.toMap(c -> c.operation.getKey(), c -> c.type));
    soft.assertThat(entries)
        .map(e -> Maps.immutableEntry(e.getName(), e.getType()))
        .containsAll(expect.entrySet())
        .doesNotContainAnyElementsOf(notExpect.entrySet());

    // Diff against of committed HEAD and previous commit must yield the content in the
    // Put operations
    soft.assertThat(diff(committed, branch))
        .filteredOn(e -> e.getFrom() != null)
        .extracting(BaseTestServiceImpl::diffEntryWithoutContentId)
        .containsExactlyInAnyOrderElementsOf(
            contentAndOps.stream()
                .map(c -> c.operation)
                .filter(op -> op instanceof Put)
                .map(Put.class::cast)
                .map(put -> DiffEntry.diffEntry(put.getKey(), put.getContent()))
                .collect(Collectors.toList()));

    // Verify that 'get contents' for the HEAD commit returns exactly the committed contents
    ContentKey[] allKeys =
        contentAndOps.stream()
            .map(contentAndOperationType -> contentAndOperationType.operation.getKey())
            .toArray(ContentKey[]::new);
    Map<ContentKey, Content> expected =
        contentAndOps.stream()
            .map(
                c -> {
                  if (c.operation instanceof Put) {
                    return Maps.immutableEntry(
                        c.operation.getKey(), ((Put) c.operation).getContent());
                  }
                  if (c.operation instanceof Unchanged) {
                    return Maps.immutableEntry(c.operation.getKey(), c.prepare.getContent());
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    soft.assertThat(contents(committed, allKeys))
        .containsOnlyKeys(expected.keySet())
        .allSatisfy(
            (key, content) -> assertThat(clearIdOnContent(content)).isEqualTo(expected.get(key)));

    // Verify that the operations on the HEAD commit contains the committed operations
    soft.assertThat(commitLog(committed.getName(), ALL, null, committed.getHash(), null))
        .element(0)
        .extracting(LogEntry::getOperations)
        .extracting(this::clearIdOnOperations, list(Operation.class))
        .containsExactlyInAnyOrderElementsOf(
            contentAndOps.stream()
                .map(c -> c.operation)
                .filter(op -> !(op instanceof Unchanged))
                .collect(Collectors.toList()));
  }

  @ParameterizedTest
  @MethodSource("contentAndOperationTypes")
  public void verifyContentAndOperationTypesIndividually(
      ContentAndOperationType contentAndOperationType) throws BaseNessieClientServerException {
    Branch branch =
        ensureNamespacesForKeysExist(
            createBranch("contentAndOperation_" + contentAndOperationType),
            contentAndOperationType.operation.getKey());

    if (contentAndOperationType.prepare != null) {
      branch =
          commit(
                  branch,
                  fromMessage("verifyAllContentAndOperationTypes prepare"),
                  contentAndOperationType.prepare)
              .getTargetBranch();
    }

    Branch committed =
        commit(
                branch,
                fromMessage("commit " + contentAndOperationType),
                contentAndOperationType.operation)
            .getTargetBranch();

    // Oh, yea - this is weird. The property ContentAndOperationType.operation.key.namespace is null
    // (!!!) here, because somehow JUnit @MethodSource implementation re-constructs the objects
    // returned from the source-method contentAndOperationTypes.
    ContentKey fixedContentKey =
        ContentKey.of(contentAndOperationType.operation.getKey().getElements());

    if (contentAndOperationType.operation instanceof Put) {
      Put put = (Put) contentAndOperationType.operation;

      List<EntriesResponse.Entry> entries = withoutNamespaces(entries(branch.getName(), null));
      soft.assertThat(entries)
          .hasSize(1)
          .extracting(EntriesResponse.Entry::getName, EntriesResponse.Entry::getType)
          .containsExactly(tuple(fixedContentKey, contentAndOperationType.type));

      // Diff against of committed HEAD and previous commit must yield the content in the
      // Put operation
      soft.assertThat(diff(committed, branch))
          .extracting(DiffEntry::getKey, e -> clearIdOnContent(e.getFrom()), DiffEntry::getTo)
          .containsExactly(tuple(fixedContentKey, put.getContent(), null));

      // Compare content on HEAD commit with the committed content
      soft.assertThat(
              contentApi()
                  .getContent(
                      fixedContentKey, committed.getName(), committed.getHash(), false, API_READ))
          .extracting(ContentResponse::getContent)
          .extracting(this::clearIdOnContent)
          .isEqualTo(put.getContent());

      // Compare operation on HEAD commit with the committed operation
      List<LogEntry> log = commitLog(committed.getName(), ALL, null);
      soft.assertThat(log)
          .element(0)
          .extracting(LogEntry::getOperations, list(Operation.class))
          .element(0)
          // Clear content ID for comparison
          .extracting(this::clearIdOnOperation)
          .isEqualTo(put);
    } else if (contentAndOperationType.operation instanceof Delete) {
      List<EntriesResponse.Entry> entries = withoutNamespaces(entries(branch.getName(), null));
      soft.assertThat(entries).isEmpty();

      // Diff against of committed HEAD and previous commit must yield the content in the
      // Put operations
      soft.assertThat(diff(committed, branch)).filteredOn(e -> e.getFrom() != null).isEmpty();

      // Compare content on HEAD commit with the committed content
      soft.assertThatThrownBy(
              () ->
                  contentApi()
                      .getContent(
                          fixedContentKey,
                          committed.getName(),
                          committed.getHash(),
                          false,
                          API_READ))
          .isInstanceOf(NessieNotFoundException.class);

      // Compare operation on HEAD commit with the committed operation
      List<LogEntry> log = commitLog(committed.getName(), ALL, null, committed.getHash(), null);
      soft.assertThat(log)
          .element(0)
          .extracting(LogEntry::getOperations, list(Operation.class))
          .containsExactly(contentAndOperationType.operation);
    } else if (contentAndOperationType.operation instanceof Unchanged) {
      List<EntriesResponse.Entry> entries = withoutNamespaces(entries(branch.getName(), null));
      soft.assertThat(entries)
          .extracting(EntriesResponse.Entry::getName, EntriesResponse.Entry::getType)
          .containsExactly(tuple(fixedContentKey, contentAndOperationType.type));

      // Diff against of committed HEAD and previous commit must yield the content in the
      // Put operations
      soft.assertThat(diff(committed, branch)).filteredOn(e -> e.getFrom() != null).isEmpty();

      // Compare content on HEAD commit with the committed content
      soft.assertThat(
              contentApi()
                  .getContent(
                      fixedContentKey, committed.getName(), committed.getHash(), false, API_READ))
          .extracting(ContentResponse::getContent)
          .extracting(this::clearIdOnContent)
          .isEqualTo(contentAndOperationType.prepare.getContent());

      // Compare operation on HEAD commit with the committed operation
      List<LogEntry> log = commitLog(committed.getName(), ALL, null, committed.getHash(), null);
      soft.assertThat(log).element(0).extracting(LogEntry::getOperations).isNull();
    }
  }

  private List<Operation> clearIdOnOperations(List<Operation> o) {
    return o.stream().map(this::clearIdOnOperation).collect(Collectors.toList());
  }

  private Operation clearIdOnOperation(Operation o) {
    try {
      if (!(o instanceof Put)) {
        return o;
      }
      Put put = (Put) o;
      Content contentWithoutId = clearIdOnContent(put.getContent());
      return ImmutablePut.builder().from(put).content(contentWithoutId).build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Content clearIdOnContent(Content content) {
    return content.withId(null);
  }

  @Test
  public void multiget() throws BaseNessieClientServerException {
    Branch branch = createBranch("foo");
    ContentKey keyA = ContentKey.of("a");
    ContentKey keyB = ContentKey.of("b");
    IcebergTable tableA = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergTable tableB = IcebergTable.of("path2", 42, 42, 42, 42);
    commit(branch, fromMessage("commit 1"), Put.of(keyA, tableA));
    commit(branch, fromMessage("commit 2"), Put.of(keyB, tableB));
    Map<ContentKey, Content> response = contents("foo", null, keyA, keyB, ContentKey.of("noexist"));
    assertThat(response)
        .containsKeys(keyA, keyB)
        .hasEntrySatisfying(
            keyA,
            content ->
                assertThat(content)
                    .isEqualTo(IcebergTable.builder().from(tableA).id(content.getId()).build()))
        .hasEntrySatisfying(
            keyB,
            content ->
                assertThat(content)
                    .isEqualTo(IcebergTable.builder().from(tableB).id(content.getId()).build()))
        .doesNotContainKey(ContentKey.of("noexist"));
  }

  @Test
  public void fetchContentByNamelessReference() throws BaseNessieClientServerException {
    Branch branch = createBranch("fetchContentByNamelessReference");
    IcebergTable t = IcebergTable.of("loc", 1, 2, 3, 4);
    ContentKey key = ContentKey.of("key1");
    Branch committed = commit(branch, fromMessage("test commit"), Put.of(key, t)).getTargetBranch();

    assertThat(contents(committed, key).get(key)).isInstanceOf(IcebergTable.class);
  }
}
