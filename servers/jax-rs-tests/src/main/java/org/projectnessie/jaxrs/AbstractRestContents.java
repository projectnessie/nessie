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
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestContents extends AbstractRestCommitLog {

  public static final class ContentAndOperationType {
    final Content.Type type;
    final Operation operation;

    public ContentAndOperationType(Content.Type type, Operation operation) {
      this.type = type;
      this.operation = operation;
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
            Put.of(
                ContentKey.of("a", "view_dremio"),
                IcebergView.of("/iceberg/view", 1, 1, "Dremio", "SELECT foo FROM dremio"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW,
            Put.of(
                ContentKey.of("a", "view_presto"),
                IcebergView.of("/iceberg/view", 1, 1, "Presto", "SELECT foo FROM presto"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW,
            Put.of(
                ContentKey.of("b", "view_spark"),
                IcebergView.of("/iceberg/view2", 1, 1, "Spark", "SELECT foo FROM spark"))),
        new ContentAndOperationType(
            Content.Type.DELTA_LAKE_TABLE,
            Put.of(
                ContentKey.of("c", "delta"),
                ImmutableDeltaLakeTable.builder()
                    .addCheckpointLocationHistory("checkpoint")
                    .addMetadataLocationHistory("metadata")
                    .build())),
        new ContentAndOperationType(
            Content.Type.ICEBERG_TABLE, Delete.of(ContentKey.of("a", "iceberg_delete"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_TABLE, Unchanged.of(ContentKey.of("a", "iceberg_unchanged"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW, Delete.of(ContentKey.of("a", "view_dremio_delete"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW, Unchanged.of(ContentKey.of("a", "view_dremio_unchanged"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW, Delete.of(ContentKey.of("a", "view_spark_delete"))),
        new ContentAndOperationType(
            Content.Type.ICEBERG_VIEW, Unchanged.of(ContentKey.of("a", "view_spark_unchanged"))),
        new ContentAndOperationType(
            Content.Type.DELTA_LAKE_TABLE, Delete.of(ContentKey.of("a", "delta_delete"))),
        new ContentAndOperationType(
            Content.Type.DELTA_LAKE_TABLE, Unchanged.of(ContentKey.of("a", "delta_unchanged"))));
  }

  @Test
  public void verifyAllContentAndOperationTypes() throws BaseNessieClientServerException {
    Branch branch = createBranch("contentAndOperationAll");

    List<ContentAndOperationType> contentAndOps =
        contentAndOperationTypes().collect(Collectors.toList());

    CommitMultipleOperationsBuilder commit =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("verifyAllContentAndOperationTypes"));
    contentAndOps.forEach(contentAndOp -> commit.operation(contentAndOp.operation));
    Branch committed = commit.commit();

    assertAll(
        () -> {
          List<Entry> entries = getApi().getEntries().refName(branch.getName()).get().getEntries();
          List<Entry> expect =
              contentAndOps.stream()
                  .filter(c -> c.operation instanceof Put)
                  .map(c -> Entry.builder().type(c.type).name(c.operation.getKey()).build())
                  .collect(Collectors.toList());
          assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
        },
        () -> {
          // Diff against of committed HEAD and previous commit must yield the content in the
          // Put operations
          assertThat(getApi().getDiff().fromRef(committed).toRef(branch).get())
              .extracting(DiffResponse::getDiffs, list(DiffEntry.class))
              .extracting(DiffEntry::getKey, e -> clearIdOnContent(e.getFrom()), DiffEntry::getTo)
              .containsExactlyInAnyOrderElementsOf(
                  contentAndOps.stream()
                      .map(c -> c.operation)
                      .filter(op -> op instanceof Put)
                      .map(Put.class::cast)
                      .map(put -> tuple(put.getKey(), put.getContent(), null))
                      .collect(Collectors.toList()));
        },
        () -> {
          // Verify that 'get contents' for the HEAD commit returns exactly the committed contents
          List<ContentKey> allKeys =
              contentAndOps.stream()
                  .map(contentAndOperationType -> contentAndOperationType.operation.getKey())
                  .collect(Collectors.toList());
          Map<ContentKey, Content> expected =
              contentAndOps.stream()
                  .filter(c -> c.operation instanceof Put)
                  .collect(
                      Collectors.toMap(
                          e -> e.operation.getKey(), e -> ((Put) e.operation).getContent()));
          assertThat(getApi().getContent().reference(committed).keys(allKeys).get())
              .containsOnlyKeys(expected.keySet())
              .allSatisfy(
                  (key, content) ->
                      assertThat(clearIdOnContent(content)).isEqualTo(expected.get(key)));
        },
        () ->
            // Verify that the operations on the HEAD commit contains the committed operations
            assertThat(
                    getApi()
                        .getCommitLog()
                        .reference(committed)
                        .fetch(FetchOption.ALL)
                        .get()
                        .getLogEntries())
                .element(0)
                .extracting(LogEntry::getOperations)
                .extracting(this::clearIdOnOperations, list(Operation.class))
                .containsExactlyInAnyOrderElementsOf(
                    contentAndOps.stream()
                        .map(c -> c.operation)
                        .filter(op -> !(op instanceof Unchanged))
                        .collect(Collectors.toList())));
  }

  @ParameterizedTest
  @MethodSource("contentAndOperationTypes")
  public void verifyContentAndOperationTypesIndividually(
      ContentAndOperationType contentAndOperationType) throws BaseNessieClientServerException {
    Branch branch = createBranch("contentAndOperation_" + contentAndOperationType);
    CommitMultipleOperationsBuilder commit =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("commit " + contentAndOperationType))
            .operation(contentAndOperationType.operation);
    Branch committed = commit.commit();

    // Oh, yea - this is weird. The property ContentAndOperationType.operation.key.namespace is null
    // (!!!) here, because somehow JUnit @MethodSource implementation re-constructs the objects
    // returned from the source-method contentAndOperationTypes.
    ContentKey fixedContentKey =
        ContentKey.of(contentAndOperationType.operation.getKey().getElements());

    if (contentAndOperationType.operation instanceof Put) {
      Put put = (Put) contentAndOperationType.operation;
      assertAll(
          () -> {
            List<Entry> entries =
                getApi().getEntries().refName(branch.getName()).get().getEntries();
            assertThat(entries)
                .containsExactly(
                    Entry.builder()
                        .name(fixedContentKey)
                        .type(contentAndOperationType.type)
                        .build());
          },
          () -> {
            // Diff against of committed HEAD and previous commit must yield the content in the
            // Put operation
            assertThat(getApi().getDiff().fromRef(committed).toRef(branch).get())
                .extracting(DiffResponse::getDiffs, list(DiffEntry.class))
                .extracting(DiffEntry::getKey, e -> clearIdOnContent(e.getFrom()), DiffEntry::getTo)
                .containsExactly(tuple(fixedContentKey, put.getContent(), null));
          },
          () -> {
            // Compare content on HEAD commit with the committed content
            Map<ContentKey, Content> content =
                getApi().getContent().key(fixedContentKey).reference(committed).get();
            assertThat(content)
                .extractingByKey(fixedContentKey)
                .extracting(this::clearIdOnContent)
                .isEqualTo(put.getContent());
          },
          () -> {
            // Compare operation on HEAD commit with the committed operation
            LogResponse log =
                getApi().getCommitLog().reference(committed).fetch(FetchOption.ALL).get();
            assertThat(log)
                .extracting(LogResponse::getLogEntries, list(LogEntry.class))
                .element(0)
                .extracting(LogEntry::getOperations, list(Operation.class))
                .element(0)
                // Clear content ID for comparison
                .extracting(this::clearIdOnOperation)
                .isEqualTo(put);
          });
    } else {
      // not a Put operation
      assertAll(
          () -> {
            List<Entry> entries =
                getApi().getEntries().refName(branch.getName()).get().getEntries();
            assertThat(entries).isEmpty();
          },
          () -> {
            // Diff against of committed HEAD and previous commit must yield the content in the
            // Put operations
            assertThat(getApi().getDiff().fromRef(committed).toRef(branch).get())
                .extracting(DiffResponse::getDiffs, list(DiffEntry.class))
                .isEmpty();
          },
          () -> {
            // Compare content on HEAD commit with the committed content
            Map<ContentKey, Content> content =
                getApi().getContent().key(fixedContentKey).reference(committed).get();
            assertThat(content).isEmpty();
          },
          () -> {
            // Compare operation on HEAD commit with the committed operation
            LogResponse log =
                getApi().getCommitLog().reference(committed).fetch(FetchOption.ALL).get();
            assertThat(log)
                .extracting(LogResponse::getLogEntries, list(LogEntry.class))
                .element(0)
                .extracting(LogEntry::getOperations)
                .satisfies(
                    ops -> {
                      if (contentAndOperationType.operation instanceof Delete) {
                        // Delete ops are persisted - must occur in commit
                        assertThat(ops).containsExactly(contentAndOperationType.operation);
                      } else if (contentAndOperationType.operation instanceof Unchanged) {
                        // Unchanged ops are not persisted - cannot occur in commit
                        assertThat(ops).isNullOrEmpty();
                      } else {
                        fail("Unexpected operation " + contentAndOperationType.operation);
                      }
                    });
          });
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
    try {
      return (Content)
          content
              .getClass()
              .getDeclaredMethod("withId", String.class)
              .invoke(content, new Object[1]);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void multiget() throws BaseNessieClientServerException {
    Branch branch = createBranch("foo");
    ContentKey keyA = ContentKey.of("a");
    ContentKey keyB = ContentKey.of("b");
    IcebergTable tableA = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergTable tableB = IcebergTable.of("path2", 42, 42, 42, 42);
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(keyA, tableA))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commit();
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(keyB, tableB))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .commit();
    Map<ContentKey, Content> response =
        getApi()
            .getContent()
            .key(keyA)
            .key(keyB)
            .key(ContentKey.of("noexist"))
            .refName("foo")
            .get();
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
}
