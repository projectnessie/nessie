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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.SqlView.Dialect;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestContents extends AbstractRestCommitLog {

  public static final class ContentAndOperationType {
    final Type type;
    final Operation operation;
    final Operation globalOperation;

    public ContentAndOperationType(Type type, Operation operation) {
      this(type, operation, null);
    }

    public ContentAndOperationType(Type type, Operation operation, Operation globalOperation) {
      this.type = type;
      this.operation = operation;
      this.globalOperation = globalOperation;
    }

    @Override
    public String toString() {
      String s = opString(operation);
      if (globalOperation != null) {
        s = "_" + opString(globalOperation);
      }
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
            Type.ICEBERG_TABLE,
            Put.of(ContentKey.of("iceberg"), IcebergTable.of("/iceberg/table", 42, 42, 42, 42))),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentKey.of("view_dremio"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.DREMIO)
                    .sqlText("SELECT foo FROM dremio")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentKey.of("view_presto"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.PRESTO)
                    .sqlText("SELECT foo FROM presto")
                    .build())),
        new ContentAndOperationType(
            Type.VIEW,
            Put.of(
                ContentKey.of("view_spark"),
                ImmutableSqlView.builder()
                    .dialect(Dialect.SPARK)
                    .sqlText("SELECT foo FROM spark")
                    .build())),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE,
            Put.of(
                ContentKey.of("delta"),
                ImmutableDeltaLakeTable.builder()
                    .addCheckpointLocationHistory("checkpoint")
                    .addMetadataLocationHistory("metadata")
                    .build())),
        new ContentAndOperationType(Type.ICEBERG_TABLE, Delete.of(ContentKey.of("iceberg_delete"))),
        new ContentAndOperationType(
            Type.ICEBERG_TABLE, Unchanged.of(ContentKey.of("iceberg_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentKey.of("view_dremio_delete"))),
        new ContentAndOperationType(
            Type.VIEW, Unchanged.of(ContentKey.of("view_dremio_unchanged"))),
        new ContentAndOperationType(Type.VIEW, Delete.of(ContentKey.of("view_spark_delete"))),
        new ContentAndOperationType(Type.VIEW, Unchanged.of(ContentKey.of("view_spark_unchanged"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Delete.of(ContentKey.of("delta_delete"))),
        new ContentAndOperationType(
            Type.DELTA_LAKE_TABLE, Unchanged.of(ContentKey.of("delta_unchanged"))));
  }

  @Test
  public void verifyAllContentAndOperationTypes() throws BaseNessieClientServerException {
    Branch branch = createBranch("contentAndOperationAll");

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

    List<Entry> entries = getApi().getEntries().refName(branch.getName()).get().getEntries();
    List<Entry> expect =
        contentAndOperationTypes()
            .filter(c -> c.operation instanceof Put)
            .map(c -> Entry.builder().type(c.type).name(c.operation.getKey()).build())
            .collect(Collectors.toList());
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
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
    if (contentAndOperationType.globalOperation != null) {
      commit.operation(contentAndOperationType.globalOperation);
    }
    commit.commit();
    List<Entry> entries = getApi().getEntries().refName(branch.getName()).get().getEntries();
    // Oh, yea - this is weird. The property ContentAndOperationType.operation.key.namespace is null
    // (!!!)
    // here, because somehow JUnit @MethodSource implementation re-constructs the objects returned
    // from
    // the source-method contentAndOperationTypes.
    ContentKey fixedContentKey =
        ContentKey.of(contentAndOperationType.operation.getKey().getElements());
    List<Entry> expect =
        contentAndOperationType.operation instanceof Put
            ? singletonList(
                Entry.builder().name(fixedContentKey).type(contentAndOperationType.type).build())
            : emptyList();
    assertThat(entries).containsExactlyInAnyOrderElementsOf(expect);
  }

  @Test
  public void multiget() throws BaseNessieClientServerException {
    Branch branch = createBranch("foo");
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("b");
    IcebergTable ta = IcebergTable.of("path1", 42, 42, 42, 42);
    IcebergTable tb = IcebergTable.of("path2", 42, 42, 42, 42);
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(a, ta))
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commit();
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(b, tb))
        .commitMeta(CommitMeta.fromMessage("commit 2"))
        .commit();
    Map<ContentKey, Content> response =
        getApi().getContent().key(a).key(b).key(ContentKey.of("noexist")).refName("foo").get();
    assertThat(response)
        .containsEntry(a, ta)
        .containsEntry(b, tb)
        .doesNotContainKey(ContentKey.of("noexist"));
  }
}
