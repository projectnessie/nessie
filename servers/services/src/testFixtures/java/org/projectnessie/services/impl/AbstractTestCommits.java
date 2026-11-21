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

import static org.assertj.core.api.Assertions.entry;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Conflict.ConflictType.PAYLOAD_DIFFERS;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;
import static org.projectnessie.model.FetchOption.ALL;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;

public abstract class AbstractTestCommits extends BaseTestServiceImpl {

  @Test
  public void renameTable() throws Exception {
    Branch main = createBranch("renameTable");

    ContentKey oldName = ContentKey.of("old_table_name");
    ContentKey newName = ContentKey.of("new_table_name");

    IcebergTable tableNoId = IcebergTable.of("meta", 1, 2, 3, 4);

    CommitResponse commit = commit(main, fromMessage("setup"), Put.of(oldName, tableNoId));
    String cid = commit.toAddedContentsMap().get(oldName);
    IcebergTable table = IcebergTable.builder().from(tableNoId).id(cid).build();

    soft.assertThat(contents(commit.getTargetBranch(), oldName, newName))
        .containsExactly(entry(oldName, table));

    soft.assertThatCode(() ->
            commit(
                commit.getTargetBranch(),
                fromMessage("setup"),
                Delete.of(oldName),
                Put.of(newName, table)))
        .doesNotThrowAnyException();

    soft.assertThat(contents(main.getName(), null, oldName, newName))
        .containsExactly(entry(newName, table));
  }

  @Test
  public void replaceTableWithView() throws Exception {
    Branch branch = createBranch("replaceTableWithView");
    ContentKey key = ContentKey.of("table");
    IcebergTable table = IcebergTable.of("table-metadata", 1, 2, 3, 4);
    CommitResponse tableCommit = commit(branch, fromMessage("add table"), Put.of(key, table));
    IcebergTable storedTable = table.withId(tableCommit.toAddedContentsMap().get(key));

    IcebergView view = IcebergView.of("view-metadata", 1, 2);
    CommitResponse viewCommit =
        commit(
            tableCommit.getTargetBranch(),
            fromMessage("replace table"),
            Delete.of(key),
            Put.of(key, view));
    IcebergView storedView = view.withId(viewCommit.toAddedContentsMap().get(key));

    soft.assertThat(storedView.getId()).isNotNull().isNotEqualTo(storedTable.getId());
    soft.assertThat(content(branch.getName(), null, false, key).getContent()).isEqualTo(storedView);
    soft.assertThat(entries(branch.getName(), null, null, null, true))
        .containsExactly(Entry.entry(key, ICEBERG_VIEW, storedView));
    soft.assertThat(commitLog(branch.getName(), ALL, null))
        .extracting(LogEntry::getOperations)
        .containsExactly(List.of(Put.of(key, storedView)), List.of(Put.of(key, storedTable)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void rejectContentTypeChangeWithSameId(boolean rename) throws Exception {
    Branch branch = createBranch("rejectContentTypeChangeWithSameId");
    ContentKey key = ContentKey.of("table");
    IcebergTable table = IcebergTable.of("table-metadata", 1, 2, 3, 4);
    CommitResponse tableCommit = commit(branch, fromMessage("add table"), Put.of(key, table));
    IcebergTable storedTable = table.withId(tableCommit.toAddedContentsMap().get(key));
    Branch head = tableCommit.getTargetBranch();
    IcebergView view = IcebergView.of(storedTable.getId(), "view-metadata", 1, 2);
    Operation[] operations =
        rename
            ? new Operation[] {Delete.of(key), Put.of(ContentKey.of("renamed"), view)}
            : new Operation[] {Put.of(key, view)};

    soft.assertThatThrownBy(() -> commit(head, fromMessage("change content type"), operations))
        .isInstanceOfSatisfying(
            NessieReferenceConflictException.class,
            e ->
                soft.assertThat(e.getErrorDetails().conflicts())
                    .extracting(Conflict::conflictType)
                    .containsExactly(PAYLOAD_DIFFERS));
    soft.assertThat(getReference(branch.getName())).isEqualTo(head);
    soft.assertThat(entries(branch.getName(), null, null, null, true))
        .containsExactly(Entry.entry(key, ICEBERG_TABLE, storedTable));
  }
}
