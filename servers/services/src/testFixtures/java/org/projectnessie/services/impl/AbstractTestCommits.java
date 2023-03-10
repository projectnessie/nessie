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

import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
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

    soft.assertThatCode(
            () ->
                commit(
                    commit.getTargetBranch(),
                    fromMessage("setup"),
                    Delete.of(oldName),
                    Put.of(newName, table, table)))
        .doesNotThrowAnyException();

    soft.assertThat(contents(main.getName(), null, oldName, newName))
        .containsExactly(entry(newName, table));
  }
}
