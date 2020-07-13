/*
 * Copyright (C) 2020 Dremio
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

package com.dremio.nessie.jgit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.nessie.backend.simple.InMemory;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.CommitMeta.Action;
import com.dremio.nessie.model.ImmutableCommitMeta;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.ImmutableTableMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.model.TableMeta;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRepo {

  private static InMemory backend;
  private static JgitBranchController controller;

  @BeforeAll
  public static void init() {
    backend = new InMemory();
    controller = new JgitBranchController(backend);
  }

  @SuppressWarnings("MissingJavadocMethod")
  @BeforeEach
  public void create() throws IOException {
    controller.create("master", null, commitMeta("master",
                                                 "",
                                                 Action.CREATE_BRANCH,
                                                 1));
  }

  @Test
  public void test() throws IOException {
    Branch branch = controller.getBranch("master");
    assertEquals("master", branch.getName());
    List<Branch> branches = controller.getBranches();
    assertEquals(1, branches.size());
    assertEquals(branch.getId(), branches.get(0).getId());
    assertEquals(branch.getName(), branches.get(0).getName());
    List<String> tables = controller.getTables("master", null);
    assertTrue(tables.isEmpty());
    Table table = ImmutableTable.builder()
                                .id("db.table")
                                .namespace("db")
                                .name("table")
                                .metadataLocation("")
                                .build();
    String commit = controller.commit("master",
                                      commitMeta("master", "", Action.COMMIT, 1),
                                      branch.getId(),
                                      table);
    assertNotEquals(branch.getId(), commit);
    branch = controller.getBranch("master");
    assertEquals(branch.getId(), commit);
    tables = controller.getTables("master", null);
    assertEquals(1, tables.size());
    tables = controller.getTables("master", "db");
    assertEquals(1, tables.size());
    Table newTable = controller.getTable("master", "db.table", false);
    assertEquals(tables.get(0), newTable.getId());
    assertEquals(table.getId(), newTable.getId());
    assertEquals(table.getNamespace(), newTable.getNamespace());
    assertEquals(table.getName(), newTable.getName());
    Table table1 = ImmutableTable.builder()
                                 .id("dbx.table")
                                 .namespace("dbx")
                                 .name("table")
                                 .metadataLocation("")
                                 .build();
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 1),
                      branch.getId(),
                      table1);
    tables = controller.getTables("master", null);
    assertEquals(2, tables.size());
    tables = controller.getTables("master", "db");
    assertEquals(1, tables.size());
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 2),
                      commit,
                      ImmutableTable.copyOf(table1).withIsDeleted(true),
                      ImmutableTable.copyOf(table).withIsDeleted(true));
    tables = controller.getTables("master", null);
    assertTrue(tables.isEmpty());
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  public void testMerge() throws IOException {
    Branch branch = controller.create("test",
                                      "master",
                                      commitMeta("test", "", Action.CREATE_BRANCH, 1));
    List<Branch> branches = controller.getBranches();
    assertEquals(2, branches.size());
    Table table = ImmutableTable.builder()
                                .id("db.table")
                                .namespace("db")
                                .name("table")
                                .metadataLocation("")
                                .build();
    String commit = controller.commit("test",
                                      commitMeta("test", "", Action.COMMIT, 1),
                                      branch.getId(),
                                      table);
    assertTrue(controller.getTables("master", null).isEmpty());
    assertEquals(1, controller.getTables("test", null).size());
    Table newTable = controller.getTable("test", "db.table", false);
    assertEquals(table.getId(), newTable.getId());
    assertEquals(table.getNamespace(), newTable.getNamespace());
    assertEquals(table.getName(), newTable.getName());
    String finalCommit = commit;
    assertThrows(IllegalStateException.class,
        () -> controller.promote("master",
                                 "test",
                                 finalCommit,
                                 commitMeta("master", "", Action.MERGE, 1),
                                 false,
                                 false,
                                 null));
    commit = controller.getBranch("master").getId();
    commit = controller.promote("master",
                                "test",
                                commit,
                                commitMeta("master", "", Action.MERGE, 1),
                                false,
                                false,
                                null);
    assertEquals(1, controller.getTables("master", null).size());
    newTable = controller.getTable("master", "db.table", false);
    assertEquals(table.getId(), newTable.getId());
    assertEquals(table.getNamespace(), newTable.getNamespace());
    assertEquals(table.getName(), newTable.getName());
    controller.deleteBranch("test", commit, commitMetaDelete("test"));
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 2),
                      commit,
                      ImmutableTable.copyOf(table).withIsDeleted(true));
    branches = controller.getBranches();
    assertEquals(1, branches.size());
  }

  @Test
  public void testForceMerge() throws IOException {
    Branch branch = controller.create("test",
                                      "master",
                                      commitMeta("test", "", Action.CREATE_BRANCH, 1));
    List<Branch> branches = controller.getBranches();
    assertEquals(2, branches.size());
    Table table = ImmutableTable.builder()
                                .id("db.table")
                                .namespace("db")
                                .name("table")
                                .metadataLocation("")
                                .build();
    String commit = controller.commit("test",
                                      commitMeta("test", "", Action.COMMIT, 1),
                                      branch.getId(),
                                      table);
    Table table1 = ImmutableTable.builder()
                                 .id("dbx.table")
                                 .namespace("dbx")
                                 .name("table")
                                 .metadataLocation("")
                                 .build();
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 1),
                      branch.getId(),
                      table1);
    assertEquals(1, controller.getTables("master", null).size());
    assertEquals(1, controller.getTables("test", null).size());
    String finalCommit = commit;
    assertThrows(IllegalStateException.class, () -> controller.promote("master",
                                                                       "test",
                                                                       finalCommit,
                                                                       commitMeta("master",
                                                                                  "",
                                                                                  Action.MERGE,
                                                                                  1),
                                                                       false,
                                                                       false,
                                                                       null));
    commit = controller.getBranch("master").getId();
    commit = controller.promote("master",
                                "test",
                                commit,
                                commitMeta("master", "", Action.MERGE, 1),
                                true,
                                false,
                                null);
    assertEquals(controller.getTables("master", null),
                 controller.getTables("test", null));
    controller.deleteBranch("test", commit, commitMetaDelete("test"));
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 2),
                      commit,
                      ImmutableTable.copyOf(table).withIsDeleted(true));
    branches = controller.getBranches();
    assertEquals(1, branches.size());
    List<String> tables = controller.getTables("master", null);
    assertTrue(tables.isEmpty());
  }

  @Test
  public void testCherryPick() throws IOException {
    Branch branch = controller.create("test",
                                      "master",
                                      commitMeta("test", "", Action.CREATE_BRANCH, 1));
    List<Branch> branches = controller.getBranches();
    assertEquals(2, branches.size());
    Table table = ImmutableTable.builder()
                                .id("db.table")
                                .namespace("db")
                                .name("table")
                                .metadataLocation("")
                                .build();
    String commit = controller.commit("test",
                                      commitMeta("test", "", Action.COMMIT, 1),
                                      branch.getId(),
                                      table);
    Table table1 = ImmutableTable.builder()
                                 .id("dbx.table")
                                 .namespace("dbx")
                                 .name("table")
                                 .metadataLocation("")
                                 .build();
    commit = controller.commit("master",
                               commitMeta("master", "", Action.COMMIT, 1),
                               branch.getId(),
                               table1);
    assertEquals(1, controller.getTables("master", null).size());
    assertEquals(1, controller.getTables("test", null).size());
    commit = controller.promote("master",
                                "test",
                                commit,
                                commitMeta("master", "", Action.MERGE, 1),
                                false,
                                true,
                                null);
    assertEquals(2, controller.getTables("master", null).size());
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 2),
                      commit,
                      ImmutableTable.copyOf(table1).withIsDeleted(true),
                      ImmutableTable.copyOf(table).withIsDeleted(true));
    List<String> tables = controller.getTables("master", null);
    assertTrue(tables.isEmpty());
    commit = controller.getBranch("test").getId();
    controller.deleteBranch("test", commit, commitMetaDelete("test"));
    branches = controller.getBranches();
    assertEquals(1, branches.size());
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  public void testMetadata() throws IOException {
    Branch branch = controller.getBranch("master");
    TableMeta tableMeta = ImmutableTableMeta.builder()
                                            .schema("x")
                                            .sourceId("y")
                                            .build();
    Table table = ImmutableTable.builder()
                                .id("db.table")
                                .namespace("db")
                                .name("table")
                                .metadataLocation("")
                                .metadata(tableMeta)
                                .build();
    String commit = controller.commit("master",
                                      commitMeta("master", "", Action.COMMIT, 1),
                                      branch.getId(),
                                      table);
    assertEquals(1, controller.getTables("master", null).size());
    table = controller.getTable("master", "db.table", true);
    assertNotNull(table.getMetadata());
    assertEquals(tableMeta, table.getMetadata());
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 1),
                      commit,
                      ImmutableTable.copyOf(table).withIsDeleted(true));
    List<String> tables = controller.getTables("master", null);
    assertTrue(tables.isEmpty());
  }

  @Test
  public void testConflict() throws IOException {
    Branch branch = controller.getBranch("master");
    Table table = ImmutableTable.builder()
                                .id("db.table")
                                .namespace("db")
                                .name("table")
                                .metadataLocation("")
                                .build();
    String commit = controller.commit("master",
                                      commitMeta("master", "", Action.COMMIT, 1),
                                      branch.getId(),
                                      table);
    assertThrows(IllegalStateException.class,
        () -> controller.commit("master",
                                commitMeta("master", "", Action.COMMIT, 1),
                                branch.getId(),
                                ImmutableTable.copyOf(table).withMetadataLocation("x")));
    Table table1 = ImmutableTable.builder()
                                 .id("dbx.table")
                                 .namespace("dbx")
                                 .name("table")
                                 .metadataLocation("")
                                 .build();
    commit = controller.commit("master",
                               commitMeta("master", "", Action.COMMIT, 1),
                               branch.getId(),
                               table1
    );
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 1),
                      commit,
                      ImmutableTable.copyOf(table).withMetadataLocation("x")
    );
    assertEquals(2, controller.getTables("master", null).size());
    controller.commit("master",
                      commitMeta("master", "", Action.COMMIT, 2),
                      commit,
                      ImmutableTable.copyOf(table1).withIsDeleted(true),
                      ImmutableTable.copyOf(table).withIsDeleted(true));
    List<String> tables = controller.getTables("master", null);
    assertTrue(tables.isEmpty());
  }

  @Test
  public void testMultipleControllers() throws IOException {
    JgitBranchController controller2 = new JgitBranchController(backend);
    Branch branch = controller.getBranch("master");
    Branch branch2 = controller2.getBranch("master");
    Table table = ImmutableTable.builder()
                                .id("db.table")
                                .namespace("db")
                                .name("table")
                                .metadataLocation("")
                                .build();
    String commit = controller.commit("master",
                                      commitMeta("master", "", Action.COMMIT, 1),
                                      branch.getId(),
                                      table);
    assertThrows(IllegalStateException.class,
        () -> controller2.commit("master",
                                 commitMeta("master", "", Action.COMMIT, 1),
                                 branch2.getId(),
                                 ImmutableTable.copyOf(table).withMetadataLocation("xx")));
    commit = controller2.getBranch("master").getId();
    commit = controller2.commit("master",
                       commitMeta("master", "", Action.COMMIT, 1),
                       commit,
                       ImmutableTable.copyOf(table).withMetadataLocation("xx"));
    controller2.commit("master",
                      commitMeta("master", "", Action.COMMIT, 2),
                      commit,
                      ImmutableTable.copyOf(table).withIsDeleted(true));
    controller.getBranch("master");
    List<String> tables = controller.getTables("master", null);
    assertTrue(tables.isEmpty());
  }

  @SuppressWarnings("MissingJavadocMethod")
  @AfterEach
  public void empty() throws IOException {
    assertEquals(1, controller.getBranches().size());
    assertEquals("master", controller.getBranches().get(0).getName());
    assertTrue(controller.getTables("master", null).isEmpty());
    backend.close();
    controller.getBranch("master");
  }

  @AfterAll
  public static void close() {
    backend.close();
  }

  private static CommitMeta commitMetaDelete(String branch) {
    return commitMeta(branch, "", Action.DELETE_BRANCH, 1);
  }

  private static CommitMeta commitMeta(String branch, String comment, Action action, int changes) {
    return ImmutableCommitMeta.builder()
                              .action(action)
                              .branch(branch)
                              .changes(changes)
                              .comment(comment)
                              .commiter("test")
                              .email("test@test.org")
                              .build();
  }
}
