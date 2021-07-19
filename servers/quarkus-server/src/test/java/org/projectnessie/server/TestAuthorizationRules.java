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
package org.projectnessie.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.rest.NessieForbiddenException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableDelete;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.Operations;
import org.projectnessie.server.authz.NessieAuthorizationTestProfile;

@QuarkusTest
@TestProfile(value = NessieAuthorizationTestProfile.class)
class TestAuthorizationRules {

  private static NessieClient client;
  private static TreeApi tree;
  private static ContentsApi contents;

  @BeforeEach
  @TestSecurity(user = "admin_user")
  void setupClient() {
    client = NessieClient.builder().withUri("http://localhost:19121/api/v1").build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();
  }

  @AfterEach
  void closeClient() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @TestSecurity(user = "test_user")
  void testAllOpsWithTestUser(boolean shouldFail)
      throws NessieNotFoundException, NessieConflictException {
    String branchName = "allowedBranchForTestUser";
    ContentsKey key = ContentsKey.of("allowed", "x");
    if (shouldFail) {
      branchName = "disallowedBranchForTestUser";
      key = ContentsKey.of("disallowed", "x");
    }
    String role = "test_user";

    createBranch(Branch.of(branchName, null), role, shouldFail);

    Branch branch =
        shouldFail
            ? Branch.of(branchName, "1234567890123456")
            : (Branch) tree.getReferenceByName(branchName);

    addContent(
        branch,
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(key).contents(IcebergTable.of("foo")).build())
            .commitMeta(CommitMeta.fromMessage("empty message"))
            .build(),
        role,
        shouldFail);

    getCommitLog(branchName, role, shouldFail);
    getEntriesFor(branchName, role, shouldFail);
    readContent(branchName, key, role, shouldFail);

    branch =
        shouldFail
            ? Branch.of(branchName, "1234567890123456")
            : (Branch) tree.getReferenceByName(branchName);

    deleteContent(
        branch,
        ImmutableOperations.builder()
            .addOperations(ImmutableDelete.builder().key(key).build())
            .commitMeta(CommitMeta.fromMessage(""))
            .build(),
        role,
        shouldFail);

    branch =
        shouldFail
            ? Branch.of(branchName, "1234567890123456")
            : (Branch) tree.getReferenceByName(branchName);
    deleteBranch(branch, role, shouldFail);
  }

  @Test
  // test_user2 has all permissions on a Branch, but not permissions on a Key
  @TestSecurity(user = "test_user2")
  void testCanCommitButNotUpdateOrDeleteEntity()
      throws NessieNotFoundException, NessieConflictException {
    String role = "test_user2";
    ContentsKey key = ContentsKey.of("allowed", "some");
    String branchName = "allowedBranchForTestUser2";
    createBranch(Branch.of(branchName, null), role, false);

    final Branch branch = (Branch) tree.getReferenceByName(branchName);
    ImmutableOperations updateOps =
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(key).contents(IcebergTable.of("foo")).build())
            .commitMeta(CommitMeta.fromMessage("empty message"))
            .build();

    assertThatThrownBy(
            () -> tree.commitMultipleOperations(branch.getName(), branch.getHash(), updateOps))
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'UPDATE_ENTITY' is not allowed for Role '%s' on Content '%s'",
                role, updateOps.getOperations().get(0).getKey().toPathString()));

    readContent(branchName, key, role, true);

    final Branch b = (Branch) tree.getReferenceByName(branchName);

    ImmutableOperations deleteOps =
        ImmutableOperations.builder()
            .addOperations(ImmutableDelete.builder().key(key).build())
            .commitMeta(CommitMeta.fromMessage(""))
            .build();

    assertThatThrownBy(() -> tree.commitMultipleOperations(b.getName(), b.getHash(), deleteOps))
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'DELETE_ENTITY' is not allowed for Role '%s' on Content '%s'",
                role, deleteOps.getOperations().get(0).getKey().toPathString()));

    deleteBranch(branch, role, false);
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testAdminUserIsAllowedEverything() throws NessieNotFoundException, NessieConflictException {
    String branchName = "testAdminUserIsAllowedAllBranch";
    String role = "admin_user";
    createBranch(Branch.of(branchName, null), role, false);

    Branch branch = (Branch) tree.getReferenceByName(branchName);

    ContentsKey key = ContentsKey.of("x", "x");
    addContent(
        branch,
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(key).contents(IcebergTable.of("foo")).build())
            .commitMeta(CommitMeta.fromMessage("empty message"))
            .build(),
        role,
        false);

    getEntriesFor(branchName, role, false);
    getCommitLog(branchName, role, false);
    readContent(branchName, key, role, false);

    Branch master = (Branch) tree.getReferenceByName(branchName);
    Branch test = ImmutableBranch.builder().hash(master.getHash()).name("testy").build();
    createBranch(Branch.of(test.getName(), test.getHash()), role, false);
    deleteBranch((Branch) tree.getReferenceByName("testy"), role, false);

    deleteContent(
        master,
        ImmutableOperations.builder()
            .addOperations(ImmutableDelete.builder().key(key).build())
            .commitMeta(CommitMeta.fromMessage(""))
            .build(),
        role,
        false);

    // all required rules are already defined
    tree.commitMultipleOperations(
        branch.getName(),
        branch.getHash(),
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(key).contents(IcebergTable.of("bar")).build())
            .commitMeta(CommitMeta.fromMessage(""))
            .build());
  }

  private static void createBranch(Branch branch, String role, boolean shouldFail)
      throws NessieConflictException, NessieNotFoundException {
    if (shouldFail) {
      assertThatThrownBy(() -> tree.createReference(branch))
          .isInstanceOf(NessieForbiddenException.class)
          .hasMessageContaining(
              String.format(
                  "'CREATE_REFERENCE' is not allowed for Role '%s' on Reference '%s'",
                  role, branch.getName()));
    } else {
      tree.createReference(branch);
    }
  }

  private void deleteBranch(Branch branch, String role, boolean shouldFail)
      throws NessieConflictException, NessieNotFoundException {
    if (shouldFail) {
      assertThatThrownBy(() -> tree.deleteBranch(branch.getName(), branch.getHash()))
          .isInstanceOf(NessieForbiddenException.class)
          .hasMessageContaining(
              String.format(
                  "'DELETE_REFERENCE' is not allowed for Role '%s' on Reference '%s'",
                  role, branch.getName()));
    } else {
      tree.deleteBranch(branch.getName(), branch.getHash());
    }
  }

  private void readContent(String branchName, ContentsKey key, String role, boolean shouldFail)
      throws NessieNotFoundException {
    if (shouldFail) {
      assertThatThrownBy(
              () -> contents.getContents(key, branchName, null).unwrap(IcebergTable.class).get())
          .isInstanceOf(NessieForbiddenException.class)
          .hasMessageContaining(
              String.format(
                  "'READ_ENTITY_VALUE' is not allowed for Role '%s' on Content '%s'",
                  role, key.toPathString()));
    } else {
      assertThat(contents.getContents(key, branchName, null).unwrap(IcebergTable.class).get())
          .isNotNull()
          .isInstanceOf(IcebergTable.class);
    }
  }

  private void getEntriesFor(String branchName, String role, boolean shouldFail)
      throws NessieNotFoundException {
    if (shouldFail) {
      assertThatThrownBy(() -> tree.getEntries(branchName, EntriesParams.empty()).getEntries())
          .isInstanceOf(NessieForbiddenException.class)
          .hasMessageContaining(
              String.format("'READ_ENTRIES' is not allowed for Role '%s' on Reference", role));
    } else {
      List<Entry> tables = tree.getEntries(branchName, EntriesParams.empty()).getEntries();
      assertThat(tables).isNotEmpty();
    }
  }

  private void getCommitLog(String branchName, String role, boolean shouldFail)
      throws NessieNotFoundException {
    if (shouldFail) {
      assertThatThrownBy(
              () -> tree.getCommitLog(branchName, CommitLogParams.empty()).getOperations())
          .isInstanceOf(NessieForbiddenException.class)
          .hasMessageContaining(
              String.format("'LIST_COMMIT_LOG' is not allowed for Role '%s' on Reference", role));
    } else {
      List<CommitMeta> commits =
          tree.getCommitLog(branchName, CommitLogParams.empty()).getOperations();
      assertThat(commits).isNotEmpty();
    }
  }

  private void addContent(Branch branch, Operations operations, String role, boolean shouldFail)
      throws NessieNotFoundException, NessieConflictException {

    if (shouldFail) {
      // adding content requires COMMIT_CHANGE_AGAINST_REFERENCE & UPDATE_ENTITY, but this is
      // difficult to test here, so we're testing this in a separate method
      assertThatThrownBy(
              () -> tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations))
          .isInstanceOf(NessieForbiddenException.class)
          .hasMessageContaining(
              String.format(
                  "'COMMIT_CHANGE_AGAINST_REFERENCE' is not allowed for Role '%s' on Reference '%s'",
                  role, branch.getName()));
    } else {
      tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations);
    }
  }

  private void deleteContent(Branch branch, Operations operations, String role, boolean shouldFail)
      throws NessieConflictException, NessieNotFoundException {
    if (shouldFail) {
      // deleting content requires COMMIT_CHANGE_AGAINST_REFERENCE & DELETE_ENTITY, but this is
      // difficult to test here, so we're testing this in a separate method
      assertThatThrownBy(
              () -> tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations))
          .isInstanceOf(NessieForbiddenException.class)
          .hasMessageContaining(
              String.format(
                  "'COMMIT_CHANGE_AGAINST_REFERENCE' is not allowed for Role '%s' on Reference '%s'",
                  role, branch.getName()));
    } else {
      tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations);
    }
  }
}
