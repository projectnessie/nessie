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
import java.util.Collections;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.server.authz.NessieAuthorizationTestProfile;

@SuppressWarnings("resource") // api() returns an AutoCloseable
@QuarkusTest
@TestProfile(value = NessieAuthorizationTestProfile.class)
class TestAuthorizationRules extends BaseClientAuthTest {

  @Test
  @TestSecurity(user = "admin_user")
  void testCreateBranchAdmin() throws BaseNessieClientServerException {
    assertThat(
            api()
                .createReference()
                .reference(
                    Branch.of(
                        "testAdminUserIsAllowedAllBranch", api().getDefaultBranch().getHash()))
                .create()
                .getHash())
        .isNotNull();
  }

  @Test
  @TestSecurity(user = "test_user")
  void testCreateBranchUserAllowed() throws Exception {
    assertThat(
            api()
                .createReference()
                .reference(
                    Branch.of("allowedBranchForTestUser", api().getDefaultBranch().getHash()))
                .create()
                .getHash())
        .isNotNull();
  }

  @Test
  @TestSecurity(user = "test_user")
  void testCreateBranchUserDisallowed() {
    assertThatThrownBy(
            () ->
                api()
                    .createReference()
                    .reference(
                        Branch.of(
                            "disallowedBranchForTestUser", api().getDefaultBranch().getHash()))
                    .create())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'CREATE_REFERENCE' is not allowed for role '%s' on reference '%s'",
                "test_user", "disallowedBranchForTestUser"));
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testDeleteBranchAdmin() throws BaseNessieClientServerException {
    Reference branch =
        api()
            .createReference()
            .reference(Branch.of("testDeleteBranchAdmin", api().getDefaultBranch().getHash()))
            .create();
    api().deleteBranch().branch((Branch) branch).delete();
  }

  @Test
  @TestSecurity(user = "delete_branch_disallowed_user")
  void testDeleteBranchDisallowed() throws BaseNessieClientServerException {
    Branch main = api().getDefaultBranch();
    api()
        .createReference()
        .reference(Branch.of("testDeleteBranchDisallowed", main.getHash()))
        .sourceRefName(main.getName())
        .create();
    assertThatThrownBy(
            () ->
                api()
                    .deleteBranch()
                    .branchName("testDeleteBranchDisallowed")
                    .hash("11223344556677")
                    .delete())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'DELETE_REFERENCE' is not allowed for role '%s' on reference '%s'",
                "delete_branch_disallowed_user", "testDeleteBranchDisallowed"));
  }

  @Test
  @TestSecurity(user = "test_user")
  void testListReferencesAllowed() throws Exception {
    assertThat(api().getAllReferences().stream()).extracting(Reference::getName).contains("main");
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  void testListReferencesDisallowed() throws Exception {
    assertThat(api().getAllReferences().stream())
        .extracting(Reference::getName)
        .doesNotContain("main");
  }

  @Test
  @TestSecurity(user = "test_user")
  void testGetReferenceAllowed() throws Exception {
    assertThat(api().getReference().refName("main").get().getHash()).isNotNull();
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  void testGetReferenceDisallowed() {
    assertThatThrownBy(() -> api().getReference().refName("main").get())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'VIEW_REFERENCE' is not allowed for role '%s' on reference '%s'",
                "disallowed_user", "main"));
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testGetCommitLogAllowed() throws Exception {
    assertThat(api().getCommitLog().refName("main").stream()).isNotNull();
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  void testGetCommitLogDisallowed() {
    assertThatThrownBy(() -> api().getCommitLog().refName("main").stream())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'LIST_COMMIT_LOG' is not allowed for role '%s' on reference", "disallowed_user"));
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testGetEntriesAllowed() throws Exception {
    assertThat(api().getEntries().refName("main").stream()).isNotNull();
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  void testGetEntriesDisallowed() {
    assertThatThrownBy(() -> api().getEntries().refName("main").stream())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'READ_ENTRIES' is not allowed for role '%s' on reference", "disallowed_user"));
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  void testCommitDisallowed() {
    // Adding content requires COMMIT_CHANGE_AGAINST_REFERENCE & UPDATE_ENTITY, but this is
    // difficult to test here, so we're testing this in a separate method
    assertThatThrownBy(
            api()
                    .commitMultipleOperations()
                    .branchName("main")
                    .hash("2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d")
                    .operation(Delete.of(ContentKey.of("testKey")))
                    .commitMeta(CommitMeta.fromMessage("test commit"))
                ::commit)
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'COMMIT_CHANGE_AGAINST_REFERENCE' is not allowed for role '%s' on reference '%s'",
                "disallowed_user", "main"));
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testReadEntityAllowed() throws Exception {
    Reference ref =
        api()
            .createReference()
            .reference(Branch.of("testReadEntityAllowed", api().getDefaultBranch().getHash()))
            .create();
    ContentKey key = ContentKey.of("testKey");
    addContent((Branch) ref, Put.of(key, IcebergTable.of("test", 1, 2, 3, 4)));
    assertThat(api().getContent().refName(ref.getName()).key(key).get().get(key)).isNotNull();
  }

  @Test
  @TestSecurity(user = "admin_user")
  @NessieApiVersions(versions = NessieApiVersion.V1) // Reflog is not supported in API v2
  @SuppressWarnings("deprecation")
  void testRefLogAllowed() throws Exception {
    assertThat(api().getRefLog().stream()).isNotNull();
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  @NessieApiVersions(versions = NessieApiVersion.V1) // Reflog is not supported in API v2
  @SuppressWarnings("deprecation")
  void testRefLogDisallowed() {
    assertThatThrownBy(() -> api().getRefLog().stream())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format("'VIEW_REFLOG' is not allowed for role '%s'", "disallowed_user"));
  }

  @Test
  // test_user2 has all permissions on a Branch, but no permissions to READ_ENTITY_VALUE
  @TestSecurity(user = "test_user2")
  void testCanCommitButNotReadEntity() throws BaseNessieClientServerException {
    String role = "test_user2";
    ContentKey key = ContentKey.of("allowed-some");
    String branchName = "allowedBranchForTestUser2";
    Branch branch = createBranch(Branch.of(branchName, api().getDefaultBranch().getHash()));

    api()
        .commitMultipleOperations()
        .branch(branch)
        .commitMeta(CommitMeta.fromMessage("add stuff"))
        .operation(Put.of(key, IcebergTable.of("foo", 42, 42, 42, 42)))
        .commit();

    assertThatThrownBy(() -> api().getContent().refName(branchName).key(key).get())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'READ_ENTITY_VALUE' is not allowed for role '%s' on content '%s'",
                role, key.toPathString()));
  }

  @Test
  // test_user3 has all permissions on a Branch, but not permissions to DELETE_ENTITY
  @TestSecurity(user = "test_user3")
  void testCanCommitButNotDeleteEntity() throws BaseNessieClientServerException {
    String role = "test_user3";
    ContentKey key = ContentKey.of("allowed-some");
    Branch branch =
        createBranch(Branch.of("allowedBranchForTestUser3", api().getDefaultBranch().getHash()));

    Branch b =
        api()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("add stuff"))
            .operation(Put.of(key, IcebergTable.of("foo", 42, 42, 42, 42)))
            .commit();

    assertThatThrownBy(
            () ->
                api()
                    .commitMultipleOperations()
                    .branch(b)
                    .commitMeta(CommitMeta.fromMessage("delete stuff"))
                    .operation(Delete.of(key))
                    .commit())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'DELETE_ENTITY' is not allowed for role '%s' on content '%s'",
                role, key.toPathString()));
  }

  @Test
  // test_user3 has all permissions on a Branch, but not permissions to DELETE_ENTITY
  @TestSecurity(user = "test_user4")
  void testCanCommitButNotUpdateEntity() throws BaseNessieClientServerException {
    String role = "test_user4";
    ContentKey key = ContentKey.of("allowed-some");
    Branch branch =
        createBranch(Branch.of("allowedBranchForTestUser4", api().getDefaultBranch().getHash()));

    assertThatThrownBy(
            () ->
                api()
                    .commitMultipleOperations()
                    .branch(branch)
                    .commitMeta(CommitMeta.fromMessage("add stuff"))
                    .operation(Put.of(key, IcebergTable.of("foo", 42, 42, 42, 42)))
                    .commit())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'CREATE_ENTITY' is not allowed for role '%s' on content '%s'",
                role, key.toPathString()));
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testCanReadTargetBranchDuringAssign() throws BaseNessieClientServerException {
    String branchName = "adminCanReadWhenAssigning";
    String targetBranchName = "targetBranchForAssign";
    Branch main = api().getDefaultBranch();
    Branch branch = createBranch(Branch.of(branchName, main.getHash()));

    Branch targetBranch = createBranch(Branch.of(targetBranchName, main.getHash()));

    addContent(
        targetBranch, Put.of(ContentKey.of("allowed-x"), IcebergTable.of("foo", 42, 42, 42, 42)));
    targetBranch = retrieveBranch(targetBranchName);

    api().assignBranch().branch(branch).assignTo(targetBranch).assign();
    branch = retrieveBranch(branchName);
    assertThat(branch.getHash()).isEqualTo(targetBranch.getHash());
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testCanReadTargetBranchDuringMerge() throws BaseNessieClientServerException {

    Branch main = retrieveBranch("main");
    addContent(
        main, Put.of(ContentKey.of("unrelated-key"), IcebergTable.of("unrelated", 11, 12, 13, 14)));
    main = retrieveBranch("main");

    String branchName = "adminCanReadWhenMerging";
    String targetBranchName = "targetBranchForMerge";
    Branch branch = createBranch(Branch.of(branchName, main.getHash()));

    Branch targetBranch = createBranch(Branch.of(targetBranchName, main.getHash()));

    addContent(branch, Put.of(ContentKey.of("allowed-x"), IcebergTable.of("foo", 42, 42, 42, 42)));
    branch = retrieveBranch(branchName);

    api().mergeRefIntoBranch().fromRef(branch).branch(targetBranch).merge();

    targetBranch = retrieveBranch(targetBranch.getName());

    assertThat(api().getCommitLog().reference(targetBranch).stream()).isNotEmpty();
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testCanReadTargetBranchDuringTransplant() throws BaseNessieClientServerException {
    String branchName = "adminCanReadWhenTransplanting";
    String targetBranchName = "targetBranchForTransplant";
    Branch main = api().getDefaultBranch();
    Branch branch = createBranch(Branch.of(branchName, main.getHash()));

    Branch targetBranch = createBranch(Branch.of(targetBranchName, main.getHash()));

    addContent(branch, Put.of(ContentKey.of("allowed-x"), IcebergTable.of("foo", 42, 42, 42, 42)));
    branch = retrieveBranch(branchName);

    api()
        .transplantCommitsIntoBranch()
        .fromRefName(branch.getName())
        .hashesToTransplant(
            api().getCommitLog().reference(branch).stream()
                .map(e -> e.getCommitMeta().getHash())
                .collect(Collectors.toList()))
        .branch(targetBranch)
        .transplant();

    targetBranch = retrieveBranch(targetBranch.getName());

    assertThat(api().getCommitLog().reference(targetBranch).stream()).isNotEmpty();
  }

  @Test
  @TestSecurity(user = "user1")
  void testCannotReadTargetBranch() throws BaseNessieClientServerException {
    String role = "user1";
    String branchName = "allowedBranchForUser1";
    Branch main = api().getDefaultBranch();
    Branch branch = createBranch(Branch.of(branchName, main.getHash()));
    String disallowedBranch = "disallowedBranchForUser1";
    createBranch(Branch.of(disallowedBranch, main.getHash()));

    String errorMessage =
        String.format(
            "'VIEW_REFERENCE' is not allowed for role '%s' on reference '%s'",
            role, disallowedBranch);
    assertThatThrownBy(
            () ->
                api()
                    .assignBranch()
                    .branch(branch)
                    .assignTo(Branch.of(disallowedBranch, branch.getHash()))
                    .assign())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(errorMessage);

    assertThatThrownBy(
            () ->
                api()
                    .mergeRefIntoBranch()
                    .fromRef(branch)
                    .branch(Branch.of(disallowedBranch, branch.getHash()))
                    .merge())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(errorMessage);

    assertThatThrownBy(
            () ->
                api()
                    .transplantCommitsIntoBranch()
                    .hashesToTransplant(Collections.singletonList(branch.getHash()))
                    .fromRefName(branch.getName())
                    .branch(Branch.of(disallowedBranch, branch.getHash()))
                    .transplant())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(errorMessage);
  }

  private Branch retrieveBranch(String branchName) throws NessieNotFoundException {
    return (Branch) api().getReference().refName(branchName).get();
  }

  private Branch createBranch(Branch branch) throws BaseNessieClientServerException {
    return (Branch) api().createReference().sourceRefName("main").reference(branch).create();
  }

  private void addContent(Branch branch, Put put) throws BaseNessieClientServerException {
    CommitMultipleOperationsBuilder commitOp =
        api()
            .commitMultipleOperations()
            .branch(branch)
            .operation(put)
            .commitMeta(CommitMeta.fromMessage("add stuff"));

    commitOp.commit();
  }
}
