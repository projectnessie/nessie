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
import java.util.UUID;
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

@QuarkusTest
@TestProfile(value = NessieAuthorizationTestProfile.class)
class TestAuthorizationRules extends BaseClientAuthTest {

  @Test
  @TestSecurity(user = "admin_user")
  void testCreateBranchAdmin() throws BaseNessieClientServerException {
    assertThat(
            api()
                .createReference()
                .reference(Branch.of("testAdminUserIsAllowedAllBranch", null))
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
                .reference(Branch.of("allowedBranchForTestUser", null))
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
                    .reference(Branch.of("disallowedBranchForTestUser", null))
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
        api().createReference().reference(Branch.of("testDeleteBranchAdmin", null)).create();
    api().deleteBranch().branch((Branch) branch).delete();
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  void testDeleteBranchDisallowed() {
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
                "disallowed_user", "testDeleteBranchDisallowed"));
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
                    .hash("11223344556677")
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
        api().createReference().reference(Branch.of("testReadEntityAllowed", null)).create();
    ContentKey key = ContentKey.of("testKey");
    addContent((Branch) ref, Put.of(key, IcebergTable.of("test", 1, 2, 3, 4)));
    assertThat(api().getContent().refName(ref.getName()).key(key).get().get(key)).isNotNull();
  }

  @Test
  @TestSecurity(user = "admin_user")
  @NessieApiVersions(versions = NessieApiVersion.V1) // Reflog is not supported in API v2
  void testRefLogAllowed() throws Exception {
    //noinspection deprecation
    assertThat(api().getRefLog().stream()).isNotNull();
  }

  @Test
  @TestSecurity(user = "disallowed_user")
  @NessieApiVersions(versions = NessieApiVersion.V1) // Reflog is not supported in API v2
  void testRefLogDisallowed() {
    //noinspection deprecation
    assertThatThrownBy(() -> api().getRefLog().stream())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format("'VIEW_REFLOG' is not allowed for role '%s'", "disallowed_user"));
  }

  @Test
  // test_user2 has all permissions on a Branch, but not permissions on a Key
  @TestSecurity(user = "test_user2")
  void testCanCommitButNotUpdateOrDeleteEntity() throws BaseNessieClientServerException {
    String role = "test_user2";
    ContentKey key = ContentKey.of("allowed", "some");
    String branchName = "allowedBranchForTestUser2";
    createBranch(Branch.of(branchName, null));

    final Branch branch = retrieveBranch(branchName);

    assertThatThrownBy(
            () ->
                api()
                    .commitMultipleOperations()
                    .branch(branch)
                    .commitMeta(CommitMeta.fromMessage("add stuff"))
                    .operation(Put.of(key, IcebergTable.of("foo", 42, 42, 42, 42, "cid-foo")))
                    .commit())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'UPDATE_ENTITY' is not allowed for role '%s' on content '%s'",
                role, key.toPathString()));

    assertThatThrownBy(() -> api().getContent().refName(branchName).key(key).get())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'READ_ENTITY_VALUE' is not allowed for role '%s' on content '%s'",
                role, key.toPathString()));

    final Branch b = retrieveBranch(branchName);

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
  @TestSecurity(user = "admin_user")
  void testCanReadTargetBranchDuringAssign() throws BaseNessieClientServerException {
    String branchName = "adminCanReadWhenAssigning";
    String targetBranchName = "targetBranchForAssign";
    createBranch(Branch.of(branchName, null));
    Branch branch = retrieveBranch(branchName);

    createBranch(Branch.of(targetBranchName, null));
    Branch targetBranch = retrieveBranch(targetBranchName);

    addContent(
        targetBranch,
        Put.of(
            ContentKey.of("allowed", "x"),
            IcebergTable.of("foo", 42, 42, 42, 42, UUID.randomUUID().toString())));
    targetBranch = retrieveBranch(targetBranchName);

    api().assignBranch().branch(branch).assignTo(targetBranch).assign();
    branch = retrieveBranch(branchName);
    assertThat(branch.getHash()).isEqualTo(targetBranch.getHash());
  }

  @Test
  @TestSecurity(user = "admin_user")
  void testCanReadTargetBranchDuringMerge() throws BaseNessieClientServerException {
    String branchName = "adminCanReadWhenMerging";
    String targetBranchName = "targetBranchForMerge";
    createBranch(Branch.of(branchName, null));
    Branch branch = retrieveBranch(branchName);

    createBranch(Branch.of(targetBranchName, null));
    Branch targetBranch = retrieveBranch(targetBranchName);

    addContent(
        branch,
        Put.of(
            ContentKey.of("allowed", "x"),
            IcebergTable.of("foo", 42, 42, 42, 42, UUID.randomUUID().toString())));
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
    createBranch(Branch.of(branchName, null));
    Branch branch = retrieveBranch(branchName);

    createBranch(Branch.of(targetBranchName, null));
    Branch targetBranch = retrieveBranch(targetBranchName);

    addContent(
        branch,
        Put.of(
            ContentKey.of("allowed", "x"),
            IcebergTable.of("foo", 42, 42, 42, 42, UUID.randomUUID().toString())));
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
    createBranch(Branch.of(branchName, null));
    String disallowedBranch = "disallowedBranchForUser1";
    createBranch(Branch.of(disallowedBranch, null));

    final Branch branch = retrieveBranch(branchName);

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

  private void createBranch(Branch branch) throws BaseNessieClientServerException {
    api().createReference().sourceRefName("main").reference(branch).create();
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
