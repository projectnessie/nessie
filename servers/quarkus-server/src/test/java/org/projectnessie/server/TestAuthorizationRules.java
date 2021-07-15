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
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.RulesApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.rest.NessieForbiddenException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.AuthorizationRule;
import org.projectnessie.model.AuthorizationRuleType;
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

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;
  private RulesApi rules;

  @BeforeEach
  void setupClient() {
    client = NessieClient.builder().withUri("http://localhost:19121/api/v1").build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();
    rules = client.getRulesApi();
  }

  @AfterEach
  void closeClient() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @Test
  @TestSecurity(
      user = "admin_user",
      roles = {"admin", "user"})
  void testRules() throws NessieNotFoundException, NessieConflictException {
    String branchName = "testRulesBranch";
    createBranch(Branch.of(branchName, null));

    Branch branch = (Branch) tree.getReferenceByName(branchName);
    getEntriesFor(branchName);
    ContentsKey key = ContentsKey.of("x", "x");
    addContent(
        branch,
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(key).contents(IcebergTable.of("foo")).build())
            .commitMeta(CommitMeta.fromMessage("empty message"))
            .build());
    readContent(branchName, key);

    Branch master = (Branch) tree.getReferenceByName(branchName);
    Branch test = ImmutableBranch.builder().hash(master.getHash()).name("testy").build();
    createBranch(Branch.of(test.getName(), test.getHash()));
    deleteBranch((Branch) tree.getReferenceByName("testy"));

    deleteContent(
        master,
        ImmutableOperations.builder()
            .addOperations(ImmutableDelete.builder().key(key).build())
            .commitMeta(CommitMeta.fromMessage(""))
            .build());

    // all required rules are already defined
    tree.commitMultipleOperations(
        branch.getName(),
        branch.getHash(),
        ImmutableOperations.builder()
            .addOperations(ImmutablePut.builder().key(key).contents(IcebergTable.of("bar")).build())
            .commitMeta(CommitMeta.fromMessage(""))
            .build());
  }

  private void createBranch(Branch branch) throws NessieConflictException, NessieNotFoundException {
    assertThatThrownBy(() -> tree.createReference(branch))
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining("'CREATE_REFERENCE' is not allowed for Role 'admin_user'");

    rules.addRule(
        AuthorizationRule.of(
            "allow_branch_creation_" + branch.getName(),
            AuthorizationRuleType.CREATE_REFERENCE,
            String.format("ref=='%s'", branch.getName()),
            "role=='admin_user'"));
    tree.createReference(branch);
  }

  private void deleteBranch(Branch branch) throws NessieConflictException, NessieNotFoundException {
    assertThatThrownBy(() -> tree.deleteBranch(branch.getName(), branch.getHash()))
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            "'DELETE_REFERENCE' is not allowed for Role 'admin_user' on Reference");

    rules.addRule(
        AuthorizationRule.of(
            "allow_branch_deletion_" + branch.getName(),
            AuthorizationRuleType.DELETE_REFERENCE,
            String.format("ref=='%s'", branch.getName()),
            "role=='admin_user'"));
    tree.deleteBranch(branch.getName(), branch.getHash());
  }

  private void readContent(String branchName, ContentsKey key)
      throws NessieNotFoundException, NessieConflictException {
    assertThatThrownBy(
            () -> contents.getContents(key, branchName, null).unwrap(IcebergTable.class).get())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'READ_ENTITY_VALUE' is not allowed for Role 'admin_user' on Content '%s'",
                key.toPathString()));

    rules.addRule(
        AuthorizationRule.of(
            "allow_reading_contents_for_" + branchName,
            AuthorizationRuleType.READ_ENTITY_VALUE,
            String.format("path=='%s'", key.toPathString()),
            "role=='admin_user'"));
    assertThat(contents.getContents(key, branchName, null).unwrap(IcebergTable.class).get())
        .isNotNull()
        .isInstanceOf(IcebergTable.class);
  }

  private void getEntriesFor(String branchName)
      throws NessieNotFoundException, NessieConflictException {
    assertThatThrownBy(() -> tree.getEntries(branchName, EntriesParams.empty()).getEntries())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining("'READ_OBJECT_CONTENT' is not allowed for Role 'admin_user'");

    rules.addRule(
        AuthorizationRule.of(
            "allow_reading_objects_" + branchName,
            AuthorizationRuleType.READ_OBJECT_CONTENT,
            String.format("ref=='%s'", branchName),
            "role=='admin_user'"));

    List<Entry> tables = tree.getEntries(branchName, EntriesParams.empty()).getEntries();
    assertThat(tables).isEmpty();
  }

  private void addContent(Branch branch, Operations operations)
      throws NessieNotFoundException, NessieConflictException {

    assertThatThrownBy(
            () -> tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations))
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            "'COMMIT_CHANGE_AGAINST_REFERENCE' is not allowed for Role 'admin_user' on Reference");

    rules.addRule(
        AuthorizationRule.of(
            "allow_committing_objects_against_" + branch.getName(),
            AuthorizationRuleType.COMMIT_CHANGE_AGAINST_REFERENCE,
            String.format("ref=='%s'", branch.getName()),
            "role=='admin_user'"));

    assertThatThrownBy(
            () -> tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations))
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'UPDATE_ENTITY' is not allowed for Role 'admin_user' on Content '%s'",
                operations.getOperations().get(0).getKey().toPathString()));

    operations
        .getOperations()
        .forEach(
            op -> {
              try {
                rules.addRule(
                    AuthorizationRule.of(
                        "allow_updating_entity_for_" + op.getKey().toPathString(),
                        AuthorizationRuleType.UPDATE_ENTITY,
                        String.format("path=='%s'", op.getKey().toPathString()),
                        "role=='admin_user'"));
              } catch (NessieConflictException e) {
                throw new RuntimeException(e);
              }
            });

    tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations);
  }

  private void deleteContent(Branch branch, Operations operations)
      throws NessieConflictException, NessieNotFoundException {
    assertThatThrownBy(
            () -> tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations))
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining(
            String.format(
                "'DELETE_ENTITY' is not allowed for Role 'admin_user' on Content '%s'",
                operations.getOperations().get(0).getKey().toPathString()));

    operations
        .getOperations()
        .forEach(
            op -> {
              try {
                rules.addRule(
                    AuthorizationRule.of(
                        "allow_deleting_entity_for_" + op.getKey().toPathString(),
                        AuthorizationRuleType.DELETE_ENTITY,
                        String.format("path=='%s'", op.getKey().toPathString()),
                        "role=='admin_user'"));
              } catch (NessieConflictException e) {
                throw new RuntimeException(e);
              }
            });

    tree.commitMultipleOperations(branch.getName(), branch.getHash(), operations);
  }
}
