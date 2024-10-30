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
package org.projectnessie.server.authz;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;
import org.projectnessie.quarkus.config.QuarkusNessieAuthorizationConfig;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

/**
 * A simple {@link QuarkusTestProfile} that enables the Nessie authorization flag, which is fetched
 * by {@link QuarkusNessieAuthorizationConfig#enabled()}.
 */
public class NessieAuthorizationTestProfile extends AuthenticationEnabledProfile {

  public static final Map<String, String> AUTHZ_RULES =
      ImmutableMap.<String, String>builder()
          .put(
              "nessie.server.authorization.rules.allow_all",
              "op in ['VIEW_REFERENCE','CREATE_REFERENCE','DELETE_REFERENCE','VIEW_REFLOG',"
                  + "'LIST_COMMITLOG','READ_ENTRIES','READ_CONTENT_KEY','LIST_COMMIT_LOG','COMMIT_CHANGE_AGAINST_REFERENCE',"
                  + "'ASSIGN_REFERENCE_TO_HASH','CREATE_ENTITY','UPDATE_ENTITY','READ_ENTITY_VALUE','DELETE_ENTITY'] && role=='admin_user'")
          .put(
              "nessie.server.authorization.rules.allow_branch_listing",
              "op=='VIEW_REFERENCE' && role.startsWith('test_user') && ref.matches('.*')")
          .put(
              "nessie.server.authorization.rules.allow_test_user_allowed_branch",
              "op in ['CREATE_REFERENCE','DELETE_REFERENCE','LIST_COMMIT_LOG','READ_ENTRIES','READ_CONTENT_KEY','ASSIGN_REFERENCE_TO_HASH'] "
                  + "&& role.startsWith('test_user') && ref.startsWith('allowedBranch')")
          .put(
              "nessie.server.authorization.rules.allow_commits",
              ("op=='COMMIT_CHANGE_AGAINST_REFERENCE' && role.startsWith('test_user') && ref.startsWith"
                  + "('allowedBranch')"))
          .put(
              "nessie.server.authorization.rules.allow_create_not_read_entity",
              "op in ['VIEW_REFERENCE', 'CREATE_ENTITY', 'UPDATE_ENTITY'] "
                  + "&& role=='test_user2' && path.startsWith('allowed-') && ref.startsWith('allowedBranch')")
          .put(
              "nessie.server.authorization.rules.allow_create_not_delete_entity",
              "op in ['VIEW_REFERENCE', 'DELETE_ENTITY_VALUE', 'CREATE_ENTITY', 'UPDATE_ENTITY'] "
                  + "&& role=='test_user3' && path.startsWith('allowed-') && ref.startsWith('allowedBranch')")
          .put(
              "nessie.server.authorization.rules.allow_no_create_entity",
              "op in ['VIEW_REFERENCE', 'READ_ENTITY_VALUE', 'DELETE_ENTITY'] "
                  + "&& role=='test_user4' && path.startsWith('allowed-') && ref.startsWith('allowedBranch')")
          .put(
              "nessie.server.authorization.rules.allow_commits_without_entity_changes",
              "op=='COMMIT_CHANGE_AGAINST_REFERENCE' && role=='test_user2' && ref.startsWith('allowedBranch')")
          .put(
              "nessie.server.authorization.rules.allow_creation_user1",
              "op=='CREATE_REFERENCE' && role=='user1' && ref.matches('.*')")
          .put(
              "nessie.server.authorization.rules.allow_view_merge_delete_user1",
              "op in ['VIEW_REFERENCE', 'ASSIGN_REFERENCE_TO_HASH', 'COMMIT_CHANGE_AGAINST_REFERENCE', 'DELETE_REFERENCE'] "
                  + "&& role=='user1' && (ref.startsWith('allowedBranch') || ref == 'main')")
          .put(
              "nessie.server.authorization.rules.delete_branch_disallowed",
              "op in ['VIEW_REFERENCE', 'CREATE_REFERENCE'] && role=='delete_branch_disallowed_user' && ref in ['testDeleteBranchDisallowed', 'main']")
          .build();

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .putAll(super.getConfigOverrides())
        .putAll(AUTHZ_RULES)
        .put("nessie.server.authorization.enabled", "true")
        .put("nessie.server.authorization.type", "CEL")
        // Need a dummy URL to satisfy the Quarkus OIDC extension.
        .put("quarkus.oidc.auth-server-url", "http://127.255.0.0:0/auth/realms/unset/")
        .build();
  }
}
