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
import io.quarkus.test.security.TestSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.AuthorizationRule;
import org.projectnessie.model.AuthorizationRuleType;

@QuarkusTest
class TestRulesRestApi {

  private NessieClient client;

  @BeforeEach
  void setupClient() {
    client = NessieClient.builder().withUri("http://localhost:19121/api/v1").build();
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
  void testAddingAndRemovingRules() throws NessieConflictException {
    AuthorizationRule one =
        AuthorizationRule.of(
            "some_rule_1",
            AuthorizationRuleType.CREATE_REFERENCE,
            String.format("ref == '%s'", "testbranch"),
            "role == ''");
    AuthorizationRule two =
        AuthorizationRule.of(
            "some_rule_2",
            AuthorizationRuleType.UPDATE_ENTITY,
            String.format("ref == '%s'", "testbranch"),
            "role == ''");
    client.getRulesApi().addRule(one);
    assertThat(client.getRulesApi().getRules()).isNotEmpty().containsExactly(one);

    assertThatThrownBy(() -> client.getRulesApi().addRule(one))
        .isInstanceOf(NessieConflictException.class)
        .hasMessage("Authorization rule 'some_rule_1' already exists");
    assertThat(client.getRulesApi().getRules()).isNotEmpty().containsExactly(one);

    client.getRulesApi().addRule(two);
    assertThat(client.getRulesApi().getRules()).isNotEmpty().containsExactlyInAnyOrder(one, two);

    client.getRulesApi().deleteRule(one.id());
    assertThat(client.getRulesApi().getRules()).isNotEmpty().containsExactly(two);

    assertThatThrownBy(() -> client.getRulesApi().deleteRule(one.id()))
        .isInstanceOf(NessieConflictException.class)
        .hasMessage("Authorization rule 'some_rule_1' does not exist");
  }

  @Test
  @TestSecurity
  void rulesApiNotAccessibleForNonAdmins() {
    AuthorizationRule rule =
        AuthorizationRule.of(
            "random_rule",
            AuthorizationRuleType.CREATE_REFERENCE,
            String.format("ref == '%s'", "testbranch"),
            "role == ''");
    assertThatThrownBy(() -> client.getRulesApi().addRule(rule))
        .isInstanceOf(NessieNotAuthorizedException.class)
        .hasMessageContaining("Unauthorized (HTTP/401): Unauthorized");

    assertThatThrownBy(() -> client.getRulesApi().deleteRule(rule.id()))
        .isInstanceOf(NessieNotAuthorizedException.class)
        .hasMessageContaining("Unauthorized (HTTP/401): Unauthorized");

    assertThatThrownBy(() -> client.getRulesApi().getRules())
        .isInstanceOf(NessieNotAuthorizedException.class)
        .hasMessageContaining("Unauthorized (HTTP/401): Unauthorized");
  }
}
