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
import static org.projectnessie.client.NessieClient.AuthType.BASIC;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.rest.NessieNotAuthorizedException;

@QuarkusTest
class TestBasicAuthentication extends BaseClientAuthTest {

  @Test
  void testValidCredentials() {
    withClientCustomizer(
        c -> c.withAuthType(BASIC).withUsername("test_user").withPassword("test_user"));
    assertThat(client().getTreeApi().getAllReferences()).isNotEmpty();
  }

  @Test
  void testValidAdminCredentials() {
    withClientCustomizer(
        c -> c.withAuthType(BASIC).withUsername("admin_user").withPassword("test123"));
    assertThat(client().getTreeApi().getAllReferences()).isNotEmpty();
  }

  @Test
  void testInvalidCredentials() {
    withClientCustomizer(
        c -> c.withAuthType(BASIC).withUsername("test_user").withPassword("bad_password"));
    assertThatThrownBy(() -> client().getTreeApi().getAllReferences())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }
}
