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
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@QuarkusTest
@TestProfile(value = AuthenticationEnabledProfile.class)
class TestBasicAuthentication extends BaseClientAuthTest {

  @Test
  void testValidCredentials() {
    withClientCustomizer(
        c -> c.withAuthentication(BasicAuthenticationProvider.create("test_user", "test_user")));
    assertThat(api().getAllReferences().submit()).isNotEmpty();
  }

  @Test
  void testValidAdminCredentials() {
    withClientCustomizer(
        c -> c.withAuthentication(BasicAuthenticationProvider.create("admin_user", "test123")));
    assertThat(api().getAllReferences().submit()).isNotEmpty();
  }

  @Test
  void testInvalidCredentials() {
    withClientCustomizer(
        c -> c.withAuthentication(BasicAuthenticationProvider.create("test_user", "bad_password")));
    assertThatThrownBy(() -> api().getAllReferences().submit())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }
}
