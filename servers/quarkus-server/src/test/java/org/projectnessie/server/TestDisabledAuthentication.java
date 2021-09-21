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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.server.authn.AuthenticationDisabledProfile;

/**
 * This test validates that setting `nessie.server.authentication.enabled=false` allows all requests
 * regardless of their authentication type.
 */
@QuarkusTest
@TestProfile(value = AuthenticationDisabledProfile.class)
public class TestDisabledAuthentication extends BaseClientAuthTest {

  @Test
  void testBasic() {
    withClientCustomizer(
        c -> c.withAuthentication(BasicAuthenticationProvider.create("any_user", "any_password")));
    assertThat(api().getAllReferences().get()).isNotEmpty();
  }

  @Test
  void testBearer() {
    withClientCustomizer(
        c -> c.withAuthentication(BearerAuthenticationProvider.create("any_token")));
    assertThat(api().getAllReferences().get()).isNotEmpty();
  }

  @Test
  void testNone() {
    assertThat(api().getAllReferences().get()).isNotEmpty();
  }
}
