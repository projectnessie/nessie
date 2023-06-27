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

import io.smallrye.jwt.build.Jwt;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.client.rest.NessieNotAuthorizedException;

@SuppressWarnings("resource") // api() returns an AutoCloseable
public abstract class AbstractBearerAuthentication extends BaseClientAuthTest {

  protected abstract String getValidJwtToken();

  private String getInvalidJwtToken() {
    return Jwt.preferredUserName("expired")
        .issuer("https://server.example.com")
        .expiresAt(0)
        .sign();
  }

  @Test
  void testValidJwtToken() throws Exception {
    withClientCustomizer(
        b -> b.withAuthentication(BearerAuthenticationProvider.create(getValidJwtToken())));
    assertThat(api().getAllReferences().stream()).isNotEmpty();
  }

  @Test
  void testInvalidJwtToken() {
    withClientCustomizer(
        b -> b.withAuthentication(BearerAuthenticationProvider.create(getInvalidJwtToken())));
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }

  @Test
  void testAbsentToken() {
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }
}
