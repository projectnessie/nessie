/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.server.authn;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import java.util.Set;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.server.config.QuarkusNessieAuthenticationConfig;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieHttpAuthenticator {
  @InjectSoftAssertions SoftAssertions soft;

  static SecurityIdentity ANONYMOUS_IDENTITY =
      QuarkusSecurityIdentity.builder().setAnonymous(true).build();
  static SecurityIdentity SOME_USER =
      QuarkusSecurityIdentity.builder().setPrincipal(() -> "some-user").build();

  @Test
  public void authentication() {
    NessieHttpAuthenticator.BaseNessieHttpAuthenticator baseAuth =
        new NessieHttpAuthenticator.BaseNessieHttpAuthenticator(
            new QuarkusNessieAuthenticationConfig() {
              @Override
              public boolean enabled() {
                return false; // not evaluated
              }

              @Override
              public Set<String> anonymousPaths() {
                return emptySet();
              }
            },
            () -> Uni.createFrom().item(ANONYMOUS_IDENTITY));

    soft.assertThat(baseAuth.anonymous())
        .extracting(TestNessieHttpAuthenticator::extract)
        .isSameAs(ANONYMOUS_IDENTITY);

    soft.assertThat(auth(baseAuth, SOME_USER, "/foo")).isSameAs(SOME_USER);

    soft.assertThatThrownBy(() -> auth(baseAuth, null, "/foo"))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasMessage("Missing or unrecognized credentials");

    NessieHttpAuthenticator.BaseNessieHttpAuthenticator baseAuthWithPathFoo =
        new NessieHttpAuthenticator.BaseNessieHttpAuthenticator(
            new QuarkusNessieAuthenticationConfig() {
              @Override
              public boolean enabled() {
                return false; // not evaluated
              }

              @Override
              public Set<String> anonymousPaths() {
                return singleton("/foo");
              }
            },
            () -> Uni.createFrom().item(ANONYMOUS_IDENTITY));

    soft.assertThat(auth(baseAuthWithPathFoo, null, "/foo")).isSameAs(ANONYMOUS_IDENTITY);

    soft.assertThatThrownBy(() -> auth(baseAuthWithPathFoo, null, "/bar"))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasMessage("Missing or unrecognized credentials");
  }

  private static SecurityIdentity auth(
      NessieHttpAuthenticator.BaseNessieHttpAuthenticator baseAuth,
      SecurityIdentity input,
      String path) {
    return extract(baseAuth.maybeTransform(input, path));
  }

  private static SecurityIdentity extract(Uni<SecurityIdentity> uni) {
    try {
      return uni.subscribeAsCompletionStage().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
