/*
 * Copyright (C) 2024 Dremio
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.server.config.QuarkusNessieAuthorizationConfig;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthorizerType;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCelAuthorizer {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  void celAuthorizer() {
    QuarkusNessieAuthorizationConfig configEnabled = buildConfig(true);
    QuarkusNessieAuthorizationConfig configDisabled = buildConfig(false);

    QuarkusCompiledAuthorizationRules rules = new QuarkusCompiledAuthorizationRules(configEnabled);
    CelAuthorizer celAuthorizer = new CelAuthorizer(rules);

    @SuppressWarnings("unchecked")
    Instance<Authorizer> authorizers = mock(Instance.class);
    @SuppressWarnings("unchecked")
    Instance<Authorizer> celAuthorizerInstance = mock(Instance.class);

    when(celAuthorizerInstance.get()).thenReturn(celAuthorizer);
    when(authorizers.select(new AuthorizerType.Literal("CEL"))).thenReturn(celAuthorizerInstance);
    soft.assertThat(
            new QuarkusAuthorizer(configEnabled, authorizers)
                .startAccessCheck(() -> () -> "some-user"))
        .isInstanceOf(CelBatchAccessChecker.class);

    when(celAuthorizerInstance.get()).thenReturn(celAuthorizer);
    when(authorizers.select(new AuthorizerType.Literal("CEL"))).thenReturn(celAuthorizerInstance);
    soft.assertThat(
            new QuarkusAuthorizer(configDisabled, authorizers)
                .startAccessCheck(() -> () -> "some-user"))
        .isSameAs(AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER);
  }

  private static QuarkusNessieAuthorizationConfig buildConfig(boolean enabled) {
    return new QuarkusNessieAuthorizationConfig() {
      @Override
      public String authorizationType() {
        return "CEL";
      }

      @Override
      public boolean enabled() {
        return enabled;
      }

      @Override
      public Map<String, String> rules() {
        return Map.of(
            "foo",
            "false",
            "bar",
            "'bar' in roles",
            "baz",
            "role=='baz'",
            "contentType",
            "op in ['READ_CONTENT_KEY', 'READ_ENTITY_VALUE', 'CREATE_ENTITY', 'UPDATE_ENTITY', 'DELETE_ENTITY'] "
                + "&& contentType=='foo'");
      }
    };
  }
}
