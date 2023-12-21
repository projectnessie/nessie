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
package org.projectnessie.server.authz;

import static org.projectnessie.services.authz.Check.CheckType.CREATE_REFERENCE;
import static org.projectnessie.services.authz.Check.CheckType.VIEW_REFERENCE;

import java.util.Collections;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.server.config.QuarkusNessieAuthorizationConfig;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessCheckException;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.services.authz.ServerAccessContext;
import org.projectnessie.versioned.BranchName;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCELAuthZ {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void addsViewAllRefsRule() throws ScriptException {
    CompiledAuthorizationRules rules = new CompiledAuthorizationRules(buildConfig(true));
    soft.assertThat(rules.getRules())
        .hasSize(2)
        .containsKey("foo")
        .containsKey("__ALLOW_VIEWING_REF_ID");

    soft.assertThat(
            rules
                .getRules()
                .get("foo")
                .execute(Boolean.class, Map.of("op", VIEW_REFERENCE.name(), "ref", "main")))
        .isFalse();

    Script allowRefScript = rules.getRules().get("__ALLOW_VIEWING_REF_ID");
    soft.assertThat(
            allowRefScript.execute(
                Boolean.class, Map.of("op", VIEW_REFERENCE.name(), "ref", "main")))
        .isTrue();
    soft.assertThat(
            allowRefScript.execute(
                Boolean.class, Map.of("op", CREATE_REFERENCE.name(), "ref", "main")))
        .isFalse();
  }

  @Test
  void celBatchAccessChecker() {
    QuarkusNessieAuthorizationConfig config = buildConfig(true);
    CompiledAuthorizationRules rules = new CompiledAuthorizationRules(config);
    CelBatchAccessChecker batchAccessChecker =
        new CelBatchAccessChecker(rules, ServerAccessContext.of("meep", () -> "some-user"));

    soft.assertThatCode(
            () -> batchAccessChecker.canViewReference(BranchName.of("main")).checkAndThrow())
        .doesNotThrowAnyException();
    soft.assertThatThrownBy(
            () -> batchAccessChecker.canCreateReference(BranchName.of("main")).checkAndThrow())
        .isInstanceOf(AccessCheckException.class)
        .hasMessage("'CREATE_REFERENCE' is not allowed for role 'some-user' on reference 'main'");
  }

  @ParameterizedTest
  @EnumSource(CheckType.class)
  void celBatchAccessCheckerEmptyChecks(CheckType type) {
    QuarkusNessieAuthorizationConfig config = buildConfig(true);
    CompiledAuthorizationRules rules = new CompiledAuthorizationRules(config);
    CelBatchAccessChecker batchAccessChecker =
        new CelBatchAccessChecker(rules, ServerAccessContext.of("meep", () -> null));
    Check check = Check.builder(type).build();
    if (type == CheckType.VIEW_REFERENCE) {
      soft.assertThatCode(() -> batchAccessChecker.can(check).checkAndThrow())
          .doesNotThrowAnyException();
    } else {
      soft.assertThatThrownBy(() -> batchAccessChecker.can(check).checkAndThrow())
          .isInstanceOf(AccessCheckException.class);
    }
  }

  @Test
  void celAuthorizer() {
    QuarkusNessieAuthorizationConfig configEnabled = buildConfig(true);
    QuarkusNessieAuthorizationConfig configDisabled = buildConfig(false);

    CompiledAuthorizationRules rules = new CompiledAuthorizationRules(configEnabled);

    soft.assertThat(
            new CelAuthorizer(configEnabled, rules)
                .startAccessCheck(ServerAccessContext.of("meep", () -> "some-user")))
        .isInstanceOf(CelBatchAccessChecker.class);
    soft.assertThat(
            new CelAuthorizer(configDisabled, rules)
                .startAccessCheck(ServerAccessContext.of("meep", () -> "some-user")))
        .isSameAs(AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER);
  }

  private static QuarkusNessieAuthorizationConfig buildConfig(boolean enabled) {
    QuarkusNessieAuthorizationConfig config =
        new QuarkusNessieAuthorizationConfig() {
          @Override
          public boolean enabled() {
            return enabled;
          }

          @Override
          public Map<String, String> rules() {
            return Collections.singletonMap("foo", "false");
          }
        };
    return config;
  }
}
