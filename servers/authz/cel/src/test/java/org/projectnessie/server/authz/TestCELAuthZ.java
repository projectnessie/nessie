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

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.services.authz.AccessCheckException;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.versioned.BranchName;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCELAuthZ {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void addsViewAllRefsRule() throws ScriptException {
    CompiledAuthorizationRules rules = new CompiledAuthorizationRules(buildConfig());
    soft.assertThat(rules.getRules().keySet())
        .containsExactlyInAnyOrder("foo", "bar", "baz", "contentType", "__ALLOW_VIEWING_REF_ID");

    soft.assertThat(
            rules
                .getRules()
                .get("foo")
                .execute(
                    Boolean.class, Map.of("op", CheckType.VIEW_REFERENCE.name(), "ref", "main")))
        .isFalse();

    Script allowRefScript = rules.getRules().get("__ALLOW_VIEWING_REF_ID");
    soft.assertThat(
            allowRefScript.execute(
                Boolean.class, Map.of("op", CheckType.VIEW_REFERENCE.name(), "ref", "main")))
        .isTrue();
    soft.assertThat(
            allowRefScript.execute(
                Boolean.class, Map.of("op", CheckType.CREATE_REFERENCE.name(), "ref", "main")))
        .isFalse();
  }

  @Test
  void celBatchAccessChecker() {
    Map<String, String> config = buildConfig();

    AtomicReference<String> user = new AtomicReference<>("some-user");
    AtomicReference<Set<String>> roles = new AtomicReference<>(Set.of("some-user"));

    CompiledAuthorizationRules rules = new CompiledAuthorizationRules(config);
    CelBatchAccessChecker batchAccessChecker =
        new CelBatchAccessChecker(
            rules,
            new AccessContext() {
              @Override
              public Principal user() {
                return user::get;
              }

              @Override
              public Set<String> roleIds() {
                return roles.get();
              }
            });

    BranchName main = BranchName.of("main");
    soft.assertThatCode(() -> batchAccessChecker.canViewReference(main).checkAndThrow())
        .doesNotThrowAnyException();
    soft.assertThatThrownBy(() -> batchAccessChecker.canCreateReference(main).checkAndThrow())
        .isInstanceOf(AccessCheckException.class)
        .hasMessage("'CREATE_REFERENCE' is not allowed for role 'some-user' on reference 'main'");

    soft.assertThatThrownBy(
            () -> batchAccessChecker.canCommitChangeAgainstReference(main).checkAndThrow())
        .isInstanceOf(AccessCheckException.class);

    user.set("baz");
    roles.set(Set.of("baz"));
    soft.assertThatCode(
            () -> batchAccessChecker.canCommitChangeAgainstReference(main).checkAndThrow())
        .doesNotThrowAnyException();
    user.set("foo");
    roles.set(Set.of("foo"));
    soft.assertThatThrownBy(
            () -> batchAccessChecker.canCommitChangeAgainstReference(main).checkAndThrow())
        .isInstanceOf(AccessCheckException.class);
    roles.set(Set.of("foo", "bar"));
    soft.assertThatCode(
            () -> batchAccessChecker.canCommitChangeAgainstReference(main).checkAndThrow())
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(CheckType.class)
  void celBatchAccessCheckerEmptyChecks(CheckType type) {
    Map<String, String> config = buildConfig();
    CompiledAuthorizationRules rules = new CompiledAuthorizationRules(config);
    CelBatchAccessChecker batchAccessChecker = new CelBatchAccessChecker(rules, () -> () -> null);
    Check check = Check.builder(type).build();
    if (type == CheckType.VIEW_REFERENCE) {
      soft.assertThatCode(() -> batchAccessChecker.can(check).checkAndThrow())
          .doesNotThrowAnyException();
    } else {
      soft.assertThatThrownBy(() -> batchAccessChecker.can(check).checkAndThrow())
          .isInstanceOf(AccessCheckException.class);
    }
  }

  private static Map<String, String> buildConfig() {
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
}
