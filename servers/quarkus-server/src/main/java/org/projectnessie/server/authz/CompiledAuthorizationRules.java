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

import static org.projectnessie.server.authz.CelAccessChecker.AuthorizationRuleType.VIEW_REFERENCE;

import com.google.common.collect.ImmutableMap;
import io.quarkus.runtime.Startup;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.server.config.QuarkusNessieAuthorizationConfig;
import org.projectnessie.services.cel.CELUtil;

/**
 * Compiles the authorization rules from {@link QuarkusNessieAuthorizationConfig} at startup and
 * provides access to them via {@link CompiledAuthorizationRules#getRules()}.
 */
@Singleton
@Startup
public class CompiledAuthorizationRules {
  private final QuarkusNessieAuthorizationConfig config;
  private final Map<String, Script> compiledRules;
  private static final String ALLOW_VIEWING_ALL_REFS_ID = "__ALLOW_VIEWING_REF_ID";
  private static final String ALLOW_VIEWING_ALL_REFS =
      String.format("op=='%s' && ref.matches('.*')", VIEW_REFERENCE);

  @Inject
  public CompiledAuthorizationRules(QuarkusNessieAuthorizationConfig config) {
    this.config = config;
    this.compiledRules = compileAuthorizationRules();
  }

  /**
   * Compiles all authorization rules and returns them.
   *
   * @return A map of compiled authorization rules
   */
  private Map<String, Script> compileAuthorizationRules() {
    Map<String, String> rules = new HashMap<>(config.rules());
    // by default we allow viewing all references until there's a user-defined VIEW_REFERENCE rule
    if (rules.entrySet().stream().noneMatch(r -> r.getValue().contains(VIEW_REFERENCE.name()))) {
      rules.put(ALLOW_VIEWING_ALL_REFS_ID, ALLOW_VIEWING_ALL_REFS);
    }
    Map<String, Script> scripts = new HashMap<>();
    rules.forEach(
        (key, value) ->
            scripts.computeIfAbsent(
                value,
                (k) -> {
                  try {
                    return CELUtil.SCRIPT_HOST
                        .buildScript(value)
                        .withContainer(CELUtil.CONTAINER)
                        .withDeclarations(CELUtil.AUTHORIZATION_RULE_DECLARATIONS)
                        .build();
                  } catch (ScriptException e) {
                    throw new RuntimeException(
                        String.format(
                            "Failed to compile authorization rule with id '%s' and expression '%s' due to: %s",
                            key, value, e.getMessage()),
                        e);
                  }
                }));
    return ImmutableMap.copyOf(scripts);
  }

  /**
   * Returns a map of compiled authorization rules.
   *
   * @return A map of compiled authorization rules
   */
  public Map<String, Script> getRules() {
    return compiledRules;
  }
}
