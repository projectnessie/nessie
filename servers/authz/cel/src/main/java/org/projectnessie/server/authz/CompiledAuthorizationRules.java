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

import static org.projectnessie.services.authz.Check.CheckType.VIEW_REFERENCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.relocated.com.google.api.expr.v1alpha1.Decl;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

/**
 * Compiles the CEL authorization rules and provides access to them via {@link
 * CompiledAuthorizationRules#getRules()}.
 */
public class CompiledAuthorizationRules {
  private static final String ALLOW_VIEWING_ALL_REFS_ID = "__ALLOW_VIEWING_REF_ID";
  private static final String ALLOW_VIEWING_ALL_REFS =
      String.format("op=='%s' && ref.matches('.*')", VIEW_REFERENCE);

  private final Map<String, Script> compiledRules;

  public static final String CONTAINER = "org.projectnessie.model";
  public static final String VAR_REF = "ref";
  public static final String VAR_PATH = "path";
  public static final String VAR_ROLE = "role";
  public static final String VAR_ROLES = "roles";
  public static final String VAR_OP = "op";
  public static final String VAR_CONTENT_TYPE = "contentType";

  public static final List<Decl> AUTHORIZATION_RULE_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_REF, Decls.String),
          Decls.newVar(VAR_PATH, Decls.String),
          Decls.newVar(VAR_CONTENT_TYPE, Decls.String),
          Decls.newVar(VAR_ROLE, Decls.String),
          Decls.newVar(VAR_ROLES, Decls.newListType(Decls.String)),
          Decls.newVar(VAR_OP, Decls.String));

  private final ScriptHost scriptHost =
      ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

  public CompiledAuthorizationRules(Map<String, String> rules) {
    this.compiledRules = compileAuthorizationRules(rules);
  }

  /**
   * Compiles all authorization rules and returns them.
   *
   * @return A map of compiled authorization rules
   */
  private Map<String, Script> compileAuthorizationRules(Map<String, String> rules) {
    rules = new HashMap<>(rules);
    // by default we allow viewing all references until there's a user-defined VIEW_REFERENCE rule
    if (rules.entrySet().stream().noneMatch(r -> r.getValue().contains(VIEW_REFERENCE.name()))) {
      rules.put(ALLOW_VIEWING_ALL_REFS_ID, ALLOW_VIEWING_ALL_REFS);
    }
    Map<String, Script> scripts = new HashMap<>();
    rules.forEach(
        (key, value) ->
            scripts.computeIfAbsent(
                key,
                (k) -> {
                  try {
                    return scriptHost
                        .buildScript(value)
                        .withContainer(CONTAINER)
                        .withDeclarations(AUTHORIZATION_RULE_DECLARATIONS)
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
