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

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;

/**
 * A reference implementation of the {@link BatchAccessChecker} that performs access checks using
 * CEL expressions.
 */
final class CelBatchAccessChecker extends AbstractBatchAccessChecker {
  private final CompiledAuthorizationRules compiledRules;
  private final AccessContext context;

  CelBatchAccessChecker(CompiledAuthorizationRules compiledRules, AccessContext context) {
    this.compiledRules = compiledRules;
    this.context = context;
  }

  @Override
  public Map<Check, String> check() {
    Map<Check, String> failed = new LinkedHashMap<>();
    getChecks()
        .forEach(
            check -> {
              if (check.type().isContent()) {
                canPerformOpOnPath(check, failed);
              } else if (check.type().isRef()) {
                canPerformOpOnReference(check, failed);
              } else {
                canPerformOp(check, failed);
              }
            });
    return failed;
  }

  private String getRoleName() {
    return null != context.user() ? context.user().getName() : "";
  }

  private void canPerformOp(Check check, Map<Check, String> failed) {
    String roleName = getRoleName();
    ImmutableMap<String, Object> arguments =
        ImmutableMap.of("role", roleName, "op", check.type().name(), "path", "", "ref", "");

    Supplier<String> errorMsgSupplier =
        () -> String.format("'%s' is not allowed for role '%s' ", check.type(), roleName);
    canPerformOp(arguments, check, errorMsgSupplier, failed);
  }

  private void canPerformOpOnReference(Check check, Map<Check, String> failed) {
    String roleName = getRoleName();
    ImmutableMap<String, Object> arguments =
        ImmutableMap.of(
            "ref", check.ref().getName(), "role", roleName, "op", check.type().name(), "path", "");

    Supplier<String> errorMsgSupplier =
        () ->
            String.format(
                "'%s' is not allowed for role '%s' on reference '%s'",
                check.type(), roleName, check.ref().getName());
    canPerformOp(arguments, check, errorMsgSupplier, failed);
  }

  private void canPerformOpOnPath(Check check, Map<Check, String> failed) {
    String roleName = getRoleName();
    ImmutableMap<String, Object> arguments =
        ImmutableMap.of(
            "ref",
            check.ref().getName(),
            "path",
            check.key().toPathString(),
            "role",
            roleName,
            "op",
            check.type().name());

    Supplier<String> errorMsgSupplier =
        () ->
            String.format(
                "'%s' is not allowed for role '%s' on content '%s'",
                check.type(), roleName, check.key().toPathString());

    canPerformOp(arguments, check, errorMsgSupplier, failed);
  }

  private void canPerformOp(
      Map<String, Object> arguments,
      Check check,
      Supplier<String> errorMessageSupplier,
      Map<Check, String> failed) {
    boolean allowed =
        compiledRules.getRules().entrySet().stream()
            .anyMatch(
                entry -> {
                  try {
                    return entry.getValue().execute(Boolean.class, arguments);
                  } catch (ScriptException e) {
                    throw new RuntimeException(
                        String.format(
                            "Failed to execute authorization rule with id '%s' and expression '%s' due to: %s",
                            entry.getKey(), entry.getValue(), e.getMessage()),
                        e);
                  }
                });
    if (!allowed) {
      failed.put(check, errorMessageSupplier.get());
    }
  }
}
