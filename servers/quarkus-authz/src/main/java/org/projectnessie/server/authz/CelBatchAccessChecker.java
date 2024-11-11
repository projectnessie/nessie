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

import static org.projectnessie.services.cel.CELUtil.VAR_ACTIONS;
import static org.projectnessie.services.cel.CELUtil.VAR_API;
import static org.projectnessie.services.cel.CELUtil.VAR_CONTENT_TYPE;
import static org.projectnessie.services.cel.CELUtil.VAR_OP;
import static org.projectnessie.services.cel.CELUtil.VAR_PATH;
import static org.projectnessie.services.cel.CELUtil.VAR_REF;
import static org.projectnessie.services.cel.CELUtil.VAR_ROLE;
import static org.projectnessie.services.cel.CELUtil.VAR_ROLES;

import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;
import org.projectnessie.versioned.NamedRef;

/**
 * A reference implementation of the {@link BatchAccessChecker} that performs access checks using
 * CEL expressions.
 */
final class CelBatchAccessChecker extends AbstractBatchAccessChecker {
  private final CompiledAuthorizationRules compiledRules;
  private final AccessContext context;

  CelBatchAccessChecker(
      CompiledAuthorizationRules compiledRules, AccessContext context, ApiContext apiContext) {
    super(apiContext);
    this.compiledRules = compiledRules;
    this.context = context;
  }

  @Override
  public Map<Check, String> check() {
    Map<Check, String> failed = new LinkedHashMap<>();
    getChecks()
        .forEach(
            check -> {
              if (check.type().isRepositoryConfigType()) {
                canPerformRepositoryConfig(check, failed);
              } else if (check.type().isContent()) {
                canPerformOpOnPath(check, failed);
              } else if (check.type().isRef()) {
                canPerformOpOnReference(check, failed);
              } else {
                canPerformOp(check, failed);
              }
            });
    return failed;
  }

  private String roleName() {
    Principal user = context.user();
    if (user == null) {
      return "";
    }
    String name = user.getName();
    return name != null ? name : "";
  }

  private List<String> roles() {
    // CEL only accepts lists and maps, but not sets
    return List.copyOf(context.roleIds());
  }

  private void canPerformOp(Check check, Map<Check, String> failed) {
    String roleName = roleName();
    Map<String, Object> arguments =
        Map.of(
            VAR_ROLE,
            roleName,
            VAR_ROLES,
            roles(),
            VAR_OP,
            check.type().name(),
            VAR_ACTIONS,
            check.actions(),
            VAR_API,
            getApiContext(),
            VAR_PATH,
            "",
            VAR_REF,
            "",
            VAR_CONTENT_TYPE,
            "");

    Supplier<String> errorMsgSupplier =
        () -> String.format("'%s' is not allowed for role '%s' ", check.type(), roleName);
    canPerformOp(arguments, check, errorMsgSupplier, failed);
  }

  private void canPerformOpOnReference(Check check, Map<Check, String> failed) {
    String role = roleName();
    String op = check.type().name();
    String ref = Optional.ofNullable(check.ref()).map(NamedRef::getName).orElse("");
    Map<String, Object> arguments =
        Map.of("ref", ref, "role", role, "roles", roles(), "op", op, "path", "", "contentType", "");

    Supplier<String> errorMsgSupplier =
        () ->
            String.format(
                "'%s' is not allowed for role '%s' on reference '%s'", check.type(), role, ref);
    canPerformOp(arguments, check, errorMsgSupplier, failed);
  }

  private void canPerformOpOnPath(Check check, Map<Check, String> failed) {
    String role = roleName();
    String op = check.type().name();
    String contentType = Optional.ofNullable(check.contentType()).map(Type::name).orElse("");
    String path = Optional.ofNullable(check.key()).map(ContentKey::toPathString).orElse("");
    String ref = Optional.ofNullable(check.ref()).map(NamedRef::getName).orElse("");
    Map<String, Object> arguments =
        Map.of(
            "ref",
            ref,
            "path",
            path,
            "role",
            role,
            "roles",
            roles(),
            "op",
            op,
            "contentType",
            contentType);

    Supplier<String> errorMsgSupplier =
        () -> String.format("'%s' is not allowed for role '%s' on content '%s'", op, role, path);

    canPerformOp(arguments, check, errorMsgSupplier, failed);
  }

  private void canPerformRepositoryConfig(Check check, Map<Check, String> failed) {
    String op = check.type().name();
    String type =
        Optional.ofNullable(check.repositoryConfigType())
            .map(RepositoryConfig.Type::name)
            .orElse("");

    Map<String, Object> arguments =
        Map.of("ref", "", "path", "", "role", roleName(), "roles", roles(), "op", op, "type", type);

    Supplier<String> errorMsgSupplier =
        () ->
            String.format(
                "'%s' is not allowed for repository config type '%s'", check.type(), type);

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
                            "Failed to execute authorization rule with id '%s' due to: %s",
                            entry.getKey(), e.getMessage()),
                        e);
                  }
                });
    if (!allowed) {
      failed.put(check, errorMessageSupplier.get());
    }
  }
}
