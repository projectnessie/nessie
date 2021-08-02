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
import java.security.AccessControlException;
import java.util.Map;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.server.config.QuarkusNessieAuthorizationConfig;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.versioned.NamedRef;

/**
 * A reference implementation of the {@link AccessChecker} that performs access checks using CEL
 * expressions.
 */
@ApplicationScoped
public class CelAccessChecker implements AccessChecker {
  private final QuarkusNessieAuthorizationConfig config;
  private final CompiledAuthorizationRules compiledRules;

  public enum AuthorizationRuleType {
    VIEW_REFERENCE,
    CREATE_REFERENCE,
    DELETE_REFERENCE,
    LIST_COMMIT_LOG,
    READ_ENTRIES,
    ASSIGN_REFERENCE_TO_HASH,
    COMMIT_CHANGE_AGAINST_REFERENCE,
    READ_ENTITY_VALUE,
    UPDATE_ENTITY,
    DELETE_ENTITY;
  }

  @Inject
  public CelAccessChecker(
      QuarkusNessieAuthorizationConfig config, CompiledAuthorizationRules compiledRules) {
    this.config = config;
    this.compiledRules = compiledRules;
  }

  @Override
  public void canViewReference(AccessContext context, NamedRef ref) throws AccessControlException {
    canPerformOpOnReference(context, ref, AuthorizationRuleType.VIEW_REFERENCE);
  }

  @Override
  public void canCreateReference(AccessContext context, NamedRef ref)
      throws AccessControlException {
    canPerformOpOnReference(context, ref, AuthorizationRuleType.CREATE_REFERENCE);
  }

  @Override
  public void canAssignRefToHash(AccessContext context, NamedRef ref)
      throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnReference(context, ref, AuthorizationRuleType.ASSIGN_REFERENCE_TO_HASH);
  }

  @Override
  public void canDeleteReference(AccessContext context, NamedRef ref)
      throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnReference(context, ref, AuthorizationRuleType.DELETE_REFERENCE);
  }

  @Override
  public void canReadEntries(AccessContext context, NamedRef ref) throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnReference(context, ref, AuthorizationRuleType.READ_ENTRIES);
  }

  @Override
  public void canListCommitLog(AccessContext context, NamedRef ref) throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnReference(context, ref, AuthorizationRuleType.LIST_COMMIT_LOG);
  }

  @Override
  public void canCommitChangeAgainstReference(AccessContext context, NamedRef ref)
      throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnReference(context, ref, AuthorizationRuleType.COMMIT_CHANGE_AGAINST_REFERENCE);
  }

  @Override
  public void canReadEntityValue(
      AccessContext context, NamedRef ref, ContentsKey key, String contentsId)
      throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnPath(context, ref, key, AuthorizationRuleType.READ_ENTITY_VALUE);
  }

  @Override
  public void canUpdateEntity(
      AccessContext context, NamedRef ref, ContentsKey key, String contentsId)
      throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnPath(context, ref, key, AuthorizationRuleType.UPDATE_ENTITY);
  }

  @Override
  public void canDeleteEntity(
      AccessContext context, NamedRef ref, ContentsKey key, String contentsId)
      throws AccessControlException {
    canViewReference(context, ref);
    canPerformOpOnPath(context, ref, key, AuthorizationRuleType.DELETE_ENTITY);
  }

  private String getRoleName(AccessContext context) {
    return null != context.user() ? context.user().getName() : "";
  }

  private void canPerformOpOnReference(
      AccessContext context, NamedRef ref, AuthorizationRuleType type) {
    if (!config.enabled()) {
      return;
    }
    String roleName = getRoleName(context);
    ImmutableMap<String, Object> arguments =
        ImmutableMap.of("ref", ref.getName(), "role", roleName, "op", type.name());

    Supplier<String> errorMsgSupplier =
        () ->
            String.format(
                "'%s' is not allowed for role '%s' on reference '%s'",
                type, roleName, ref.getName());
    canPerformOp(arguments, errorMsgSupplier);
  }

  private void canPerformOpOnPath(
      AccessContext context, NamedRef ref, ContentsKey contentsKey, AuthorizationRuleType type) {
    if (!config.enabled()) {
      return;
    }
    String roleName = getRoleName(context);
    ImmutableMap<String, Object> arguments =
        ImmutableMap.of(
            "ref",
            ref.getName(),
            "path",
            contentsKey.toPathString(),
            "role",
            roleName,
            "op",
            type.name());

    Supplier<String> errorMsgSupplier =
        () ->
            String.format(
                "'%s' is not allowed for role '%s' on content '%s'",
                type, roleName, contentsKey.toPathString());

    canPerformOp(arguments, errorMsgSupplier);
  }

  private void canPerformOp(Map<String, Object> arguments, Supplier<String> errorMessageSupplier) {
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
      throw new AccessControlException(errorMessageSupplier.get());
    }
  }
}
