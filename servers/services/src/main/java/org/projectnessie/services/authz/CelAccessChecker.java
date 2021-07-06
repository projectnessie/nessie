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
package org.projectnessie.services.authz;

import static org.projectnessie.model.AuthorizationRuleType.ALLOW_ALL;
import static org.projectnessie.model.AuthorizationRuleType.ASSIGN_REFERENCE_TO_HASH;
import static org.projectnessie.model.AuthorizationRuleType.COMMIT_CHANGE_AGAINST_REFERENCE;
import static org.projectnessie.model.AuthorizationRuleType.CREATE_REFERENCE;
import static org.projectnessie.model.AuthorizationRuleType.DELETE_ENTITY;
import static org.projectnessie.model.AuthorizationRuleType.DELETE_REFERENCE;
import static org.projectnessie.model.AuthorizationRuleType.LIST_OBJECTS;
import static org.projectnessie.model.AuthorizationRuleType.READ_ENTITY_VALUE;
import static org.projectnessie.model.AuthorizationRuleType.READ_OBJECT_CONTENT;
import static org.projectnessie.model.AuthorizationRuleType.UPDATE_ENTITY;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.model.AuthorizationRule;
import org.projectnessie.model.AuthorizationRuleType;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.services.cel.CELUtil;
import org.projectnessie.services.rest.RulesResource;
import org.projectnessie.versioned.NamedRef;

@ApplicationScoped
public class CelAccessChecker implements AccessChecker {

  private final boolean authorizationEnabled;

  // TODO how should we determine whether authz is enabled or not?
  @Inject
  public CelAccessChecker(
      @ConfigProperty(name = "quarkus.oauth2.enabled", defaultValue = "true")
          boolean authorizationEnabled) {
    this.authorizationEnabled = authorizationEnabled;
  }

  @Override
  public void canCreateReference(AccessContext context, NamedRef ref) throws ForbiddenException {
    canPerformOpOnReference(context, ref, CREATE_REFERENCE);
  }

  @Override
  public void canAssignRefToHash(AccessContext context, NamedRef ref) throws ForbiddenException {
    canPerformOpOnReference(context, ref, ASSIGN_REFERENCE_TO_HASH);
  }

  @Override
  public void canDeleteReference(AccessContext context, NamedRef ref) throws ForbiddenException {
    canPerformOpOnReference(context, ref, DELETE_REFERENCE);
  }

  @Override
  public void canReadObjectContent(AccessContext context, NamedRef ref) throws ForbiddenException {
    canPerformOpOnReference(context, ref, READ_OBJECT_CONTENT);
  }

  @Override
  public void canListObjects(AccessContext context, NamedRef ref) throws ForbiddenException {
    canPerformOpOnReference(context, ref, LIST_OBJECTS);
  }

  @Override
  public void canCommitChangeAgainstReference(AccessContext context, NamedRef ref)
      throws ForbiddenException {
    canPerformOpOnReference(context, ref, COMMIT_CHANGE_AGAINST_REFERENCE);
  }

  @Override
  public void canReadEntityValue(AccessContext context, NamedRef ref, ContentsKey key)
      throws ForbiddenException {
    canPerformOpOnPath(context, key, READ_ENTITY_VALUE);
  }

  @Override
  public void canUpdateEntity(AccessContext context, NamedRef ref, ContentsKey key)
      throws ForbiddenException {
    canPerformOpOnPath(context, key, UPDATE_ENTITY);
  }

  @Override
  public void canDeleteEntity(AccessContext context, NamedRef ref, ContentsKey key)
      throws ForbiddenException {
    canPerformOpOnPath(context, key, DELETE_ENTITY);
  }

  private void canPerformOpOnReference(
      AccessContext context, NamedRef ref, AuthorizationRuleType type) {
    if (!authorizationEnabled) {
      return;
    }
    boolean allowed =
        RulesResource.rulesByType.containsKey(ALLOW_ALL)
            || RulesResource.rulesByType.getOrDefault(type, Collections.emptySet()).stream()
                .filter(AuthorizationRule::isReferenceRule)
                .filter(r -> isRoleAllowed(context, r))
                .anyMatch(
                    r -> {
                      try {
                        return CELUtil.SCRIPT_HOST
                            .buildScript(r.ruleExpression())
                            .withContainer(CELUtil.CONTAINER)
                            .withDeclarations(CELUtil.AUTHORIZATION_RULE_REF_DECLARATIONS)
                            .build()
                            .execute(Boolean.class, ImmutableMap.of("ref", ref.getName()));
                      } catch (ScriptException e) {
                        throw new ForbiddenException(
                            String.format(
                                "Failed to compile query expression '%s' due to: %s",
                                r.ruleExpression(), e.getMessage()));
                      }
                    });
    if (!allowed) {
      throw new ForbiddenException(
          String.format(
              "'%s' is not allowed for Role '%s' on Reference '%s'",
              type, getRoleName(context), ref.getName()));
    }
  }

  private String getRoleName(AccessContext context) {
    return null != context.user() ? context.user().getName() : "";
  }

  private void canPerformOpOnPath(
      AccessContext context, ContentsKey contentsKey, AuthorizationRuleType type) {
    if (!authorizationEnabled) {
      return;
    }
    boolean allowed =
        RulesResource.rulesByType.containsKey(ALLOW_ALL)
            || RulesResource.rulesByType.getOrDefault(type, Collections.emptySet()).stream()
                .filter(AuthorizationRule::isPathRule)
                .filter(r -> isRoleAllowed(context, r))
                .anyMatch(
                    r -> {
                      try {
                        return CELUtil.SCRIPT_HOST
                            .buildScript(r.ruleExpression())
                            .withContainer(CELUtil.CONTAINER)
                            .withDeclarations(CELUtil.AUTHORIZATION_RULE_PATH_DECLARATIONS)
                            .build()
                            .execute(
                                Boolean.class, ImmutableMap.of("path", contentsKey.toPathString()));
                      } catch (ScriptException e) {
                        throw new ForbiddenException(
                            String.format(
                                "Failed to compile query expression '%s' due to: %s",
                                r.ruleExpression(), e.getMessage()));
                      }
                    });
    if (!allowed) {
      throw new ForbiddenException(
          String.format(
              "'%s' is not allowed for Role '%s' on Content '%s'",
              type, getRoleName(context), contentsKey.toPathString()));
    }
  }

  private boolean isRoleAllowed(AccessContext context, AuthorizationRule r) {
    try {
      return CELUtil.SCRIPT_HOST
          .buildScript(r.roleExpression())
          .withContainer(CELUtil.CONTAINER)
          .withDeclarations(CELUtil.ROLE_DECLARATION)
          .build()
          .execute(Boolean.class, ImmutableMap.of("role", getRoleName(context)));
    } catch (ScriptException e) {
      throw new ForbiddenException(
          String.format(
              "Failed to compile query expression '%s' due to: %s",
              r.ruleExpression(), e.getMessage()));
    }
  }
}
