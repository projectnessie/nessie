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
package org.projectnessie.services.rest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.security.RolesAllowed;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import org.projectnessie.api.RulesApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.AuthorizationRule;
import org.projectnessie.model.AuthorizationRuleType;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.VersionStore;

@RequestScoped
public class RulesResource extends BaseResource implements RulesApi {
  public static final Map<String, AuthorizationRule> rulesById = new HashMap<>();
  public static final Map<AuthorizationRuleType, Set<AuthorizationRule>> rulesByType =
      new HashMap<>();

  @Context SecurityContext securityContext;

  @Inject
  public RulesResource(
      ServerConfig config,
      MultiTenant multiTenant,
      VersionStore<Contents, CommitMeta, Type> store,
      AccessChecker accessChecker) {
    super(config, multiTenant, store, accessChecker);
  }

  @Override
  protected SecurityContext getSecurityContext() {
    return securityContext;
  }

  @Override
  @RolesAllowed("admin")
  public void addRule(AuthorizationRule rule) throws NessieConflictException {
    if (rulesById.containsKey(rule.id())) {
      throw new NessieConflictException(
          String.format("Authorization rule '%s' already exists", rule.id()));
    }
    // TODO: this will be maintained later in the versionstore
    rulesById.put(rule.id(), rule);
    rulesByType.computeIfAbsent(rule.type(), s -> new HashSet<>()).add(rule);
  }

  @Override
  @RolesAllowed("admin")
  public Set<AuthorizationRule> getRules() {
    return new HashSet<>(rulesById.values());
  }

  @Override
  @RolesAllowed("admin")
  public void deleteRule(String id) throws NessieConflictException {
    AuthorizationRule rule = rulesById.get(id);
    if (null == rule) {
      throw new NessieConflictException(
          String.format("Authorization rule '%s' does not exist", id));
    }

    // TODO: this will be maintained later in the versionstore
    rulesById.remove(id);
    rulesByType.get(rule.type()).remove(rule);
  }
}
