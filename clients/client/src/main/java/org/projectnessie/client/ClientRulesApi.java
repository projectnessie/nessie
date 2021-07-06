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
package org.projectnessie.client;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.projectnessie.api.RulesApi;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.model.AuthorizationRule;

class ClientRulesApi implements RulesApi {

  private static final TypeReference<Set<AuthorizationRule>> RULES_SET =
      new TypeReference<Set<AuthorizationRule>>() {};
  private final HttpClient client;

  public ClientRulesApi(HttpClient client) {
    this.client = client;
  }

  @Override
  public void addRule(@NotNull @Valid AuthorizationRule rule) {
    client.newRequest().path("/rules/rule").post(rule);
  }

  @Override
  public Set<AuthorizationRule> getRules() {
    return client.newRequest().path("rules").get().readEntity(RULES_SET);
  }

  @Override
  public void deleteRule(@NotNull String id) {
    client.newRequest().path("rules/rule/{id}").resolveTemplate("id", id).delete();
  }
}
