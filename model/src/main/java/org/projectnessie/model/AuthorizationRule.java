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
package org.projectnessie.model;

import static org.projectnessie.model.AuthorizationRuleType.ALLOW_ALL;
import static org.projectnessie.model.AuthorizationRuleType.DELETE_ENTITY;
import static org.projectnessie.model.AuthorizationRuleType.READ_ENTITY_VALUE;
import static org.projectnessie.model.AuthorizationRuleType.UPDATE_ENTITY;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "Authorization Rule")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableAuthorizationRule.class)
@JsonDeserialize(as = ImmutableAuthorizationRule.class)
public interface AuthorizationRule {

  String id();

  AuthorizationRuleType type();

  String ruleExpression();

  String roleExpression();

  @JsonIgnore
  @Value.Derived
  default boolean isAllowAll() {
    return ALLOW_ALL.equals(type());
  }

  @JsonIgnore
  @Value.Derived
  default boolean isPathRule() {
    return UPDATE_ENTITY.equals(type())
        || DELETE_ENTITY.equals(type())
        || READ_ENTITY_VALUE.equals(type());
  }

  @JsonIgnore
  @Value.Derived
  default boolean isReferenceRule() {
    return !isPathRule();
  }

  static AuthorizationRule of(
      String id, AuthorizationRuleType type, String ruleExpression, String roleExpression) {
    return ImmutableAuthorizationRule.builder()
        .id(id)
        .type(type)
        .ruleExpression(ruleExpression)
        .roleExpression(roleExpression)
        .build();
  }

  static AuthorizationRule of(String id, AuthorizationRuleType type) {
    if (!ALLOW_ALL.equals(type)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid rule '%s' provided. Only allowed to use with ALLOW_ALL rule", type));
    }
    return ImmutableAuthorizationRule.builder()
        .id(id)
        .type(type)
        .ruleExpression("")
        .roleExpression("")
        .build();
  }
}
