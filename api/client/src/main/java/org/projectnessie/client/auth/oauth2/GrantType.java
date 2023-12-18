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
package org.projectnessie.client.auth.oauth2;

import com.fasterxml.jackson.annotation.JsonValue;

public enum GrantType {

  // initial grant types
  CLIENT_CREDENTIALS("client_credentials"),
  PASSWORD("password"),
  AUTHORIZATION_CODE("authorization_code"),

  // grant types for refreshing tokens (cannot be used for initial token acquisition)
  REFRESH_TOKEN("refresh_token"),
  TOKEN_EXCHANGE("urn:ietf:params:oauth:grant-type:token-exchange"),
  ;

  private final String canonicalName;

  GrantType(String canonicalName) {
    this.canonicalName = canonicalName;
  }

  @JsonValue
  public String canonicalName() {
    return canonicalName;
  }

  public static GrantType fromCanonicalName(String canonicalName) {
    for (GrantType grantType : values()) {
      if (grantType.canonicalName.equals(canonicalName)) {
        return grantType;
      }
    }
    throw new IllegalArgumentException("Unknown grant type: " + canonicalName);
  }
}
