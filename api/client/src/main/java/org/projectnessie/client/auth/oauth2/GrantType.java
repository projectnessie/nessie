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
import java.util.Locale;

public enum GrantType {

  // initial grant types

  CLIENT_CREDENTIALS("client_credentials") {
    @Override
    Flow newFlow(OAuth2ClientConfig config) {
      return new ClientCredentialsFlow(config);
    }
  },
  PASSWORD("password") {
    @Override
    Flow newFlow(OAuth2ClientConfig config) {
      return new ResourceOwnerPasswordFlow(config);
    }
  },
  AUTHORIZATION_CODE("authorization_code") {
    @Override
    Flow newFlow(OAuth2ClientConfig config) {
      return new AuthorizationCodeFlow(config);
    }
  },
  DEVICE_CODE("urn:ietf:params:oauth:grant-type:device_code") {
    @Override
    Flow newFlow(OAuth2ClientConfig config) {
      return new DeviceCodeFlow(config);
    }
  },

  // grant types for refreshing tokens (cannot be used for initial token acquisition)

  REFRESH_TOKEN("refresh_token") {
    @Override
    Flow newFlow(OAuth2ClientConfig config) {
      return new RefreshTokensFlow(config);
    }
  },
  TOKEN_EXCHANGE("urn:ietf:params:oauth:grant-type:token-exchange") {
    @Override
    Flow newFlow(OAuth2ClientConfig config) {
      return new TokenExchangeFlow(config);
    }
  },
  ;

  private final String canonicalName;

  GrantType(String canonicalName) {
    this.canonicalName = canonicalName;
  }

  @JsonValue
  public String canonicalName() {
    return canonicalName;
  }

  public static GrantType fromConfigName(String name) {
    for (GrantType grantType : values()) {
      if (grantType.name().equals(name.toUpperCase(Locale.ROOT))
          || grantType.canonicalName.equals(name)) {
        return grantType;
      }
    }
    throw new IllegalArgumentException("Unknown grant type: " + name);
  }

  abstract Flow newFlow(OAuth2ClientConfig config);

  public boolean requiresUserInteraction() {
    return this == AUTHORIZATION_CODE || this == DEVICE_CODE;
  }
}
