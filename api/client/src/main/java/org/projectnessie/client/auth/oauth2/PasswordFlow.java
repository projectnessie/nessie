/*
 * Copyright (C) 2024 Dremio
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

import javax.annotation.Nullable;

/**
 * An implementation of the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.3">Resource Owner Password
 * Credentials Grant</a> flow.
 */
class PasswordFlow extends AbstractFlow {

  PasswordFlow(OAuth2ClientConfig config) {
    super(config);
  }

  @Override
  public Tokens fetchNewTokens(@Nullable Tokens ignored) {
    String username =
        config.getUsername().orElseThrow(() -> new IllegalStateException("Username is required"));
    String password =
        config
            .getPassword()
            .map(Secret::getString)
            .orElseThrow(() -> new IllegalStateException("Password is required"));
    PasswordTokenRequest.Builder request =
        PasswordTokenRequest.builder().username(username).password(password);
    return invokeTokenEndpoint(request, PasswordTokenResponse.class);
  }
}
