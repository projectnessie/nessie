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

import java.net.URI;
import java.util.Objects;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 *
 * <p>This implementation is limited to the use case of exchanging an OAuth 2.0 access token for an
 * OAuth 2.0 refresh token.
 */
class TokenExchangeFlow extends AbstractFlow {

  static final URI ACCESS_TOKEN_ID = URI.create("urn:ietf:params:oauth:token-type:access_token");

  TokenExchangeFlow(OAuth2ClientConfig config) {
    super(config);
  }

  @Override
  public Tokens fetchNewTokens(Tokens currentTokens) {
    if (!config.getTokenExchangeEnabled()) {
      throw new MustFetchNewTokensException("Token exchange is disabled");
    }
    Objects.requireNonNull(currentTokens);
    TokensExchangeRequest.Builder request =
        TokensExchangeRequest.builder()
            .subjectToken(currentTokens.getAccessToken().getPayload())
            .subjectTokenType(ACCESS_TOKEN_ID)
            .requestedTokenType(ACCESS_TOKEN_ID);
    Tokens response = invokeTokenEndpoint(request, TokensExchangeResponse.class);
    // Keycloak may return the same access token instead of a new one,
    // so we need to check if the access token is about to expire.
    if (isAboutToExpire(response.getAccessToken())) {
      throw new MustFetchNewTokensException("Access token is about to expire");
    }
    return response;
  }
}
