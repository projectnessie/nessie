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
import java.util.Optional;
import org.projectnessie.client.http.HttpAuthentication;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 *
 * <p>This implementation is limited to the use case of exchanging an OAuth 2.0 access token for an
 * OAuth 2.0 refresh token.
 */
class TokenExchangeFlow extends AbstractFlow {

  TokenExchangeFlow(OAuth2ClientConfig config) {
    super(config);
  }

  @Override
  public Tokens fetchNewTokens(Tokens currentTokens) {
    Objects.requireNonNull(currentTokens);

    TokenExchangeConfig tokenExchangeConfig = config.getTokenExchangeConfig();

    AccessToken accessToken = currentTokens.getAccessToken();
    RefreshToken refreshToken = currentTokens.getRefreshToken();

    TypedToken subjectToken =
        tokenExchangeConfig.getSubjectTokenProvider().apply(accessToken, refreshToken);
    TypedToken actorToken =
        tokenExchangeConfig.getActorTokenProvider().apply(accessToken, refreshToken);

    Objects.requireNonNull(subjectToken);

    TokenExchangeRequest.Builder request =
        TokenExchangeRequest.builder()
            .subjectToken(subjectToken.getPayload())
            .subjectTokenType(subjectToken.getTokenType())
            .actorToken(actorToken == null ? null : actorToken.getPayload())
            .actorTokenType(actorToken == null ? null : actorToken.getTokenType())
            .resource(tokenExchangeConfig.getResource().orElse(null))
            .audience(tokenExchangeConfig.getAudience().orElse(null))
            .requestedTokenType(tokenExchangeConfig.getRequestedTokenType());

    return invokeTokenEndpoint(request, TokenExchangeResponse.class);
  }

  @Override
  Optional<String> getScope() {
    return config.getScopesForTokenExchange().stream().reduce((a, b) -> a + " " + b);
  }

  @Override
  URI getResolvedTokenEndpoint() {
    return config.getResolvedTokenEndpointForTokenExchange();
  }

  @Override
  String getClientId() {
    return config.getClientIdForTokenExchange();
  }

  @Override
  boolean isPublicClient() {
    return config.isPublicClientForTokenExchange();
  }

  @Override
  Optional<HttpAuthentication> getBasicAuthentication() {
    return config.getBasicAuthenticationForTokenExchange();
  }
}
