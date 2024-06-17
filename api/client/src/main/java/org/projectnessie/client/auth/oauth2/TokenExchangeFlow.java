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

import java.util.Objects;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 */
class TokenExchangeFlow extends AbstractFlow {

  TokenExchangeFlow(OAuth2ClientConfig config) {
    super(config);
  }

  @Override
  public Tokens fetchNewTokens(Tokens currentTokens) {
    TokenExchangeConfig tokenExchangeConfig = config.getTokenExchangeConfig();

    AccessToken accessToken = currentTokens == null ? null : currentTokens.getAccessToken();
    RefreshToken refreshToken = currentTokens == null ? null : currentTokens.getRefreshToken();

    TypedToken subjectToken =
        tokenExchangeConfig.getSubjectTokenProvider().apply(accessToken, refreshToken);
    TypedToken actorToken =
        tokenExchangeConfig.getActorTokenProvider().apply(accessToken, refreshToken);

    Objects.requireNonNull(
        subjectToken, "Cannot execute token exchange: missing required subject token");

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
}
