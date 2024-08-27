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
import java.util.Optional;
import org.projectnessie.client.http.HttpAuthentication;

/**
 * A specialized {@link TokenExchangeFlow} that is performed after the initial token fetch flow, in
 * order to obtain a more fine-grained token through impersonation (or delegation).
 */
class ImpersonationFlow extends TokenExchangeFlow {

  private final ImpersonationConfig impersonationConfig;

  ImpersonationFlow(OAuth2ClientConfig config) {
    super(config);
    impersonationConfig = config.getImpersonationConfig();
  }

  @Override
  public Tokens fetchNewTokens(Tokens currentTokens) {
    Tokens newTokens = super.fetchNewTokens(currentTokens);
    return Tokens.of(newTokens.getAccessToken(), currentTokens.getRefreshToken());
  }

  @Override
  Optional<String> getScope() {
    if (!impersonationConfig.getScopes().equals(ImpersonationConfig.SCOPES_INHERIT)) {
      return impersonationConfig.getScopes().stream().reduce((a, b) -> a + " " + b);
    }
    return super.getScope();
  }

  @Override
  URI getResolvedTokenEndpoint() {
    return config
        .getResolvedImpersonationTokenEndpoint()
        .orElseGet(super::getResolvedTokenEndpoint);
  }

  @Override
  String getClientId() {
    return impersonationConfig.getClientId().orElseGet(super::getClientId);
  }

  @Override
  boolean isPublicClient() {
    return config.isImpersonationPublicClient().orElseGet(super::isPublicClient);
  }

  @Override
  Optional<HttpAuthentication> getBasicAuthentication() {
    return config.getImpersonationBasicAuthentication().or(super::getBasicAuthentication);
  }
}
