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
package org.projectnessie.restcatalog.service.auth;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.restcatalog.api.IcebergV1Oauth.TokenType;

@Value.Immutable
public interface OAuthRequest {
  @Value.Parameter(order = 1)
  @Nullable
  @jakarta.annotation.Nullable
  String grantType();

  @Value.Parameter(order = 2)
  @Nullable
  @jakarta.annotation.Nullable
  String scope();

  @Value.Parameter(order = 3)
  @Nullable
  @jakarta.annotation.Nullable
  String clientId();

  @Value.Parameter(order = 4)
  @Nullable
  @jakarta.annotation.Nullable
  String clientSecret();

  @Value.Parameter(order = 5)
  @Nullable
  @jakarta.annotation.Nullable
  TokenType requestedTokenType();

  @Value.Parameter(order = 6)
  @Nullable
  @jakarta.annotation.Nullable
  String subjectToken();

  @Value.Parameter(order = 7)
  @Nullable
  @jakarta.annotation.Nullable
  TokenType subjectTokenType();

  @Value.Parameter(order = 8)
  @Nullable
  @jakarta.annotation.Nullable
  String actorToken();

  @Value.Parameter(order = 9)
  @Nullable
  @jakarta.annotation.Nullable
  TokenType actorTokenType();

  static OAuthRequest oauthRequest(
      String grantType,
      String scope,
      String clientId,
      String clientSecret,
      TokenType requestedTokenType,
      String subjectToken,
      TokenType subjectTokenType,
      String actorToken,
      TokenType actorTokenType) {
    return ImmutableOAuthRequest.of(
        grantType,
        scope,
        clientId,
        clientSecret,
        requestedTokenType,
        subjectToken,
        subjectTokenType,
        actorToken,
        actorTokenType);
  }
}
