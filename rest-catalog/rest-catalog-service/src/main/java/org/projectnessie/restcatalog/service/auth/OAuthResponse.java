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

@Value.Immutable
public interface OAuthResponse {
  @Value.Parameter(order = 1)
  String accessToken();

  @Value.Parameter(order = 2)
  String issuedTokenType();

  @Value.Parameter(order = 3)
  String tokenType();

  @Value.Parameter(order = 4)
  @Nullable
  @jakarta.annotation.Nullable
  Integer expiresIn();

  @Value.Parameter(order = 5)
  @Nullable
  @jakarta.annotation.Nullable
  String scope();

  static OAuthResponse oauthResponse(
      String accessToken,
      String issuedTokenType,
      String tokenType,
      Integer expiresIn,
      String scope) {
    return ImmutableOAuthResponse.of(accessToken, issuedTokenType, tokenType, expiresIn, scope);
  }
}
