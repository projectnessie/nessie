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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import javax.annotation.Nullable;

/**
 * Common base for all requests to the token endpoint.
 *
 * @see ClientCredentialsTokenRequest
 * @see PasswordTokenRequest
 * @see AuthorizationCodeTokenRequest
 * @see DeviceCodeRequest
 * @see RefreshTokenRequest
 * @see TokenExchangeRequest
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "grant_type")
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = AuthorizationCodeTokenRequest.class,
      name = GrantType.Constants.AUTHORIZATION_CODE),
  @JsonSubTypes.Type(
      value = ClientCredentialsTokenRequest.class,
      name = GrantType.Constants.CLIENT_CREDENTIALS),
  @JsonSubTypes.Type(value = DeviceCodeTokenRequest.class, name = GrantType.Constants.DEVICE_CODE),
  @JsonSubTypes.Type(value = PasswordTokenRequest.class, name = GrantType.Constants.PASSWORD),
  @JsonSubTypes.Type(value = RefreshTokenRequest.class, name = GrantType.Constants.REFRESH_TOKEN),
  @JsonSubTypes.Type(value = TokenExchangeRequest.class, name = GrantType.Constants.TOKEN_EXCHANGE),
})
interface TokenRequestBase {

  /** REQUIRED. The authorization grant type. */
  @JsonProperty("grant_type")
  GrantType getGrantType();

  /**
   * OPTIONAL, if identical to the scope requested by the client; otherwise, REQUIRED. The scope of
   * the access token as described by <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">Section 3.3</a>.
   *
   * <p>In case of refresh, the requested scope MUST NOT include any scope not originally granted by
   * the resource owner, and if omitted is treated as equal to the scope originally granted by the
   * resource owner.
   */
  @Nullable
  @JsonProperty("scope")
  String getScope();

  interface Builder<T extends TokenRequestBase> {

    @CanIgnoreReturnValue
    Builder<T> scope(String scope);

    T build();
  }
}
