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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.1.3">Token Request</a> using
 * the "authorization_code" grant type to obtain a new access token.
 *
 * <p>Example:
 *
 * <pre>
 * POST /token HTTP/1.1
 * Host: server.example.com
 * Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=authorization_code&code=SplxlOBeZQQYbYS6WxSbIA
 * &redirect_uri=https%3A%2F%2Fclient%2Eexample%2Ecom%2Fcb
 * </pre>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableAuthorizationCodeTokenRequest.class)
@JsonDeserialize(as = ImmutableAuthorizationCodeTokenRequest.class)
@JsonTypeName(GrantType.Constants.AUTHORIZATION_CODE)
interface AuthorizationCodeTokenRequest extends TokenRequestBase, PublicClientRequest {

  /** REQUIRED. Value MUST be set to "authorization_code". */
  @Value.Derived
  @Override
  default GrantType getGrantType() {
    return GrantType.AUTHORIZATION_CODE;
  }

  /** REQUIRED. The authorization code received from the authorization server. */
  @JsonProperty("code")
  String getCode();

  /** REQUIRED. The redirect URI used in the initial request. */
  @JsonProperty("redirect_uri")
  String getRedirectUri();

  static Builder builder() {
    return ImmutableAuthorizationCodeTokenRequest.builder();
  }

  interface Builder
      extends TokenRequestBase.Builder<AuthorizationCodeTokenRequest>,
          PublicClientRequest.Builder<AuthorizationCodeTokenRequest> {
    @CanIgnoreReturnValue
    Builder code(String code);

    @CanIgnoreReturnValue
    Builder redirectUri(String redirectUri);
  }
}
