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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-6">Token Request</a> that uses
 * the "refresh_tokens" grant type to refresh an existing access token.
 *
 * <p>Example:
 *
 * <pre>
 * POST /token HTTP/1.1
 * Host: server.example.com
 * Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=refresh_token&refresh_token=tGzv3JOkF0XG5Qx2TlKWIA
 * </pre>
 *
 * The response to this request is an {@link ClientCredentialsTokensResponse}.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableRefreshTokensRequest.class)
@JsonDeserialize(as = ImmutableRefreshTokensRequest.class)
interface RefreshTokensRequest extends TokensRequestBase {

  /** REQUIRED. Value MUST be set to "refresh_token". */
  @Value.Default
  @JsonProperty("grant_type")
  @Override
  default GrantType getGrantType() {
    return GrantType.REFRESH_TOKEN;
  }

  /** REQUIRED. The refresh token issued to the client. */
  @JsonProperty("refresh_token")
  String getRefreshToken();
}
