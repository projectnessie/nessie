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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2">Token Request</a> using
 * the "client_credentials" grant type to obtain a new access token.
 *
 * <p>Example:
 *
 * <pre>
 * POST /token HTTP/1.1
 * Host: server.example.com
 * Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=client_credentials
 * </pre>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableClientCredentialsTokenRequest.class)
@JsonDeserialize(as = ImmutableClientCredentialsTokenRequest.class)
@JsonTypeName(GrantType.Constants.CLIENT_CREDENTIALS)
interface ClientCredentialsTokenRequest extends TokenRequestBase {

  /** REQUIRED. Value MUST be set to "client_credentials". */
  @Value.Derived
  @Override
  default GrantType getGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  static Builder builder() {
    return ImmutableClientCredentialsTokenRequest.builder();
  }

  interface Builder extends TokenRequestBase.Builder<ClientCredentialsTokenRequest> {}
}
