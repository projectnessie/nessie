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
import java.net.URI;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc8693/#section-2.2.1">Token Response</a> in
 * reply to a {@link TokenExchangeRequest}.
 *
 * <p>A token exchange response is a normal OAuth 2.0 response from the token endpoint with a few
 * additional parameters.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTokenExchangeResponse.class)
@JsonDeserialize(as = ImmutableTokenExchangeResponse.class)
interface TokenExchangeResponse extends TokenResponseBase {

  /**
   * REQUIRED. An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, for the
   * representation of the issued security token.
   */
  @JsonProperty("issued_token_type")
  // This is a required field, but Keycloak does not return it for OIDC clients, only for SAML
  // clients.
  @Nullable
  URI getIssuedTokenType();
}
