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

import java.net.URI;
import org.immutables.value.Value;

/** Represents a token with a specific type URI. Such tokens are used in the Token Exchange flow. */
@Value.Immutable
public interface TypedToken extends Token {

  /**
   * Indicates that the token is an OAuth 2.0 access token issued by the given authorization server.
   */
  URI URN_ACCESS_TOKEN = URI.create("urn:ietf:params:oauth:token-type:access_token");

  /**
   * Indicates that the token is an OAuth 2.0 refresh token issued by the given authorization
   * server.
   */
  URI URN_REFRESH_TOKEN = URI.create("urn:ietf:params:oauth:token-type:refresh_token");

  /** Indicates that the token is an ID Token as defined in Section 2 of [OpenID.Core]. */
  URI URN_ID_TOKEN = URI.create("urn:ietf:params:oauth:token-type:id_token");

  /** Indicates that the token is a base64url-encoded SAML 1.1 [OASIS.saml-core-1.1] assertion. */
  URI URN_SAML1 = URI.create("urn:ietf:params:oauth:token-type:saml1");

  /**
   * Indicates that the token is a base64url-encoded SAML 2.0 [OASIS.saml-core-2.0-os] assertion.
   */
  URI URN_SAML2 = URI.create("urn:ietf:params:oauth:token-type:saml2");

  /**
   * The value urn:ietf:params:oauth:token-type:jwt, which is defined in Section 9 of [JWT],
   * indicates that the token is a JWT.
   */
  URI URN_JWT = URI.create("urn:ietf:params:oauth:token-type:jwt");

  /** The type of the token, by default {@link #URN_ACCESS_TOKEN}. */
  @Value.Default
  default URI getTokenType() {
    return URN_ACCESS_TOKEN;
  }

  static TypedToken of(String payload, URI type) {
    return ImmutableTypedToken.builder().payload(payload).tokenType(type).build();
  }

  static TypedToken fromAccessToken(AccessToken accessToken) {
    return ImmutableTypedToken.builder().from(accessToken).build();
  }
}
