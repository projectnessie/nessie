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

/**
 * Token Type Identifiers as defined in <a
 * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a> of RFC 8693.
 *
 * <p>Note: currently only {@link #ACCESS_TOKEN} and {@link #REFRESH_TOKEN} are supported. Other
 * token types are declared solely for completeness.
 */
class TokenTypeIdentifiers {

  private TokenTypeIdentifiers() {}

  /**
   * Indicates that the token is an OAuth 2.0 access token issued by the given authorization server.
   */
  public static final URI ACCESS_TOKEN =
      URI.create("urn:ietf:params:oauth:token-type:access_token");

  /**
   * Indicates that the token is an OAuth 2.0 refresh token issued by the given authorization
   * server.
   */
  public static final URI REFRESH_TOKEN =
      URI.create("urn:ietf:params:oauth:token-type:refresh_token");

  /** Indicates that the token is an ID Token as defined in Section 2 of [OpenID.Core]. */
  public static final URI ID_TOKEN = URI.create("urn:ietf:params:oauth:token-type:id_token");

  /** Indicates that the token is a base64url-encoded SAML 1.1 [OASIS.saml-core-1.1] assertion. */
  public static final URI SAML1 = URI.create("urn:ietf:params:oauth:token-type:saml1");

  /**
   * Indicates that the token is a base64url-encoded SAML 2.0 [OASIS.saml-core-2.0-os] assertion.
   */
  public static final URI SAML2 = URI.create("urn:ietf:params:oauth:token-type:saml2");

  /**
   * The value urn:ietf:params:oauth:token-type:jwt, which is defined in Section 9 of [JWT],
   * indicates that the token is a JWT.
   */
  public static final URI JWT = URI.create("urn:ietf:params:oauth:token-type:jwt");
}
