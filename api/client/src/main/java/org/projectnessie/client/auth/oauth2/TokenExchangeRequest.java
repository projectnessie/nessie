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
import java.net.URI;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc8693/#section-2.1">Token Exchange Request</a>
 * that is used to exchange an access token for a pair of access + refresh tokens.
 *
 * <p>Example:
 *
 * <pre>
 * POST /as/token.oauth2 HTTP/1.1
 * Host: as.example.com
 * Authorization: Basic cnMwODpsb25nLXNlY3VyZS1yYW5kb20tc2VjcmV0
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange
 * &resource=https%3A%2F%2Fbackend.example.com%2Fapi
 * &subject_token=accVkjcJyb4BWCxGsndESCJQbdFMogUC5PbRDqceLTC
 * &subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token
 * </pre>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTokenExchangeRequest.class)
@JsonDeserialize(as = ImmutableTokenExchangeRequest.class)
@JsonTypeName(GrantType.Constants.TOKEN_EXCHANGE)
interface TokenExchangeRequest extends TokenRequestBase, PublicClientRequest {

  /**
   * REQUIRED. The value {@link GrantType#TOKEN_EXCHANGE} indicates that a token exchange is being
   * performed.
   */
  @Value.Derived
  @Override
  default GrantType getGrantType() {
    return GrantType.TOKEN_EXCHANGE;
  }

  /**
   * OPTIONAL. A URI that indicates the target service or resource where the client intends to use
   * the requested security token. This enables the authorization server to apply policy as
   * appropriate for the target, such as determining the type and content of the token to be issued
   * or if and how the token is to be encrypted. In many cases, a client will not have knowledge of
   * the logical organization of the systems with which it interacts and will only know a URI of the
   * service where it intends to use the token. The resource parameter allows the client to indicate
   * to the authorization server where it intends to use the issued token by providing the location,
   * typically as an https URL, in the token exchange request in the same form that will be used to
   * access that resource. The authorization server will typically have the capability to map from a
   * resource URI value to an appropriate policy. The value of the resource parameter MUST be an
   * absolute URI, as specified by Section 4.3 of [RFC3986], that MAY include a query component and
   * MUST NOT include a fragment component. Multiple resource parameters may be used to indicate
   * that the issued token is intended to be used at the multiple resources listed. See
   * [OAUTH-RESOURCE] for additional background and uses of the resource parameter.
   */
  @Nullable
  URI getResource();

  /**
   * OPTIONAL. The logical name of the target service where the client intends to use the requested
   * security token. This serves a purpose similar to the resource parameter but with the client
   * providing a logical name for the target service. Interpretation of the name requires that the
   * value be something that both the client and the authorization server understand. An OAuth
   * client identifier, a SAML entity identifier [OASIS.saml-core-2.0-os], and an OpenID Connect
   * Issuer Identifier [OpenID.Core] are examples of things that might be used as audience parameter
   * values. However, audience values used with a given authorization server must be unique within
   * that server to ensure that they are properly interpreted as the intended type of value.
   * Multiple audience parameters may be used to indicate that the issued token is intended to be
   * used at the multiple audiences listed. The audience and resource parameters may be used
   * together to indicate multiple target services with a mix of logical names and resource URIs.
   */
  @Nullable
  String getAudience();

  /**
   * OPTIONAL. An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, for the type of
   * the requested security token. If the requested type is unspecified, the issued token type is at
   * the discretion of the authorization server and may be dictated by knowledge of the requirements
   * of the service or resource indicated by the resource or audience parameter.
   */
  @Nullable
  @JsonProperty("requested_token_type")
  URI getRequestedTokenType();

  /**
   * REQUIRED. A security token that represents the identity of the party on behalf of whom the
   * request is being made. Typically, the subject of this token will be the subject of the security
   * token issued in response to the request.
   */
  @JsonProperty("subject_token")
  String getSubjectToken();

  /**
   * REQUIRED. An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, that indicates
   * the type of the security token in the subject_token parameter.
   */
  @JsonProperty("subject_token_type")
  URI getSubjectTokenType();

  /**
   * OPTIONAL. A security token that represents the identity of the acting party. Typically, this
   * will be the party that is authorized to use the requested security token and act on behalf of
   * the subject.
   */
  @Nullable
  @JsonProperty("actor_token")
  String getActorToken();

  /**
   * REQUIRED. An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, that indicates
   * the type of the security token in the actor_token parameter. This is REQUIRED when the
   * actor_token parameter is present in the request but MUST NOT be included otherwise.
   */
  @Nullable
  @JsonProperty("actor_token_type")
  URI getActorTokenType();

  static Builder builder() {
    return ImmutableTokenExchangeRequest.builder();
  }

  interface Builder
      extends TokenRequestBase.Builder<TokenExchangeRequest>,
          PublicClientRequest.Builder<TokenExchangeRequest> {

    @CanIgnoreReturnValue
    Builder resource(URI resource);

    @CanIgnoreReturnValue
    Builder audience(String audience);

    @CanIgnoreReturnValue
    Builder requestedTokenType(URI requestedTokenType);

    @CanIgnoreReturnValue
    Builder subjectToken(String subjectToken);

    @CanIgnoreReturnValue
    Builder subjectTokenType(URI subjectTokenType);

    @CanIgnoreReturnValue
    Builder actorToken(String actorToken);

    @CanIgnoreReturnValue
    Builder actorTokenType(URI actorTokenType);
  }
}
