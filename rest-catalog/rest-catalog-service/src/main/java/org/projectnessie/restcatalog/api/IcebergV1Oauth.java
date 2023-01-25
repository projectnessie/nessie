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
package org.projectnessie.restcatalog.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** Iceberg REST API v1 JAX-RS interface, using request and response types from Iceberg. */
@Path("iceberg/v1")
@jakarta.ws.rs.Path("iceberg/v1")
@Tag(name = "Iceberg v1")
public interface IcebergV1Oauth {

  @POST
  @jakarta.ws.rs.POST
  @Path("/oauth/tokens")
  @jakarta.ws.rs.Path("/oauth/tokens")
  @Consumes({"application/x-www-form-urlencoded"})
  @jakarta.ws.rs.Consumes({"application/x-www-form-urlencoded"})
  @Produces({MediaType.APPLICATION_JSON})
  @jakarta.ws.rs.Produces({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
  OAuthTokenResponse getToken(
      @FormParam(value = "grant_type") @jakarta.ws.rs.FormParam(value = "grant_type")
          String grantType,
      @FormParam(value = "scope") @jakarta.ws.rs.FormParam(value = "scope") String scope,
      @FormParam(value = "client_id") @jakarta.ws.rs.FormParam(value = "client_id") String clientId,
      @FormParam(value = "client_secret") @jakarta.ws.rs.FormParam(value = "client_secret")
          String clientSecret,
      @jakarta.ws.rs.FormParam(value = "requested_token_type")
          @FormParam(value = "requested_token_type")
          TokenType requestedTokenType,
      @FormParam(value = "subject_token") @jakarta.ws.rs.FormParam(value = "subject_token")
          String subjectToken,
      @FormParam(value = "subject_token_type")
          @jakarta.ws.rs.FormParam(value = "subject_token_type")
          TokenType subjectTokenType,
      @FormParam(value = "actor_token") @jakarta.ws.rs.FormParam(value = "actor_token")
          String actorToken,
      @FormParam(value = "actor_token_type") @jakarta.ws.rs.FormParam(value = "actor_token_type")
          TokenType actorTokenType);

  enum TokenType {
    BEARER("bearer"),
    N_A("N_A"),

    ACCESS_TOKEN("urn:ietf:params:oauth:token-type:access_token"),

    REFRESH_TOKEN("urn:ietf:params:oauth:token-type:refresh_token"),

    ID_TOKEN("urn:ietf:params:oauth:token-type:id_token"),

    SAML1("urn:ietf:params:oauth:token-type:saml1"),

    SAML2("urn:ietf:params:oauth:token-type:saml2"),

    JWT("urn:ietf:params:oauth:token-type:jwt");

    private final String value;

    TokenType(String value) {
      this.value = value;
    }

    @SuppressWarnings("unused")
    public static TokenType fromString(String s) {
      for (TokenType b : TokenType.values()) {
        // using Objects.toString() to be safe if value type non-object type
        // because types like 'int' etc. will be auto-boxed
        if (java.util.Objects.toString(b.value).equals(s)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected string value '" + s + "'");
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }

    @JsonCreator
    public static TokenType fromValue(String value) {
      for (TokenType b : TokenType.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }
}
