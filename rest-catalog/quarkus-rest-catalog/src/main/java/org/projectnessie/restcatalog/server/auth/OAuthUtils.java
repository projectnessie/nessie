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
package org.projectnessie.restcatalog.server.auth;

import static org.apache.iceberg.rest.auth.OAuth2Util.parseScope;

import com.fasterxml.jackson.databind.JsonNode;
import io.vertx.core.json.JsonObject;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.projectnessie.restcatalog.api.errors.OAuthTokenEndpointException;

public class OAuthUtils {

  private static final String ACCESS_TOKEN = "access_token";
  private static final String TOKEN_TYPE = "token_type";
  private static final String EXPIRES_IN = "expires_in";
  private static final String ISSUED_TOKEN_TYPE = "issued_token_type";
  private static final String SCOPE = "scope";
  private static final String ERROR = "error";
  private static final String ERROR_DESCRIPTION = "error_description";

  /**
   * Similar to {@link
   * org.apache.iceberg.rest.auth.OAuth2Util#tokenResponseToJson(OAuthTokenResponse)}.
   */
  public static OAuthTokenResponse tokenResponseFromJson(JsonObject json) {
    OAuthTokenResponse.Builder builder =
        OAuthTokenResponse.builder()
            .withToken(json.getString(ACCESS_TOKEN))
            .withTokenType(json.getString(TOKEN_TYPE))
            .withIssuedTokenType(json.getString(ISSUED_TOKEN_TYPE));
    if (json.containsKey(EXPIRES_IN)) {
      builder.setExpirationInSeconds(json.getInteger(EXPIRES_IN));
    }
    if (json.containsKey(SCOPE)) {
      builder.addScopes(parseScope(json.getString(SCOPE)));
    }
    return builder.build();
  }

  /**
   * Similar to {@link org.apache.iceberg.rest.responses.OAuthErrorResponseParser#fromJson(int,
   * JsonNode)}.
   */
  public static OAuthTokenEndpointException errorFromJson(JsonObject json, int statusCode) {
    String error = json.getString(ERROR);
    String errorDescription = json.getString(ERROR_DESCRIPTION);
    if (error == null) {
      error = "UnknownOAuthTokenEndpointError";
      errorDescription = json.encode();
    }
    return new OAuthTokenEndpointException(statusCode, error, errorDescription);
  }
}
