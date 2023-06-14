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
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.projectnessie.restcatalog.api.errors.OAuthTokenEndpointException;
import org.projectnessie.restcatalog.service.auth.OAuthTokenRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuthUtils.class);

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
            .withTokenType(json.getString(TOKEN_TYPE));
    if (json.containsKey(ISSUED_TOKEN_TYPE)) {
      builder.withIssuedTokenType(json.getString(ISSUED_TOKEN_TYPE));
    } else {
      // Iceberg won't be able to refresh tokens without a value for this field,
      // se we need to set it to something that will work.
      builder.withIssuedTokenType(OAuth2Properties.ACCESS_TOKEN_TYPE);
    }
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

  public static void printOAuthTokenRequest(OAuthTokenRequest req) {
    String encoded = IOUtils.toString(req.body(), StandardCharsets.UTF_8.name());
    Map<String, String> formData = RESTUtil.decodeFormData(encoded);
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("Sending token request:");
    pw.println("- headers:");
    req.headers()
        .forEach(
            (k, v) -> v.forEach(s -> pw.printf("  %s: %s%n", k, StringUtils.truncate(s, 100))));
    pw.println("- body:");
    formData.forEach((k, v) -> pw.printf("  %s: %s%n", k, StringUtils.truncate(v, 100)));
    LOGGER.debug(sw.toString());
  }

  public static void printOAuthTokenResponse(HttpResponse<Buffer> resp, JsonObject json) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("Received token response:");
    pw.printf("- status:%s%n", resp.statusCode());
    pw.println("- headers:");
    resp.headers().forEach((k, v) -> pw.printf("  %s: %s%n", k, StringUtils.truncate(v, 100)));
    pw.println("- body:");
    json.fieldNames()
        .forEach(
            k ->
                pw.printf(
                    "  %s: %s%n", k, StringUtils.truncate(String.valueOf(json.getValue(k)), 100)));
    LOGGER.debug(sw.toString());
  }
}
