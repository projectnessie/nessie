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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientConfig.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.client.util.HttpTestServer.RequestHandler;

class TestOAuth2Utils {

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 10, 15, 20, 100})
  void testRandomAlphaNumString(int length) {
    String actual = OAuth2Utils.randomAlphaNumString(length);
    assertThat(actual).hasSize(length).matches("^[a-zA-Z0-9]*$");
  }

  private static final String DATA =
      "{"
          + "\"issuer\":\"http://server.com/realms/master\","
          + "\"authorization_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/auth\","
          + "\"token_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/token\""
          + "}";

  private static final String WRONG_DATA =
      "{"
          + "\"authorization_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/auth\","
          + "\"token_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/token\""
          + "}";

  @ParameterizedTest
  @CsvSource({
    "''              , /.well-known/openid-configuration",
    "/               , /.well-known/openid-configuration",
    "/realms/master  , /realms/master/.well-known/openid-configuration",
    "/realms/master/ , /realms/master/.well-known/openid-configuration"
  })
  void fetchOpenIdProviderMetadataSuccess(String issuerPath, String wellKnownPath)
      throws Exception {
    try (HttpTestServer server = new HttpTestServer(handler(wellKnownPath, DATA), true);
        HttpClient httpClient = newHttpClient(server)) {
      URI issuerUrl = server.getUri().resolve(issuerPath);
      JsonNode actual = OAuth2Utils.fetchOpenIdProviderMetadata(httpClient, issuerUrl);
      assertThat(actual).isEqualTo(OBJECT_MAPPER.readTree(DATA));
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongEndpoint() throws Exception {
    try (HttpTestServer server = new HttpTestServer(handler("/wrong/path", DATA), true);
        HttpClient httpClient = newHttpClient(server)) {
      URI issuerUrl = server.getUri().resolve("/realms/master/");
      assertThatThrownBy(() -> OAuth2Utils.fetchOpenIdProviderMetadata(httpClient, issuerUrl))
          .isInstanceOf(HttpClientException.class)
          .hasMessageContaining("404"); // messages differ between HttpClient impls
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongData() throws Exception {
    try (HttpTestServer server =
            new HttpTestServer(
                handler("/realms/master/.well-known/openid-configuration", WRONG_DATA), true);
        HttpClient httpClient = newHttpClient(server)) {
      URI issuerUrl = server.getUri().resolve("/realms/master/");
      assertThatThrownBy(() -> OAuth2Utils.fetchOpenIdProviderMetadata(httpClient, issuerUrl))
          .isInstanceOfAny(HttpClientException.class)
          .hasMessage("Invalid OpenID provider metadata");
    }
  }

  private RequestHandler handler(String wellKnownPath, String data) {
    return (req, resp) -> {
      if (req.getRequestURI().equals(wellKnownPath)) {
        resp.setStatus(200);
        resp.addHeader("Content-Type", "application/json;charset=UTF-8");
        resp.addHeader("Content-Length", String.valueOf(data.length()));
        resp.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
      } else {
        resp.setStatus(404);
      }
    };
  }

  private static HttpClient newHttpClient(HttpTestServer server) {
    return HttpClient.builder()
        .setSslContext(server.getSslContext())
        .setObjectMapper(OBJECT_MAPPER)
        .build();
  }
}
