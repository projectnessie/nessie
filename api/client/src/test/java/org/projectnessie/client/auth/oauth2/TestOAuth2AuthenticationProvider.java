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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.util.HttpTestUtil.writeResponseBody;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.client.auth.NessieAuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider.OAuth2Authentication;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.util.HttpTestServer;

class TestOAuth2AuthenticationProvider {

  @Test
  @SuppressWarnings("resource")
  void testNullParams() {
    assertThatThrownBy(() -> new OAuth2AuthenticationProvider().build(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OAuth2AuthenticationProvider.create((OAuth2AuthenticatorConfig) null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OAuth2AuthenticationProvider.create((OAuth2Authenticator) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testFromConfig() throws Exception {
    try (HttpTestServer authServer = new HttpTestServer(this::handleAuth, false);
        HttpTestServer resourceServer = new HttpTestServer(this::handleResource, true)) {
      Map<String, String> authCfg =
          ImmutableMap.of(
              CONF_NESSIE_AUTH_TYPE,
              OAuth2AuthenticationProvider.AUTH_TYPE_VALUE,
              CONF_NESSIE_OAUTH2_CLIENT_ID,
              "Alice",
              CONF_NESSIE_OAUTH2_CLIENT_SECRET,
              "s3cr3t",
              CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
              authServer.getUri().toString());
      HttpAuthentication authentication =
          (HttpAuthentication) NessieAuthenticationProvider.fromConfig(authCfg::get);
      try (HttpClient httpClient =
          HttpClient.builder()
              .setAuthentication(authentication)
              .setSslContext(resourceServer.getSslContext())
              .setBaseUri(resourceServer.getUri())
              .setObjectMapper(OAuth2ClientConfig.OBJECT_MAPPER)
              .build()) {
        JsonNode response = httpClient.newRequest().get().readEntity(JsonNode.class);
        assertThat(response.get("success").asBoolean()).isTrue();
      }
    }
  }

  @Test
  void testProgrammaticOAuth2AuthenticationConfig() throws Exception {
    try (HttpTestServer authServer = new HttpTestServer(this::handleAuth, true);
        HttpTestServer resourceServer = new HttpTestServer(this::handleResource, true)) {
      OAuth2AuthenticatorConfig config =
          OAuth2AuthenticatorConfig.builder()
              .tokenEndpoint(authServer.getUri())
              .clientId("Alice")
              .clientSecret("s3cr3t")
              .sslContext(authServer.getSslContext())
              .build();
      OAuth2Authenticator authenticator = OAuth2AuthenticationProvider.newAuthenticator(config);
      HttpAuthentication authentication = OAuth2AuthenticationProvider.create(authenticator);
      try (HttpClient httpClient =
          HttpClient.builder()
              .setAuthentication(authentication)
              .setSslContext(resourceServer.getSslContext())
              .setBaseUri(resourceServer.getUri())
              .setObjectMapper(OAuth2ClientConfig.OBJECT_MAPPER)
              .build()) {
        JsonNode response = httpClient.newRequest().get().readEntity(JsonNode.class);
        assertThat(response.get("success").asBoolean()).isTrue();
      }
    }
  }

  private void handleAuth(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    TokenResponseBase tokenResponse =
        ImmutableClientCredentialsTokenResponse.builder()
            .accessTokenPayload("cafebabe")
            .tokenType("bearer")
            .build();
    writeResponseBody(response, tokenResponse, "application/json");
  }

  private void handleResource(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String authorization = request.getHeader("Authorization");
    boolean authSuccess = authorization.equalsIgnoreCase("Bearer cafebabe");
    writeResponseBody(response, String.format("{ \"success\" : %s }", authSuccess));
  }

  @Test
  public void testAddAuthHeader() {
    OAuth2Authenticator authenticator = mock(OAuth2Authenticator.class);
    when(authenticator.authenticate())
        .thenReturn(ImmutableAccessToken.builder().payload("cafebabe").tokenType("BeArEr").build());
    OAuth2Authentication authentication = new OAuth2Authentication(authenticator);
    RequestContext context = mock(RequestContext.class);
    authentication.applyToHttpRequest(context);
    verify(authenticator).authenticate();
    verify(context).putHeader("Authorization", "Bearer cafebabe");
  }

  @Test
  public void testTokenTypeInvalid() {
    OAuth2Authenticator authenticator = mock(OAuth2Authenticator.class);
    when(authenticator.authenticate())
        .thenReturn(
            ImmutableAccessToken.builder().payload("cafebabe").tokenType("INVALID").build());
    OAuth2Authentication authentication = new OAuth2Authentication(authenticator);
    RequestContext context = mock(RequestContext.class);
    assertThatThrownBy(() -> authentication.applyToHttpRequest(context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "OAuth2 token type returned from the authenticating server must be 'Bearer', but was: INVALID");
    verify(authenticator).authenticate();
    verify(context, Mockito.never()).putHeader(Mockito.any(), Mockito.any());
  }

  @Test
  void testCloseOAuth2Authentication() {
    OAuth2Authenticator authenticator = mock(OAuth2Authenticator.class);
    OAuth2Authentication authentication = new OAuth2Authentication(authenticator);
    authentication.close();
    verify(authenticator).close();
  }
}
