/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.server;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static io.quarkus.test.oidc.server.OidcWiremockTestResource.getAccessToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.oidc.server.OidcWireMock;
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import io.smallrye.jwt.build.Jwt;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.oauth2.ResourceOwnerEmulator;
import org.projectnessie.client.http.impl.HttpUtils;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@SuppressWarnings("resource")
@QuarkusTest
@QuarkusTestResource(OidcWiremockTestResource.class)
@TestProfile(value = TestOAuth2Authentication.Profile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOAuth2Authentication extends AbstractOAuth2Authentication {

  private static final String VALID_TOKEN = getAccessToken("alice", ImmutableSet.of("user"));
  private static final String TOKEN_ENDPOINT_PATH = "/auth/realms/quarkus/token";
  private static final String AUTH_ENDPOINT_PATH = "/auth/realms/quarkus/auth";

  @OidcWireMock private WireMockServer wireMockServer;

  @Test
  void testExpired() {
    NessieAuthentication authentication = oauth2Authentication(expiredConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }

  @Test
  void testWrongIssuer() {
    NessieAuthentication authentication = oauth2Authentication(wrongIssuerConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }

  private Properties expiredConfig() {
    Properties config = clientCredentialsConfig();
    config.setProperty("nessie.authentication.oauth2.client-secret", "EXPIRED");
    return config;
  }

  private Properties wrongIssuerConfig() {
    Properties config = clientCredentialsConfig();
    config.setProperty("nessie.authentication.oauth2.client-secret", "WRONG_ISSUER");
    return config;
  }

  @Override
  protected String tokenEndpoint() {
    return wireMockServer.baseUrl() + TOKEN_ENDPOINT_PATH;
  }

  @Override
  protected String authEndpoint() {
    return wireMockServer.baseUrl() + AUTH_ENDPOINT_PATH;
  }

  @BeforeAll
  void clientCredentialsStub() {
    wireMockServer.stubFor(
        WireMock.post(TOKEN_ENDPOINT_PATH)
            .withHeader("Authorization", equalTo("Basic cXVhcmt1cy1zZXJ2aWNlLWFwcDpzZWNyZXQ="))
            .withRequestBody(containing("client_credentials"))
            .willReturn(successfulResponse(VALID_TOKEN)));
  }

  @BeforeAll
  void passwordStub() {
    wireMockServer.stubFor(
        WireMock.post(TOKEN_ENDPOINT_PATH)
            .withHeader("Authorization", equalTo("Basic cXVhcmt1cy1zZXJ2aWNlLWFwcDpzZWNyZXQ="))
            .withRequestBody(containing("password"))
            .withRequestBody(containing("username=alice"))
            .withRequestBody(containing("password=alice"))
            .willReturn(successfulResponse(VALID_TOKEN)));
  }

  @BeforeAll
  void authorizationCodeStub() {
    wireMockServer.stubFor(
        WireMock.post(TOKEN_ENDPOINT_PATH)
            .withHeader("Authorization", equalTo("Basic cXVhcmt1cy1zZXJ2aWNlLWFwcDpzZWNyZXQ="))
            .withRequestBody(containing("authorization_code"))
            .withRequestBody(containing("redirect_uri"))
            .withRequestBody(containing("code=1234"))
            .willReturn(successfulResponse(VALID_TOKEN)));
  }

  @BeforeAll
  void unauthorizedStub() {
    wireMockServer.stubFor(
        WireMock.post(TOKEN_ENDPOINT_PATH)
            .withHeader("Authorization", equalTo("Basic cXVhcmt1cy1zZXJ2aWNlLWFwcDpzZWNyZXQ="))
            .withRequestBody(containing("password"))
            .withRequestBody(containing("username=alice"))
            .withRequestBody(containing("password=WRONG"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(401)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"error\":\"invalid_credentials\","
                            + "\"error_description\":\"Try Again\"}")));
  }

  @BeforeAll
  void expiredTokenStub() {
    String token =
        Jwt.preferredUserName("alice")
            .groups(ImmutableSet.of("user"))
            .issuer("https://server.example.com")
            .expiresAt(0)
            .sign();
    wireMockServer.stubFor(
        WireMock.post(TOKEN_ENDPOINT_PATH)
            .withHeader("Authorization", equalTo("Basic cXVhcmt1cy1zZXJ2aWNlLWFwcDpFWFBJUkVE"))
            .withRequestBody(containing("client_credentials"))
            .willReturn(successfulResponse(token)));
  }

  @BeforeAll
  void wrongIssuerStub() {
    String token =
        Jwt.preferredUserName("alice")
            .groups(ImmutableSet.of("user"))
            .issuer("https://WRONG.example.com")
            .sign();
    wireMockServer.stubFor(
        WireMock.post(TOKEN_ENDPOINT_PATH)
            .withHeader(
                "Authorization", equalTo("Basic cXVhcmt1cy1zZXJ2aWNlLWFwcDpXUk9OR19JU1NVRVI="))
            .withRequestBody(containing("client_credentials"))
            .willReturn(successfulResponse(token)));
  }

  @Override
  protected ResourceOwnerEmulator newResourceOwner() throws IOException {
    ResourceOwnerEmulator resourceOwner = ResourceOwnerEmulator.forAuthorizationCode();
    resourceOwner.replaceSystemOut();
    resourceOwner.setAuthUrlListener(
        url -> {
          String state = HttpUtils.parseQueryString(url.getQuery()).get("state");
          wireMockServer.stubFor(
              WireMock.get(urlPathEqualTo(AUTH_ENDPOINT_PATH))
                  .withQueryParam("response_type", equalTo("code"))
                  .withQueryParam("client_id", equalTo("quarkus-service-app"))
                  .withQueryParam("redirect_uri", containing("http"))
                  .withQueryParam("state", equalTo(state))
                  .willReturn(authorizationCodeResponse(state)));
        });
    return resourceOwner;
  }

  private static ResponseDefinitionBuilder successfulResponse(String accessToken) {
    return WireMock.aResponse()
        .withHeader("Content-Type", "application/json")
        .withBody(
            String.format(
                "{\"access_token\":\"%s\","
                    + "\"refresh_token\":\"07e08903-1263-4dd1-9fd1-4a59b0db5283\","
                    + "\"token_type\":\"bearer\","
                    + "\"expires_in\":300}",
                accessToken));
  }

  private static ResponseDefinitionBuilder authorizationCodeResponse(String state) {
    return WireMock.aResponse()
        .withHeader(
            "Location",
            "http://localhost:8989/nessie-client/auth?"
                + "state="
                + URLEncoder.encode(state, StandardCharsets.UTF_8)
                + "&code=1234")
        .withStatus(302);
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(AuthenticationEnabledProfile.AUTH_CONFIG_OVERRIDES)
          // keycloak.url defined by OidcWiremockTestResource
          .put("quarkus.oidc.auth-server-url", "${keycloak.url}/realms/quarkus/")
          .build();
    }
  }
}
