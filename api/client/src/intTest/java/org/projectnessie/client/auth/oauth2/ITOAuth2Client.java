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

import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import dasniko.testcontainers.keycloak.KeycloakContainer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.authorization.PolicyEnforcementMode;
import org.keycloak.representations.idm.authorization.ResourceServerRepresentation;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(SoftAssertionsExtension.class)
public class ITOAuth2Client {

  @Container
  private static final KeycloakContainer KEYCLOAK =
      new KeycloakContainer().withFeaturesEnabled("preview", "token-exchange");

  private static final Logger LOGGER = LoggerFactory.getLogger(ITOAuth2Client.class);

  private static RealmResource master;
  private static URI tokenEndpoint;

  @InjectSoftAssertions private SoftAssertions soft;

  @BeforeAll
  static void setUpKeycloak() {
    Keycloak keycloakAdmin = KEYCLOAK.getKeycloakAdminClient();
    master = keycloakAdmin.realms().realm("master");
    createClient("ResourceServer", false);
    tokenEndpoint =
        URI.create(KEYCLOAK.getAuthServerUrl() + "/realms/master/protocol/openid-connect/token");
  }

  /**
   * This test exercises the OAuth2 client with different combinations of access token and refresh
   * token lifespans and with/without sending the refresh token on the initial request.
   *
   * <p>For 15 seconds, the OAuth2 client will strive to keep the access token valid; in the
   * meantime, another HTTP client, acting as a typical resource server, will attempt to use the
   * access token to obtain a UMA (User Management Access) ticket from Keycloak, symbolizing the
   * fact that Keycloak authorizes the client to access its resources.
   */
  @ParameterizedTest
  @MethodSource
  void testOAuth2Client(
      Duration accessTokenLifespan,
      Duration refreshTokenLifespan,
      boolean sendRefreshTokenOnInitialRequest,
      boolean tokenExchangeEnabled)
      throws InterruptedException {

    String id = "OAuth2Client" + System.nanoTime();

    updateMasterRealm(accessTokenLifespan, refreshTokenLifespan);
    createClient(id, sendRefreshTokenOnInitialRequest);

    OAuth2ClientParams params =
        getOAuth2ClientParams(id, accessTokenLifespan, refreshTokenLifespan, tokenExchangeEnabled);

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    try (OAuth2Client client = new OAuth2Client(params);
        HttpClient validatingClient = getValidatingHttpClient().build()) {

      client.start();

      ScheduledFuture<?> future =
          executor.scheduleWithFixedDelay(
              () -> {
                Tokens tokens = client.getCurrentTokens();
                soft.assertThat(tokens).isNotNull();
                tryUseAccessToken(validatingClient, tokens.getAccessToken());
                if (LOGGER.isDebugEnabled()) {
                  printTokens(tokens);
                }
              },
              0,
              1,
              TimeUnit.SECONDS);

      try {
        future.get(15, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // ok, expected for a ScheduledFuture
      } catch (ExecutionException e) {
        soft.fail(e.getMessage());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  static Stream<Arguments> testOAuth2Client() {
    Duration small = Duration.ofSeconds(8);
    Duration large = Duration.ofMinutes(30);
    return Stream.of(
        // Short access token and long refresh token
        Arguments.of(small, large, true, true),
        Arguments.of(small, large, false, true),
        Arguments.of(small, large, false, false),
        // Short access token and short refresh token
        Arguments.of(small, small, true, true),
        Arguments.of(small, small, false, true),
        Arguments.of(small, small, false, false),
        // Long access token and short refresh token
        Arguments.of(large, small, true, true),
        Arguments.of(large, small, false, true),
        Arguments.of(large, small, false, false));
  }

  @Test
  void testOAuth2ClientUnauthorized() {

    String id = "OAuth2Client" + System.nanoTime();
    createClient(id, false);

    OAuth2ClientParams params =
        ImmutableOAuth2ClientParams.builder()
            .tokenEndpoint(tokenEndpoint)
            .clientId(id)
            .clientSecret("BAD SECRET".getBytes(StandardCharsets.UTF_8))
            .build();

    try (OAuth2Client client = new OAuth2Client(params)) {
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus)
          .isEqualTo(Status.UNAUTHORIZED);
    }
  }

  private void tryUseAccessToken(HttpClient validatingClient, AccessToken accessToken) {
    HttpRequest request =
        validatingClient
            .newRequest()
            .path("realms/master/protocol/openid-connect/token")
            .header("Authorization", "Bearer " + accessToken.getPayload());
    HttpResponse response =
        request.postForm(
            ImmutableMap.of(
                "grant_type",
                "urn:ietf:params:oauth:grant-type:uma-ticket",
                // audience: client ID of the resource server
                "audience",
                "ResourceServer"));
    JsonNode entity = response.readEntity(JsonNode.class);
    soft.assertThat(entity).isNotNull();
    // should contain the Requesting Party Token (RPT) under access_token
    soft.assertThat(entity.has("access_token")).isTrue();
    JwtToken jwtToken = JwtToken.parse(entity.get("access_token").asText());
    soft.assertThat(jwtToken).isNotNull();
    soft.assertThat(jwtToken.getAudience()).isEqualTo("ResourceServer");
    // The authorization claim is added by the Keycloak UMA policy enforcer
    soft.assertThat(jwtToken.getPayload().has("authorization")).isTrue();
  }

  private static ImmutableOAuth2ClientParams getOAuth2ClientParams(
      String clientId,
      Duration accessTokenLifespan,
      Duration refreshTokenLifespan,
      boolean tokenExchangeEnabled) {
    return ImmutableOAuth2ClientParams.builder()
        .tokenEndpoint(tokenEndpoint)
        .clientId(clientId)
        .clientSecret("s3cr3t".getBytes(StandardCharsets.UTF_8))
        .defaultAccessTokenLifespan(accessTokenLifespan)
        .defaultRefreshTokenLifespan(refreshTokenLifespan)
        .tokenExchangeEnabled(tokenExchangeEnabled)
        .refreshSafetyWindow(Duration.ofSeconds(3))
        .build();
  }

  private static void updateMasterRealm(
      Duration accessTokenLifespan, Duration refreshTokenLifespan) {
    RealmRepresentation masterRep = master.toRepresentation();
    masterRep.setAccessTokenLifespan((int) accessTokenLifespan.getSeconds());
    // Refresh token lifespan will be equal to the smallest value between:
    // SSO Session Idle, SSO Session Max, Client Session Idle, and Client Session Max.
    masterRep.setClientSessionIdleTimeout((int) refreshTokenLifespan.getSeconds());
    masterRep.setClientSessionMaxLifespan((int) refreshTokenLifespan.getSeconds());
    masterRep.setSsoSessionIdleTimeout((int) refreshTokenLifespan.getSeconds());
    masterRep.setSsoSessionMaxLifespan((int) refreshTokenLifespan.getSeconds());
    master.update(masterRep);
  }

  @SuppressWarnings("resource")
  private static void createClient(String id, boolean sendRefreshTokenOnInitialRequest) {
    ClientRepresentation client = new ClientRepresentation();
    client.setId(id); // internal ID
    client.setClientId(id); // client ID is what applications need to use to authenticate
    client.setSecret("s3cr3t");
    client.setServiceAccountsEnabled(true);
    client.setAuthorizationServicesEnabled(true); // required to request UMA tokens
    client.setPublicClient(false);
    client.setAttributes(
        ImmutableMap.of(
            "client_credentials.use_refresh_token",
            String.valueOf(sendRefreshTokenOnInitialRequest)));
    ResourceServerRepresentation settings = new ResourceServerRepresentation();
    settings.setPolicyEnforcementMode(PolicyEnforcementMode.DISABLED);
    client.setAuthorizationSettings(settings);
    master.clients().create(client);
  }

  private static HttpClient.Builder getValidatingHttpClient() {
    return HttpClient.builder()
        .setBaseUri(URI.create(KEYCLOAK.getAuthServerUrl()))
        .setObjectMapper(new ObjectMapper())
        .setDisableCompression(true);
  }

  private static void printTokens(Tokens tokens) {
    printToken("access ", tokens.getAccessToken());
    printToken("refresh", tokens.getRefreshToken());
    Map<String, Object> params = ((TokensResponseBase) tokens).getExtraParameters();
    LOGGER.debug("extra params: {}", params);
  }

  private static void printToken(String qualifier, Token token) {
    String exp =
        token != null && token.getExpirationTime() != null
            ? Duration.between(Instant.now(), token.getExpirationTime()).toString()
            : "?";
    LOGGER.debug("{} token expires in: {}", qualifier, exp);
  }
}
