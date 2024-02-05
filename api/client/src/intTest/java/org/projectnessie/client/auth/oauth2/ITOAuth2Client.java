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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.client.auth.oauth2.GrantType.AUTHORIZATION_CODE;
import static org.projectnessie.client.auth.oauth2.GrantType.DEVICE_CODE;
import static org.projectnessie.client.auth.oauth2.GrantType.PASSWORD;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dasniko.testcontainers.keycloak.KeycloakContainer;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.keycloak.representations.idm.authorization.PolicyEnforcementMode;
import org.keycloak.representations.idm.authorization.ResourceServerRepresentation;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.Status;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(SoftAssertionsExtension.class)
public class ITOAuth2Client {

  public static final String IMAGE_TAG;

  static {
    URL resource = ITOAuth2Client.class.getResource("Dockerfile-keycloak-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          IOUtils.readLines(in, UTF_8).stream()
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow(IllegalArgumentException::new);
      IMAGE_TAG = imageTag[0] + ':' + imageTag[1];
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }

  @Container
  private static final KeycloakContainer KEYCLOAK =
      new KeycloakContainer(IMAGE_TAG).withFeaturesEnabled("preview", "token-exchange")
      // Useful when debugging Keycloak REST endpoints:
      // .withEnv("QUARKUS_HTTP_ACCESS_LOG_ENABLED", "true")
      // .withEnv("QUARKUS_HTTP_ACCESS_LOG_PATTERN", "long")
      // .withEnv("KC_LOG_LEVEL", "INFO,org.keycloak:DEBUG")
      ;

  private static RealmResource master;
  private static URI issuerUrl;
  private static URI tokenEndpoint;
  private static URI authEndpoint;
  private static URI deviceAuthEndpoint;

  @InjectSoftAssertions private SoftAssertions soft;

  @BeforeAll
  static void setUpKeycloak() {
    issuerUrl = URI.create(KEYCLOAK.getAuthServerUrl() + "/realms/master/");
    tokenEndpoint = issuerUrl.resolve("protocol/openid-connect/token");
    authEndpoint = issuerUrl.resolve("protocol/openid-connect/auth");
    deviceAuthEndpoint = issuerUrl.resolve("protocol/openid-connect/auth/device");
    Keycloak keycloakAdmin = KEYCLOAK.getKeycloakAdminClient();
    master = keycloakAdmin.realms().realm("master");
    updateMasterRealm(10, 15);
    // Create 2 clients, one sending refresh tokens for client_credentials, the other one not
    createClient("Client1", false);
    createClient("Client2", true);
    // Create a client that will act as a resource server attempting to validate access tokens
    createClient("ResourceServer", false);
    // Create a user that will be used to obtain access tokens via password grant
    createUser();
  }

  /**
   * This test exercises the OAuth2 client "in real life", that is, with background token refresh
   * running.
   *
   * <p>For 20 seconds, 2 OAuth2 clients will strive to keep the access tokens valid; in the
   * meantime, another HTTP client will attempt to validate the obtained tokens.
   *
   * <p>This should be enough to exercise the OAuth2 client's background refresh logic with all
   * supported grant types / requests:
   *
   * <ul>
   *   <li><code>client_credentials</code>
   *   <li><code>password</code>
   *   <li><code>authorization_code</code>
   *   <li><code>refresh_token</code>
   *   <li><code>urn:ietf:params:oauth2:grant-type:token-exchange</code> (token exchange)
   * </ul>
   */
  @Test
  void testOAuth2ClientWithBackgroundRefresh() throws Exception {
    OAuth2ClientConfig config1 = clientConfig("Client1", false).build();
    OAuth2ClientConfig config2 = clientConfig("Client2", false).grantType(PASSWORD).build();
    OAuth2ClientConfig config3 =
        clientConfig("Client2", false).grantType(AUTHORIZATION_CODE).build();
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    try (OAuth2Client client1 = new OAuth2Client(config1);
        OAuth2Client client2 = new OAuth2Client(config2);
        OAuth2Client client3 = new OAuth2Client(config3);
        ResourceOwnerEmulator resourceOwner =
            new ResourceOwnerEmulator(AUTHORIZATION_CODE, "Alice", "s3cr3t");
        HttpClient validatingClient = validatingHttpClient("Client1").build()) {
      resourceOwner.replaceSystemOut();
      resourceOwner.setAuthServerBaseUri(URI.create(KEYCLOAK.getAuthServerUrl()));
      resourceOwner.setErrorListener(e -> executor.shutdownNow());
      client1.start();
      client2.start();
      client3.start();
      ScheduledFuture<?> future =
          executor.scheduleWithFixedDelay(
              () -> {
                tryUseAccessToken(validatingClient, client1.getCurrentTokens().getAccessToken());
                tryUseAccessToken(validatingClient, client2.getCurrentTokens().getAccessToken());
              },
              0,
              1,
              TimeUnit.SECONDS);
      try {
        future.get(20, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // ok, expected for a ScheduledFuture
      } catch (CancellationException | ExecutionException e) {
        soft.fail(e.getMessage(), e);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Dummy sentence to make Checkstyle happy.
   *
   * <p>This test exercises the OAuth2 client with the following steps:
   *
   * <ul>
   *   <li>endpoint discovery on;
   *   <li>client_credentials, password, authorization_code or device_code grant type for obtaining
   *       the initial access token;
   *   <li>refresh token sent on the initial response;
   *   <li>refresh_token grant type for refreshing the access token.
   * </ul>
   */
  @ParameterizedTest
  @EnumSource(
      value = GrantType.class,
      names = {"CLIENT_CREDENTIALS", "PASSWORD", "AUTHORIZATION_CODE", "DEVICE_CODE"})
  void testOAuth2ClientInitialRefreshToken(GrantType initialGrantType) throws Exception {
    OAuth2ClientConfig config = clientConfig("Client2", true).grantType(initialGrantType).build();
    try (OAuth2Client client = new OAuth2Client(config);
        AutoCloseable ignored = newTestSetup(initialGrantType, client);
        HttpClient validatingClient = validatingHttpClient("Client2").build()) {
      // first request: client credentials grant
      Tokens firstTokens = client.fetchNewTokens();
      soft.assertThat(firstTokens).isInstanceOf(expectedResponseClass(initialGrantType));
      soft.assertThat(firstTokens.getRefreshToken()).isNotNull();
      tryUseAccessToken(validatingClient, firstTokens.getAccessToken());
      // second request: refresh token grant
      Tokens refreshedTokens = client.refreshTokens(firstTokens);
      soft.assertThat(refreshedTokens).isInstanceOf(RefreshTokensResponse.class);
      soft.assertThat(refreshedTokens.getRefreshToken()).isNotNull();
      tryUseAccessToken(validatingClient, refreshedTokens.getAccessToken());
      compareTokens(firstTokens, refreshedTokens, "Client2");
    }
  }

  /**
   * Dummy sentence to make Checkstyle happy.
   *
   * <p>This test exercises the OAuth2 client with the following steps:
   *
   * <ul>
   *   <li>client_credentials grant type for obtaining the access token;
   *   <li>no refresh token sent on the initial response;
   *   <li>token exchange for obtaining the refresh token;
   *   <li>refresh_token grant type for refreshing the access token.
   * </ul>
   */
  @Test
  void testOAuth2ClientTokenExchange() {
    OAuth2ClientConfig config = clientConfig("Client1", false).build();
    try (OAuth2Client client = new OAuth2Client(config);
        HttpClient validatingClient = validatingHttpClient("Client1").build()) {
      // first request: client credentials grant
      Tokens firstTokens = client.fetchNewTokens();
      soft.assertThat(firstTokens).isInstanceOf(ClientCredentialsTokensResponse.class);
      soft.assertThat(firstTokens.getRefreshToken()).isNull();
      tryUseAccessToken(validatingClient, firstTokens.getAccessToken());
      // second request: token exchange since no refresh token was sent
      Tokens exchangedTokens = client.refreshTokens(firstTokens);
      soft.assertThat(exchangedTokens).isInstanceOf(TokensExchangeResponse.class);
      soft.assertThat(exchangedTokens.getRefreshToken()).isNotNull();
      tryUseAccessToken(validatingClient, exchangedTokens.getAccessToken());
      compareTokens(firstTokens, exchangedTokens, "Client1");
      // third request: refresh token grant
      Tokens refreshedTokens = client.refreshTokens(exchangedTokens);
      soft.assertThat(refreshedTokens).isInstanceOf(RefreshTokensResponse.class);
      soft.assertThat(refreshedTokens.getRefreshToken()).isNotNull();
      tryUseAccessToken(validatingClient, refreshedTokens.getAccessToken());
      compareTokens(exchangedTokens, refreshedTokens, "Client1");
    }
  }

  /**
   * Dummy sentence to make Checkstyle happy.
   *
   * <p>This test exercises the OAuth2 client with the following steps:
   *
   * <ul>
   *   <li>client_credentials grant type for obtaining the access token;
   *   <li>no refresh token sent on the initial response;
   *   <li>no token exchange for obtaining the refresh token;
   *   <li>another client credentials grant for refreshing the access token.
   * </ul>
   */
  @Test
  void testOAuth2ClientNoRefreshToken() {
    OAuth2ClientConfig config = clientConfig("Client1", false).tokenExchangeEnabled(false).build();
    try (OAuth2Client client = new OAuth2Client(config);
        HttpClient validatingClient = validatingHttpClient("Client1").build()) {
      // first request: client credentials grant
      Tokens firstTokens = client.fetchNewTokens();
      soft.assertThat(firstTokens).isInstanceOf(ClientCredentialsTokensResponse.class);
      soft.assertThat(firstTokens.getRefreshToken()).isNull();
      tryUseAccessToken(validatingClient, firstTokens.getAccessToken());
      // second request: another client credentials grant since no refresh token was sent
      // and token exchange is disabled – cannot call refreshTokens() tokens here
      soft.assertThatThrownBy(() -> client.refreshTokens(firstTokens))
          .isInstanceOf(OAuth2Client.MustFetchNewTokensException.class);
      Tokens nextTokens = client.fetchNewTokens();
      soft.assertThat(nextTokens).isInstanceOf(ClientCredentialsTokensResponse.class);
      soft.assertThat(nextTokens.getRefreshToken()).isNull();
      tryUseAccessToken(validatingClient, nextTokens.getAccessToken());
      compareTokens(firstTokens, nextTokens, "Client1");
    }
  }

  @Test
  void testOAuth2ClientUnauthorizedBadClientSecret() {
    OAuth2ClientConfig config = clientConfig("Client1", false).clientSecret("BAD SECRET").build();
    try (OAuth2Client client = new OAuth2Client(config)) {
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus)
          .isEqualTo(Status.UNAUTHORIZED);
    }
  }

  @Test
  void testOAuth2ClientUnauthorizedBadPassword() {
    OAuth2ClientConfig config =
        clientConfig("Client2", false).grantType(PASSWORD).password("BAD PASSWORD").build();
    try (OAuth2Client client = new OAuth2Client(config)) {
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus)
          .isEqualTo(Status.UNAUTHORIZED);
    }
  }

  @Test
  void testOAuth2ClientUnauthorizedBadAuthorizationCode() throws Exception {
    OAuth2ClientConfig config =
        clientConfig("Client2", false).grantType(AUTHORIZATION_CODE).build();
    try (OAuth2Client client = new OAuth2Client(config);
        ResourceOwnerEmulator resourceOwner =
            new ResourceOwnerEmulator(AUTHORIZATION_CODE, "Alice", "s3cr3t")) {
      resourceOwner.replaceSystemOut();
      resourceOwner.setAuthServerBaseUri(URI.create(KEYCLOAK.getAuthServerUrl()));
      resourceOwner.setErrorListener(e -> client.close());
      resourceOwner.overrideAuthorizationCode("BAD_CODE", Status.UNAUTHORIZED);
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus)
          .isEqualTo(Status.BAD_REQUEST); // Keycloak replies with 400 instead of 401
    }
  }

  @Test
  void testOAuth2ClientDeviceCodeAccessDenied() throws Exception {
    OAuth2ClientConfig config = clientConfig("Client2", false).grantType(DEVICE_CODE).build();
    try (OAuth2Client client = new OAuth2Client(config);
        ResourceOwnerEmulator resourceOwner =
            new ResourceOwnerEmulator(DEVICE_CODE, "Alice", "s3cr3t")) {
      resourceOwner.replaceSystemOut();
      resourceOwner.setAuthServerBaseUri(URI.create(KEYCLOAK.getAuthServerUrl()));
      resourceOwner.setErrorListener(e -> client.close());
      resourceOwner.denyConsent();
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus, OAuth2Exception::getErrorCode)
          .containsExactly(
              Status.BAD_REQUEST, "access_denied"); // Keycloak replies with 400 instead of 401
    }
  }

  @Test
  void testOAuth2ClientExpiredToken() {
    OAuth2ClientConfig config = clientConfig("Client1", false).build();
    try (OAuth2Client client = new OAuth2Client(config);
        HttpClient validatingClient = validatingHttpClient("Client1").build()) {
      Tokens tokens = client.fetchNewTokens();
      // Emulate a token expiration; we don't want to wait 10 seconds just for the token to really
      // expire.
      revokeAccessToken(validatingClient, tokens.getAccessToken());
      soft.assertThatThrownBy(() -> tryUseAccessToken(validatingClient, tokens.getAccessToken()))
          .isInstanceOf(HttpClientException.class)
          .hasMessageContaining("401");
    }
  }

  /**
   * Attempts to use the access token to obtain a UMA (User Management Access) ticket from Keycloak,
   * authorizing the client represented by the token to access resources hosted by "ResourceServer".
   */
  private void tryUseAccessToken(HttpClient httpClient, AccessToken accessToken) {
    HttpRequest request =
        httpClient
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
  }

  private void revokeAccessToken(HttpClient httpClient, AccessToken accessToken) {
    HttpRequest request =
        httpClient.newRequest().path("realms/master/protocol/openid-connect/revoke");
    request.postForm(ImmutableMap.of("token", accessToken.getPayload()));
  }

  private void compareTokens(Tokens oldTokens, Tokens newTokens, String clientId) {
    JwtToken oldToken = JwtToken.parse(oldTokens.getAccessToken().getPayload());
    JwtToken newToken = JwtToken.parse(newTokens.getAccessToken().getPayload());
    soft.assertThat(newToken.getSubject()).isEqualTo(oldToken.getSubject());
    // azp: Authorized party - the party to which the ID Token was issued (OIDC-specific claim)
    soft.assertThat(oldToken.getPayload().get("azp").asText())
        .isEqualTo(newToken.getPayload().get("azp").asText())
        .isEqualTo(clientId);
  }

  private static OAuth2ClientConfig.Builder clientConfig(String clientId, boolean discovery) {
    OAuth2ClientConfig.Builder builder =
        OAuth2ClientConfig.builder()
            .clientId(clientId)
            .clientSecret("s3cr3t")
            .username("Alice")
            .password("s3cr3t")
            // Otherwise Keycloak complains about missing scope, but still issues tokens
            .scope("openid")
            .defaultAccessTokenLifespan(Duration.ofSeconds(10))
            .defaultRefreshTokenLifespan(Duration.ofSeconds(15))
            .refreshSafetyWindow(Duration.ofSeconds(5))
            // Exercise the code path where Keycloak will request client to slow down
            .ignoreDeviceCodeFlowServerPollInterval(true)
            .minDeviceCodeFlowPollInterval(Duration.ofSeconds(1))
            .deviceCodeFlowPollInterval(Duration.ofSeconds(1));
    if (discovery) {
      builder.issuerUrl(issuerUrl);
    } else {
      builder
          .tokenEndpoint(tokenEndpoint)
          .authEndpoint(authEndpoint)
          .deviceAuthEndpoint(deviceAuthEndpoint);
    }
    return builder;
  }

  @SuppressWarnings("SameParameterValue")
  private static void updateMasterRealm(
      int accessTokenLifespanSeconds, int refreshTokenLifespanSeconds) {
    RealmRepresentation masterRep = master.toRepresentation();
    masterRep.setAccessTokenLifespan(accessTokenLifespanSeconds);
    // Refresh token lifespan will be equal to the smallest value between:
    // SSO Session Idle, SSO Session Max, Client Session Idle, and Client Session Max.
    masterRep.setClientSessionIdleTimeout(refreshTokenLifespanSeconds);
    masterRep.setClientSessionMaxLifespan(refreshTokenLifespanSeconds);
    masterRep.setSsoSessionIdleTimeout(refreshTokenLifespanSeconds);
    masterRep.setSsoSessionMaxLifespan(refreshTokenLifespanSeconds);
    master.update(masterRep);
  }

  @SuppressWarnings("resource")
  private static void createClient(String id, boolean sendRefreshTokenOnClientCredentialsRequest) {
    ClientRepresentation client = new ClientRepresentation();
    client.setId(id); // internal ID
    client.setClientId(id); // client ID is what applications need to use to authenticate
    client.setSecret("s3cr3t");
    client.setServiceAccountsEnabled(true); // required for client credentials grant
    client.setDirectAccessGrantsEnabled(true); // required for password grant
    client.setStandardFlowEnabled(true); // required for authorization code grant
    client.setRedirectUris(ImmutableList.of("http://localhost:*"));
    client.setAuthorizationServicesEnabled(true); // required to request UMA tokens
    client.setPublicClient(false);
    client.setAttributes(
        ImmutableMap.of(
            "client_credentials.use_refresh_token",
            String.valueOf(sendRefreshTokenOnClientCredentialsRequest),
            "oauth2.device.authorization.grant.enabled",
            "true"));
    ResourceServerRepresentation settings = new ResourceServerRepresentation();
    settings.setPolicyEnforcementMode(PolicyEnforcementMode.DISABLED);
    client.setAuthorizationSettings(settings);
    Response response = master.clients().create(client);
    assertThat(response.getStatus()).isEqualTo(201);
  }

  @SuppressWarnings("resource")
  private static void createUser() {
    UserRepresentation user = new UserRepresentation();
    user.setUsername("Alice");
    CredentialRepresentation credential = new CredentialRepresentation();
    credential.setType(CredentialRepresentation.PASSWORD);
    credential.setValue("s3cr3t");
    credential.setTemporary(false);
    user.setCredentials(ImmutableList.of(credential));
    user.setEnabled(true);
    Response response = master.users().create(user);
    assertThat(response.getStatus()).isEqualTo(201);
  }

  private static HttpClient.Builder validatingHttpClient(String clientId) {
    HttpAuthentication authentication = BasicAuthenticationProvider.create(clientId, "s3cr3t");
    return HttpClient.builder()
        .setBaseUri(URI.create(KEYCLOAK.getAuthServerUrl()))
        .setObjectMapper(new ObjectMapper())
        .setDisableCompression(true)
        .setAuthentication(authentication);
  }

  private static Class<?> expectedResponseClass(GrantType initialGrantType) {
    switch (initialGrantType) {
      case CLIENT_CREDENTIALS:
        return ClientCredentialsTokensResponse.class;
      case PASSWORD:
        return PasswordTokensResponse.class;
      case AUTHORIZATION_CODE:
        return AuthorizationCodeTokensResponse.class;
      case DEVICE_CODE:
        return DeviceCodeTokensResponse.class;
      default:
        throw new IllegalArgumentException("Unexpected initial grant type: " + initialGrantType);
    }
  }

  private AutoCloseable newTestSetup(GrantType initialGrantType, OAuth2Client client)
      throws IOException {
    switch (initialGrantType) {
      case CLIENT_CREDENTIALS:
      case PASSWORD:
        return () -> {};
      case AUTHORIZATION_CODE:
      case DEVICE_CODE:
        ResourceOwnerEmulator resourceOwner =
            new ResourceOwnerEmulator(initialGrantType, "Alice", "s3cr3t");
        resourceOwner.replaceSystemOut();
        resourceOwner.setErrorListener(e -> client.close());
        resourceOwner.setAuthServerBaseUri(URI.create(KEYCLOAK.getAuthServerUrl()));
        return resourceOwner;
      default:
        throw new IllegalArgumentException("Unexpected initial grant type: " + initialGrantType);
    }
  }
}
