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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dasniko.testcontainers.keycloak.KeycloakContainer;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

  @Container
  private static final KeycloakContainer KEYCLOAK =
      new KeycloakContainer().withFeaturesEnabled("preview", "token-exchange");

  private static RealmResource master;
  private static URI tokenEndpoint;

  @InjectSoftAssertions private SoftAssertions soft;

  @BeforeAll
  static void setUpKeycloak() {
    tokenEndpoint =
        URI.create(KEYCLOAK.getAuthServerUrl() + "/realms/master/protocol/openid-connect/token");
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
   * <p>This should be enough to exercise the OAuth2 client's background refresh logic with the 4
   * supported grant types / requests:
   *
   * <ul>
   *   <li><code>client_credentials</code>
   *   <li><code>password</code>
   *   <li><code>refresh_token</code>
   *   <li><code>urn:ietf:params:oauth2:grant-type:token-exchange</code> (token exchange)
   * </ul>
   */
  @Test
  void testOAuth2ClientWithBackgroundRefresh() throws InterruptedException {
    OAuth2ClientParams params1 = clientParams("Client1").build();
    OAuth2ClientParams params2 = clientParams("Client2").grantType("password").build();
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    try (OAuth2Client client1 = new OAuth2Client(params1);
        OAuth2Client client2 = new OAuth2Client(params2);
        HttpClient validatingClient = validatingHttpClient("Client1").build()) {
      client1.start();
      client2.start();
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
      } catch (ExecutionException e) {
        soft.fail(e.getMessage());
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
   *   <li>client_credentials grant type for obtaining the access token;
   *   <li>refresh token sent on the initial response;
   *   <li>refresh_token grant type for refreshing the access token.
   * </ul>
   */
  @Test
  void testOAuth2ClientInitialRefreshToken() {
    OAuth2ClientParams params = clientParams("Client2").build();
    try (OAuth2Client client = new OAuth2Client(params);
        HttpClient validatingClient = validatingHttpClient("Client2").build()) {
      // first request: client credentials grant
      Tokens firstTokens = client.fetchNewTokens();
      soft.assertThat(firstTokens).isInstanceOf(ClientCredentialsTokensResponse.class);
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
    OAuth2ClientParams params = clientParams("Client1").build();
    try (OAuth2Client client = new OAuth2Client(params);
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
    OAuth2ClientParams params = clientParams("Client1").tokenExchangeEnabled(false).build();
    try (OAuth2Client client = new OAuth2Client(params);
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

  /**
   * Dummy sentence to make Checkstyle happy.
   *
   * <p>This test exercises the OAuth2 client with the following steps:
   *
   * <ul>
   *   <li>password grant type for obtaining the access token;
   *   <li>refresh token sent on the initial response;
   *   <li>refresh_token grant type for refreshing the access token.
   * </ul>
   */
  @Test
  void testOAuth2ClientPasswordGrant() {
    OAuth2ClientParams params = clientParams("Client2").grantType("password").build();
    try (OAuth2Client client = new OAuth2Client(params);
        HttpClient validatingClient = validatingHttpClient("Client2").build()) {
      // first request: password grant
      Tokens firstTokens = client.fetchNewTokens();
      soft.assertThat(firstTokens).isInstanceOf(PasswordTokensResponse.class);
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

  @Test
  void testOAuth2ClientUnauthorizedBadClientSecret() {
    OAuth2ClientParams params = clientParams("Client1").clientSecret("BAD SECRET").build();
    try (OAuth2Client client = new OAuth2Client(params)) {
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus)
          .isEqualTo(Status.UNAUTHORIZED);
    }
  }

  @Test
  void testOAuth2ClientUnauthorizedBadPassword() {
    OAuth2ClientParams params =
        clientParams("Client2").grantType("password").password("BAD PASSWORD").build();
    try (OAuth2Client client = new OAuth2Client(params)) {
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus)
          .isEqualTo(Status.UNAUTHORIZED);
    }
  }

  @Test
  void testOAuth2ClientExpiredToken() {
    OAuth2ClientParams params = clientParams("Client1").build();
    try (OAuth2Client client = new OAuth2Client(params);
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

  private static ImmutableOAuth2ClientParams.Builder clientParams(String clientId) {
    return ImmutableOAuth2ClientParams.builder()
        .tokenEndpoint(tokenEndpoint)
        .clientId(clientId)
        .clientSecret("s3cr3t")
        .username("Alice")
        .password("s3cr3t")
        .defaultAccessTokenLifespan(Duration.ofSeconds(10))
        .defaultRefreshTokenLifespan(Duration.ofSeconds(15))
        .refreshSafetyWindow(Duration.ofSeconds(5));
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
    client.setAuthorizationServicesEnabled(true); // required to request UMA tokens
    client.setPublicClient(false);
    client.setAttributes(
        ImmutableMap.of(
            "client_credentials.use_refresh_token",
            String.valueOf(sendRefreshTokenOnClientCredentialsRequest)));
    ResourceServerRepresentation settings = new ResourceServerRepresentation();
    settings.setPolicyEnforcementMode(PolicyEnforcementMode.DISABLED);
    client.setAuthorizationSettings(settings);
    master.clients().create(client);
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
    master.users().create(user);
  }

  private static HttpClient.Builder validatingHttpClient(String clientId) {
    @SuppressWarnings("resource")
    HttpAuthentication authentication = BasicAuthenticationProvider.create(clientId, "s3cr3t");
    HttpClient.Builder builder =
        HttpClient.builder()
            .setBaseUri(URI.create(KEYCLOAK.getAuthServerUrl()))
            .setObjectMapper(new ObjectMapper())
            .setDisableCompression(true);
    authentication.applyToHttpClient(builder);
    return builder;
  }
}
