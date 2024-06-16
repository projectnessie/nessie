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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.http.Status;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(SoftAssertionsExtension.class)
public class ITOAuth2ClientAuthelia {

  private static final int NESSIE_CALLBACK_PORT;

  static {
    try (ServerSocket socket = new ServerSocket(0)) {
      NESSIE_CALLBACK_PORT = socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("resource")
  @Container
  static final GenericContainer<?> AUTHELIA =
      new GenericContainer<>(
              ContainerSpecHelper.builder()
                  .name("authelia")
                  .containerClass(ITOAuth2ClientAuthelia.class)
                  .build()
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("authelia/authelia")
                  .toString())
          .withExposedPorts(9091)
          .withEnv("X_AUTHELIA_CONFIG_FILTERS", "template")
          .withEnv("NESSIE_CALLBACK_PORT", NESSIE_CALLBACK_PORT + "")
          .withClasspathResourceMapping(
              "org/projectnessie/client/auth/oauth2/authelia-config.yaml",
              "/config/configuration.yml",
              BindMode.READ_ONLY)
          .withClasspathResourceMapping(
              "org/projectnessie/client/auth/oauth2/authelia-users.yaml",
              "/config/users.yml",
              BindMode.READ_ONLY)
          .withClasspathResourceMapping(
              "org/projectnessie/client/auth/oauth2/authelia-key.pem",
              "/config/key.pem",
              BindMode.READ_ONLY)
          .withClasspathResourceMapping(
              "org/projectnessie/client/auth/oauth2/authelia-cert.pem",
              "/config/cert.pem",
              BindMode.READ_ONLY)
          .waitingFor(Wait.forListeningPort());

  private static URI issuerUrl;

  @InjectSoftAssertions private SoftAssertions soft;

  @BeforeAll
  static void beforeAll() {
    issuerUrl = URI.create("https://127.0.0.1:" + AUTHELIA.getMappedPort(9091));
  }

  @Test
  void testOAuth2ClientCredentials() throws Exception {
    OAuth2ClientConfig config =
        clientConfig("nessie-private-cc").grantType(GrantType.CLIENT_CREDENTIALS).build();
    try (OAuth2Client client = new OAuth2Client(config)) {
      Tokens tokens = client.fetchNewTokens();
      soft.assertThat(tokens.getAccessToken()).isNotNull();
      soft.assertThat(tokens.getRefreshToken()).isNull();
    }
  }

  @Test
  @Timeout(15)
  void testOAuth2AuthorizationCode() throws Exception {
    try (InteractiveResourceOwnerEmulator resourceOwner =
        new AutheliaAuthorizationCodeResourceOwnerEmulator(
            "Alice", "s3cr3t", insecureSslContext())) {
      resourceOwner.setAuthServerBaseUri(issuerUrl);
      OAuth2ClientConfig config =
          clientConfig("nessie-private-ac")
              .grantType(GrantType.AUTHORIZATION_CODE)
              .console(resourceOwner.getConsole())
              .build();
      try (OAuth2Client client = new OAuth2Client(config)) {
        resourceOwner.setErrorListener(e -> client.close());
        Tokens tokens = client.fetchNewTokens();
        soft.assertThat(tokens.getAccessToken()).isNotNull();
        soft.assertThat(tokens.getRefreshToken()).isNotNull();
        Tokens refreshed = client.refreshTokens(tokens);
        soft.assertThat(refreshed.getAccessToken()).isNotNull();
        soft.assertThat(refreshed.getRefreshToken()).isNotNull();
      }
    }
  }

  @Test
  void testOAuth2ClientCredentialsUnauthorized() throws Exception {
    OAuth2ClientConfig config =
        clientConfig("nessie-private-cc").clientSecret("BAD SECRET").build();
    try (OAuth2Client client = new OAuth2Client(config)) {
      client.start();
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::getStatus)
          .isEqualTo(Status.UNAUTHORIZED);
    }
  }

  private static OAuth2ClientConfig.Builder clientConfig(String clientId) throws Exception {
    return OAuth2ClientConfig.builder()
        .clientId(clientId)
        .clientSecret("s3cr3t")
        // offline_access is required to get a refresh token when using authorization code flow
        // note: Authelia has issues when more than one scope is requested
        .addScope(clientId.equals("nessie-private-cc") ? "profile" : "offline_access")
        .authorizationCodeFlowWebServerPort(NESSIE_CALLBACK_PORT)
        .issuerUrl(issuerUrl)
        .sslContext(insecureSslContext());
  }

  private static SSLContext insecureSslContext() throws Exception {
    SSLContext sslContext = SSLContext.getInstance("TLS");
    TrustManager[] trustAllCerts =
        new TrustManager[] {
          new X509ExtendedTrustManager() {
            @Override
            public void checkClientTrusted(
                X509Certificate[] chain, String authType, Socket socket) {}

            @Override
            public void checkServerTrusted(
                X509Certificate[] chain, String authType, Socket socket) {}

            @Override
            public void checkClientTrusted(
                X509Certificate[] chain, String authType, SSLEngine engine) {}

            @Override
            public void checkServerTrusted(
                X509Certificate[] chain, String authType, SSLEngine engine) {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
          }
        };
    sslContext.init(null, trustAllCerts, null);
    return sslContext;
  }
}
