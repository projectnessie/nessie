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
package org.projectnessie.testing.keycloak;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import dasniko.testcontainers.keycloak.ExtendableKeycloakContainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.RolesRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class CustomKeycloakContainer extends ExtendableKeycloakContainer<CustomKeycloakContainer> {
  public static final String KEYCLOAK_REALM = "keycloak.realm";
  public static final String KEYCLOAK_USE_HTTPS = "keycloak.use.https";
  public static final String KEYCLOAK_DOCKER_IMAGE = "keycloak.docker.image";
  public static final String KEYCLOAK_DOCKER_TAG = "keycloak.docker.tag";
  public static final String KEYCLOAK_DOCKER_NETWORK_ID = "keycloak.docker.network.id";
  public static final String FEATURES_ENABLED = "keycloak.features.enabled";
  public static final String CLIENT_SECRET = "secret";

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomKeycloakContainer.class);

  private static final int KEYCLOAK_PORT_HTTP = 8080;
  private static final int KEYCLOAK_PORT_HTTPS = 8443;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  private final KeycloakConfig config;

  public static KeycloakConfig.Builder builder() {
    return ImmutableKeycloakConfig.builder();
  }

  public static ClientRepresentation createServiceClient(
      String clientId, List<String> clientScopes) {
    ClientRepresentation client = new ClientRepresentation();

    client.setClientId(clientId);
    client.setPublicClient(false);
    client.setSecret(CLIENT_SECRET);
    client.setDirectAccessGrantsEnabled(true);
    client.setServiceAccountsEnabled(true);
    client.setDefaultClientScopes(clientScopes);

    // required for authorization code grant
    client.setStandardFlowEnabled(true);
    client.setRedirectUris(List.of("http://localhost:*"));

    // required for device code and token exchange grants
    client.setAttributes(
        Map.of(
            "oauth2.device.authorization.grant.enabled",
            "true",
            "standard.token.exchange.enabled",
            "true"));

    client.setEnabled(true);

    return client;
  }

  public static ClientRepresentation createWebAppClient(String clientId) {
    ClientRepresentation client = new ClientRepresentation();

    client.setClientId(clientId);
    client.setPublicClient(false);
    client.setSecret(CLIENT_SECRET);
    client.setRedirectUris(Collections.singletonList("*"));
    client.setEnabled(true);

    return client;
  }

  public static UserRepresentation createUser(String username, List<String> realmRoles) {
    UserRepresentation user = new UserRepresentation();

    user.setUsername(username);
    user.setFirstName(username);
    user.setLastName(username);
    user.setEnabled(true);
    user.setCredentials(new ArrayList<>());
    user.setRealmRoles(realmRoles);
    user.setEmail(username + "@gmail.com");
    user.setEmailVerified(true);
    user.setRequiredActions(Collections.emptyList());

    CredentialRepresentation credential = new CredentialRepresentation();

    credential.setType(CredentialRepresentation.PASSWORD);
    credential.setValue(username);
    credential.setTemporary(false);

    user.getCredentials().add(credential);

    return user;
  }

  @Value.Immutable
  public abstract static class KeycloakConfig {

    @Nullable
    public abstract String dockerImage();

    @Nullable
    public abstract String dockerTag();

    @Value.Derived
    public DockerImageName dockerImageName() {
      String imageTag = dockerImage() != null ? dockerImage() + ":" + dockerTag() : null;
      return ContainerSpecHelper.builder()
          .name("keycloak")
          .containerClass(KeycloakConfig.class)
          .build()
          .dockerImageName(imageTag);
    }

    @Value.Default
    public String realmName() {
      return "quarkus";
    }

    @Value.Default
    public boolean useHttps() {
      return false;
    }

    @Value.Default
    public List<String> featuresEnabled() {
      return List.of();
    }

    @Nullable
    public abstract String dockerNetworkId();

    @Value.Default
    public Consumer<RealmRepresentation> realmConfigure() {
      return r -> {};
    }

    public interface Builder {
      @CanIgnoreReturnValue
      Builder dockerImage(String dockerImage);

      @CanIgnoreReturnValue
      Builder dockerTag(String dockerTag);

      @CanIgnoreReturnValue
      Builder dockerNetworkId(String dockerNetworkId);

      @CanIgnoreReturnValue
      Builder realmName(String realmName);

      @CanIgnoreReturnValue
      Builder useHttps(boolean useHttps);

      @CanIgnoreReturnValue
      Builder addFeaturesEnabled(String element);

      @CanIgnoreReturnValue
      Builder addFeaturesEnabled(String... elements);

      @CanIgnoreReturnValue
      Builder featuresEnabled(Iterable<String> elements);

      @CanIgnoreReturnValue
      Builder realmConfigure(Consumer<RealmRepresentation> realmConfigure);

      KeycloakConfig build();

      default Builder fromProperties(Map<String, String> initArgs) {
        initArgs(initArgs, KEYCLOAK_REALM, this::realmName);
        initArgs(initArgs, KEYCLOAK_USE_HTTPS, s -> useHttps(Boolean.parseBoolean(s)));
        initArgs(initArgs, KEYCLOAK_DOCKER_IMAGE, this::dockerImage);
        initArgs(initArgs, KEYCLOAK_DOCKER_TAG, this::dockerTag);
        initArgs(initArgs, KEYCLOAK_DOCKER_NETWORK_ID, this::dockerNetworkId);
        initArgs(initArgs, FEATURES_ENABLED, s -> addFeaturesEnabled(s.split(",")));
        return this;
      }
    }

    public CustomKeycloakContainer createContainer() {
      LOGGER.info("Using Keycloak image {}", dockerImageName());
      return new CustomKeycloakContainer(this);
    }
  }

  private static void initArgs(
      Map<String, String> initArgs, String property, Consumer<String> consumer) {
    String value = initArgs.getOrDefault(property, System.getProperty(property));
    if (value != null) {
      consumer.accept(value);
    }
  }

  @SuppressWarnings("resource")
  public CustomKeycloakContainer(KeycloakConfig config) {
    super(config.dockerImageName().toString());

    this.config = config;

    withNetworkAliases("keycloak");
    if (!config.featuresEnabled().isEmpty()) {
      withFeaturesEnabled(config.featuresEnabled().toArray(new String[0]));
    }
    withStartupAttempts(3);

    // Don't use withNetworkMode, or aliases won't work!
    // See https://github.com/testcontainers/testcontainers-java/issues/1221
    String containerNetworkId = config.dockerNetworkId();
    if (containerNetworkId != null) {
      withNetwork(new ExternalNetwork(containerNetworkId));
    }

    if (config.useHttps()) {
      LOGGER.info("Enabling TLS for Keycloak");
      useTls();
    }
  }

  @Override
  public void start() {
    LOGGER.info("Starting Keycloak...");
    super.start();
    LOGGER.info("Keycloak started, creating realm {}...", config.realmName());

    RealmRepresentation realm = createRealm();
    retry(() -> createRealm(realm));

    LOGGER.info("Finished setting up Keycloak, external realm auth url: {}", getExternalRealmUri());
  }

  public String createBearerToken(String realm, String clientId, String clientSecret) {
    try {
      return token(
          keycloakUri("realms/%s/protocol/openid-connect/token".formatted(realm)),
          Map.of(
              "grant_type", "client_credentials",
              "client_id", clientId,
              "client_secret", clientSecret));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private RealmRepresentation createRealm() {
    RealmRepresentation realm = new RealmRepresentation();

    realm.setRealm(config.realmName());
    realm.setEnabled(true);
    realm.setUsers(new ArrayList<>());
    realm.setClients(new ArrayList<>());

    realm.setAccessTokenLifespan(60); // 1 minute

    // Refresh token lifespan will be equal to the smallest value between:
    // SSO Session Idle, SSO Session Max, Client Session Idle, and Client Session Max.
    int refreshTokenLifespanSeconds = 60 * 5; // 5 minutes
    realm.setClientSessionIdleTimeout(refreshTokenLifespanSeconds);
    realm.setClientSessionMaxLifespan(refreshTokenLifespanSeconds);
    realm.setSsoSessionIdleTimeout(refreshTokenLifespanSeconds);
    realm.setSsoSessionMaxLifespan(refreshTokenLifespanSeconds);

    RolesRepresentation roles = new RolesRepresentation();
    List<RoleRepresentation> realmRoles = new ArrayList<>();

    roles.setRealm(realmRoles);
    realm.setRoles(roles);

    config.realmConfigure().accept(realm);

    return realm;
  }

  @Override
  public void stop() {
    try {
      deleteRealm();
    } finally {
      super.stop();
    }
  }

  private void createRealm(RealmRepresentation realm) throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(keycloakUri("admin/realms"))
            .header("Authorization", "Bearer " + adminToken())
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(MAPPER.writeValueAsString(realm)))
            .build();

    HttpResponse<String> response = HTTP_CLIENT.send(request, BodyHandlers.ofString());
    expectStatus(response, 201, 409);
  }

  private void deleteRealm() {
    try {
      HttpRequest request =
          HttpRequest.newBuilder(keycloakUri("admin/realms/%s".formatted(config.realmName())))
              .header("Authorization", "Bearer " + adminToken())
              .DELETE()
              .build();

      HttpResponse<String> response = HTTP_CLIENT.send(request, BodyHandlers.ofString());
      expectStatus(response, 204, 404);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private String adminToken() throws IOException, InterruptedException {
    return token(
        keycloakUri("realms/master/protocol/openid-connect/token"),
        Map.of(
            "grant_type",
            "password",
            "client_id",
            "admin-cli",
            "username",
            getAdminUsername(),
            "password",
            getAdminPassword()));
  }

  private static String token(URI tokenEndpoint, Map<String, String> form)
      throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(tokenEndpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(BodyPublishers.ofString(form(form)))
            .build();

    HttpResponse<String> response = HTTP_CLIENT.send(request, BodyHandlers.ofString());
    expectStatus(response, 200);

    JsonNode json = MAPPER.readTree(response.body());
    return json.required("access_token").asText();
  }

  private URI keycloakUri(String path) {
    String base = getAuthServerUrl();
    if (!base.endsWith("/")) {
      base += "/";
    }
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return URI.create(base + path);
  }

  private static String form(Map<String, String> values) {
    return values.entrySet().stream()
        .map(e -> encode(e.getKey()) + "=" + encode(e.getValue()))
        .collect(joining("&"));
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, UTF_8);
  }

  private static void expectStatus(HttpResponse<String> response, int... expected) {
    for (int status : expected) {
      if (response.statusCode() == status) {
        return;
      }
    }

    throw new IllegalStateException(
        "Keycloak request failed with HTTP " + response.statusCode() + ": " + response.body());
  }

  private static void retry(CheckedRunnable runnable) {
    RuntimeException failure = null;
    for (int attempt = 1; attempt <= 5; attempt++) {
      try {
        runnable.run();
        return;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (IOException e) {
        failure = new UncheckedIOException(e);
      } catch (RuntimeException e) {
        failure = e;
      } catch (Exception e) {
        failure = new RuntimeException(e);
      }

      if (attempt < 5) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }

    throw failure;
  }

  @FunctionalInterface
  private interface CheckedRunnable {
    void run() throws Exception;
  }

  /** Returns the (external) root URL for Keycloak (without the context path). */
  public URI getExternalRootUri() {
    return URI.create(
        String.format(
            "%s://%s:%s",
            config.useHttps() ? "https" : "http",
            getHost(),
            config.useHttps() ? getHttpsPort() : getHttpPort() // mapped ports
            ));
  }

  /**
   * Returns the (external) URL of the Keycloak realm. This is the URL that clients outside the
   * Docker network can use to access Keycloak. This is also the URL that should be used as the
   * value of the {@code quarkus.oidc.auth-server-url} property for Quarkus applications running
   * outside the Docker network.
   */
  public URI getExternalRealmUri() {
    return URI.create(
        String.format(
            "%s%srealms/%s",
            getExternalRootUri(), ensureSlashes(getContextPath()), config.realmName()));
  }

  /**
   * Returns the (external) URL of the Keycloak token endpoint. This is the URL that should be used
   * as the value of the {@code nessie.authentication.oauth2.token-endpoint} property for Nessie
   * clients using the OAUTH2 authentication provider, and sitting outside the Docker network.
   */
  public URI getExternalTokenEndpointUri() {
    return URI.create(
        String.format(
            "%s://%s:%s%srealms/%s/protocol/openid-connect/token",
            config.useHttps() ? "https" : "http",
            getHost(),
            config.useHttps() ? getHttpsPort() : getHttpPort(), // mapped ports
            ensureSlashes(getContextPath()),
            config.realmName()));
  }

  /** Returns the (internal) root URL for Keycloak (without the context path). */
  public URI getInternalRootUri() {
    return URI.create(
        String.format(
            "%s://keycloak:%s",
            config.useHttps() ? "https" : "http",
            config.useHttps() ? KEYCLOAK_PORT_HTTPS : KEYCLOAK_PORT_HTTP)); // non-mapped ports
  }

  /**
   * Returns the (internal) URL of the Keycloak realm. This is the URL that clients inside the
   * Docker network can use to access Keycloak. This is also the URL that should be used as the
   * value of the {@code quarkus.oidc.auth-server-url} property for Quarkus applications running
   * inside the Docker network.
   */
  public URI getInternalRealmUri() {
    return URI.create(
        String.format(
            "%s%srealms/%s",
            getInternalRootUri(), ensureSlashes(getContextPath()), config.realmName()));
  }

  public String getExternalIp() {
    return requireNonNull(
            getContainerInfo(),
            "Keycloak container object available, but container info is null. Is the Keycloak container started?")
        .getNetworkSettings()
        .getNetworks()
        .values()
        .iterator()
        .next()
        .getIpAddress();
  }

  private record ExternalNetwork(String networkId) implements Network {
    @Override
    public String getId() {
      return networkId;
    }

    @Override
    public void close() {
      // don't close the external network
    }
  }

  private static String ensureSlashes(String s) {
    if (!s.startsWith("/")) {
      s = "/" + s;
    }
    if (!s.endsWith("/")) {
      s = s + "/";
    }
    return s;
  }
}
