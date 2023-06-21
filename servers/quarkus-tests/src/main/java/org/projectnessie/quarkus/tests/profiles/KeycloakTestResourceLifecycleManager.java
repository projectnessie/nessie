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
package org.projectnessie.quarkus.tests.profiles;

import dasniko.testcontainers.keycloak.ExtendableKeycloakContainer;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.RolesRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

/**
 * This is a copy of, and a drop-in replacement for, the Keycloak test resource {@code
 * io.quarkus.test.keycloak.server.KeycloakTestResourceLifecycleManager} from {@code
 * io.quarkus:quarkus-test-keycloak-server}.
 *
 * <p>This class exists because the original one does not work with recent versions of Keycloak. It
 * introduces the following changes compared to the original:
 *
 * <ul>
 *   <li>Use {@link ExtendableKeycloakContainer} from {@code dasniko/testcontainers-keycloak}
 *       instead of {@code GenericContainer};
 *   <li>Use {@link Keycloak} from {@code org.keycloak:keycloak-admin-client} instead of RestAssured
 *       to interact with the Keycloak server;
 *   <li>Token exchange is enabled;
 *   <li>Implements {@link DevServicesContext.ContextAware}};
 *   <li>Container can be configured using Quarkus test resource init args, falling back to system
 *       properties for defaults;
 *   <li>Defines a few extra configuration properties: {@code quarkus.oidc.token.issuer}, {@code
 *       quarkus.oidc.client-id} and {@code quarkus.oidc.credentials.secret};
 *   <li>Ability to start the container in a shared and/or pre-existing Docker network;
 *   <li>Ability to inject {@link KeycloakTokenEndpointUri} and {@link KeycloakRealmUri} into a test
 *       instance field;
 *   <li>Access token lifespan by default is 1 minute instead of 3 seconds;
 *   <li>Some static methods were removed, instead the {@link CustomKeycloakContainer} instance is
 *       exposed;
 *   <li>Token issuer URL is fixed to facilitate testing involving token validation.
 * </ul>
 *
 * Since it is meant as a replacement of the original test resource, it is possible to use this
 * class with the original test client {@code io.quarkus.test.keycloak.client.KeycloakTestClient}.
 *
 * <p>Any test technique outlined <a
 * href="https://quarkus.io/guides/security-openid-connect">here</a> should work as well when using
 * this test resource.
 */
@SuppressWarnings("SameParameterValue")
public class KeycloakTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  /**
   * This annotation can be used to inject the (external) auth server URI into a test instance
   * field.
   *
   * <p>This is the URL that clients outside the Docker network can use to access Keycloak. This is
   * also the URL that should be used as the value of the {@code quarkus.oidc.auth-server-url}
   * property for Quarkus applications running outside the Docker network.
   */
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface KeycloakRealmUri {}

  /**
   * This annotation can be used to inject the (external) token endpoint URI into a test instance
   * field.
   *
   * <p>This is the URL that should be used as the value of the {@code
   * nessie.authentication.oauth2.token-endpoint} property for Nessie clients using the OAUTH2
   * authentication provider, and sitting outside the Docker network.
   */
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface KeycloakTokenEndpointUri {}

  /**
   * This annotation can be used to inject the client ID into a test instance field.
   *
   * <p>This value is configured using the {@code keycloak.service.client} init arg when starting
   * the Keycloak container, and by default is {@code quarkus-service-app}.
   *
   * <p>This is the client ID that should be used as the value of the {@code
   * nessie.authentication.oauth2.client-id} property for Nessie clients using the OAUTH2
   * authentication provider, and sitting outside the Docker network.
   *
   * <p>It can also be used as the value of the {@code quarkus.oidc.client-id} property for Quarkus
   * applications running outside the Docker network.
   */
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface KeycloakClientId {}

  /**
   * This annotation can be used to inject the client secret into a test instance field.
   *
   * <p>This is the client secret that should be used as the value of the {@code
   * nessie.authentication.oauth2.client-secret} property for Nessie clients using the OAUTH2
   * authentication provider, and sitting outside the Docker network.
   *
   * <p>It can also be used as the value of the {@code quarkus.oidc.credentials.secret} property for
   * Quarkus applications running outside the Docker network.
   *
   * <p>Note: currently, this secret is hard-coded to {@code secret} in the Keycloak container, but
   * this may change in the future.
   */
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface KeycloakClientSecret {}

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KeycloakTestResourceLifecycleManager.class);

  public static final String KEYCLOAK_REALM = "keycloak.realm";
  public static final String KEYCLOAK_SERVICE_CLIENT = "keycloak.service.client";
  public static final String KEYCLOAK_WEB_APP_CLIENT = "keycloak.web-app.client";
  public static final String KEYCLOAK_USE_HTTPS = "keycloak.use.https";
  public static final String KEYCLOAK_DOCKER_IMAGE = "keycloak.docker.image";
  public static final String KEYCLOAK_DOCKER_TAG = "keycloak.docker.tag";
  public static final String KEYCLOAK_DOCKER_NETWORK_ID = "keycloak.docker.network.id";
  public static final String TOKEN_USER_ROLES = "keycloak.token.user-roles";
  public static final String TOKEN_ADMIN_ROLES = "keycloak.token.admin-roles";
  public static final String FEATURES_ENABLED = "keycloak.features.enabled";

  private static final String CLIENT_SECRET = "secret";

  private static CustomKeycloakContainer keycloak;
  private static Keycloak keycloakAdminClient;

  public static CustomKeycloakContainer getKeycloak() {
    return keycloak;
  }

  private DevServicesContext context;
  private String realmName;
  private String serviceClientId;
  private String webAppClientId;
  private boolean useHttps;
  private String dockerImage;
  private String dockerTag;
  private String dockerNetworkId;
  private List<String> tokenUserRoles;
  private List<String> tokenAdminRoles;
  private String[] featuresEnabled;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }

  @Override
  public void init(Map<String, String> initArgs) {
    realmName = initArg(initArgs, KEYCLOAK_REALM, "quarkus");
    serviceClientId = initArg(initArgs, KEYCLOAK_SERVICE_CLIENT, "quarkus-service-app");
    webAppClientId = initArg(initArgs, KEYCLOAK_WEB_APP_CLIENT, "quarkus-web-app");
    useHttps = Boolean.parseBoolean(initArg(initArgs, KEYCLOAK_USE_HTTPS, "false"));
    dockerImage = initArg(initArgs, KEYCLOAK_DOCKER_IMAGE, "quay.io/keycloak/keycloak");
    dockerTag = initArg(initArgs, KEYCLOAK_DOCKER_TAG, "latest");
    dockerNetworkId = initArg(initArgs, KEYCLOAK_DOCKER_NETWORK_ID, null);
    tokenUserRoles = List.of(initArg(initArgs, TOKEN_USER_ROLES, "user").split(","));
    tokenAdminRoles = List.of(initArg(initArgs, TOKEN_ADMIN_ROLES, "user,admin").split(","));
    featuresEnabled = initArg(initArgs, FEATURES_ENABLED, "token-exchange,preview").split(",");
  }

  private static String initArg(Map<String, String> initArgs, String initArg, String defaultValue) {
    return initArgs.getOrDefault(initArg, System.getProperty(initArg, defaultValue));
  }

  @Override
  public Map<String, String> start() {

    LOGGER.info("Using Keycloak image {}:{}", dockerImage, dockerTag);
    keycloak = new CustomKeycloakContainer();

    LOGGER.info("Starting Keycloak...");
    keycloak.start();
    LOGGER.info("Keycloak started, creating realm {}...", realmName);

    keycloakAdminClient = keycloak.getKeycloakAdminClient();
    RealmRepresentation realm = createRealm();
    keycloakAdminClient.realms().create(realm);

    LOGGER.info(
        "Finished setting up Keycloak, external realm auth url: {}",
        keycloak.getExternalRealmUri());

    return Map.of(
        "keycloak.url", keycloak.getAuthServerUrl(), // TODO check if this is needed
        "quarkus.oidc.auth-server-url", keycloak.getExternalRealmUri().toString(),
        "quarkus.oidc.token-path", keycloak.getExternalTokenEndpointUri().toString(),
        "quarkus.oidc.token.issuer", keycloak.getTokenIssuerUri().toString(),
        "quarkus.oidc.client-id", serviceClientId,
        "quarkus.oidc.credentials.secret", CLIENT_SECRET);
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        keycloak.getExternalRealmUri(),
        new TestInjector.AnnotatedAndMatchesType(KeycloakRealmUri.class, URI.class));
    testInjector.injectIntoFields(
        keycloak.getExternalTokenEndpointUri(),
        new TestInjector.AnnotatedAndMatchesType(KeycloakTokenEndpointUri.class, URI.class));
    testInjector.injectIntoFields(
        serviceClientId,
        new TestInjector.AnnotatedAndMatchesType(KeycloakClientId.class, String.class));
    testInjector.injectIntoFields(
        CLIENT_SECRET,
        new TestInjector.AnnotatedAndMatchesType(KeycloakClientSecret.class, String.class));
  }

  private RealmRepresentation createRealm() {
    RealmRepresentation realm = new RealmRepresentation();

    realm.setRealm(realmName);
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

    realm.getRoles().getRealm().add(new RoleRepresentation("user", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("admin", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("confidential", null, false));

    realm.getClients().add(createServiceClient(serviceClientId));
    realm.getClients().add(createWebAppClient(webAppClientId));

    realm.getUsers().add(createUser("alice", tokenUserRoles));
    realm.getUsers().add(createUser("admin", tokenAdminRoles));
    realm.getUsers().add(createUser("jdoe", Arrays.asList("user", "confidential")));

    return realm;
  }

  private static ClientRepresentation createServiceClient(String clientId) {
    ClientRepresentation client = new ClientRepresentation();

    client.setClientId(clientId);
    client.setPublicClient(false);
    client.setSecret(CLIENT_SECRET);
    client.setDirectAccessGrantsEnabled(true);
    client.setServiceAccountsEnabled(true);
    client.setEnabled(true);

    return client;
  }

  private static ClientRepresentation createWebAppClient(String clientId) {
    ClientRepresentation client = new ClientRepresentation();

    client.setClientId(clientId);
    client.setPublicClient(false);
    client.setSecret(CLIENT_SECRET);
    client.setRedirectUris(Collections.singletonList("*"));
    client.setEnabled(true);

    return client;
  }

  private static UserRepresentation createUser(String username, List<String> realmRoles) {
    UserRepresentation user = new UserRepresentation();

    user.setUsername(username);
    user.setEnabled(true);
    user.setCredentials(new ArrayList<>());
    user.setRealmRoles(realmRoles);
    user.setEmail(username + "@gmail.com");

    CredentialRepresentation credential = new CredentialRepresentation();

    credential.setType(CredentialRepresentation.PASSWORD);
    credential.setValue(username);
    credential.setTemporary(false);

    user.getCredentials().add(credential);

    return user;
  }

  @Override
  public void stop() {
    try {
      try {
        Keycloak adminClient = keycloakAdminClient;
        if (adminClient != null) {
          RealmResource realm = adminClient.realm(realmName);
          realm.remove();
        }
      } finally {
        keycloak.stop();
      }
    } finally {
      keycloak = null;
      keycloakAdminClient = null;
    }
  }

  public class CustomKeycloakContainer
      extends ExtendableKeycloakContainer<CustomKeycloakContainer> {

    private static final int KEYCLOAK_PORT_HTTP = 8080;
    private static final int KEYCLOAK_PORT_HTTPS = 8443;

    @SuppressWarnings("resource")
    public CustomKeycloakContainer() {
      super(dockerImage + ":" + dockerTag);

      withNetworkAliases("keycloak");
      withFeaturesEnabled(featuresEnabled);

      // This will force all token issuer claims for the configured realm to be
      // equal to getInternalRealmUri(), and in turn equal to getTokenIssuerUri(),
      // which simplifies testing.
      withEnv("KC_HOSTNAME_URL", getInternalRootUri().toString());

      // Don't use withNetworkMode, or aliases won't work!
      // See https://github.com/testcontainers/testcontainers-java/issues/1221
      if (context.containerNetworkId().isPresent()) {
        withNetwork(new ExternalNetwork(context.containerNetworkId().get()));
      } else if (dockerNetworkId != null) {
        withNetwork(new ExternalNetwork(dockerNetworkId));
      }

      if (useHttps) {
        LOGGER.info("Enabling TLS for Keycloak");
        useTls();
      }
    }

    @Override
    public String getAuthServerUrl() {
      if (context.containerNetworkId().isPresent()) {
        // TODO recheck, not sure why we need to special case this
        return String.format(
            "http%s://%s:%s%s",
            useHttps ? "s" : "",
            getCurrentContainerInfo().getConfig().getHostName(),
            useHttps ? getHttpsPort() : getHttpPort(), // mapped ports
            getContextPath());
      } else {
        return super.getAuthServerUrl();
      }
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
              "%s://%s:%s%srealms/%s",
              useHttps ? "https" : "http",
              getHost(),
              useHttps ? getHttpsPort() : getHttpPort(), // mapped ports
              ensureSlashes(getContextPath()),
              realmName));
    }

    /**
     * Returns the (external) URL of the Keycloak token endpoint. This is the URL that should be
     * used as the value of the {@code nessie.authentication.oauth2.token-endpoint} property for
     * Nessie clients using the OAUTH2 authentication provider, and sitting outside the Docker
     * network.
     */
    public URI getExternalTokenEndpointUri() {
      return URI.create(
          String.format(
              "%s://%s:%s%srealms/%s/protocol/openid-connect/token",
              useHttps ? "https" : "http",
              getHost(),
              useHttps ? getHttpsPort() : getHttpPort(), // mapped ports
              ensureSlashes(getContextPath()),
              realmName));
    }

    /**
     * Returns the (internal) root URL for Keycloak (without the context path). This is used as the
     * issuer claim for all tokens.
     */
    public URI getInternalRootUri() {
      return URI.create(
          String.format(
              "%s://keycloak:%s",
              useHttps ? "https" : "http",
              useHttps ? KEYCLOAK_PORT_HTTPS : KEYCLOAK_PORT_HTTP)); // non-mapped ports
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
              "%s%srealms/%s", getInternalRootUri(), ensureSlashes(getContextPath()), realmName));
    }

    /**
     * Returns the URI used to fill the issuer ("iss") claim for all tokens generated by this
     * server, regardless of the client IP address. Having a fixed issuer claim facilitates testing
     * by making it easier to validate tokens.
     *
     * <p>This is currently set to be the {@link #getInternalRealmUri()}, and cannot be changed.
     */
    public URI getTokenIssuerUri() {
      return getInternalRealmUri();
    }
  }

  private static class ExternalNetwork implements Network {

    private final String networkId;

    public ExternalNetwork(String networkId) {
      this.networkId = networkId;
    }

    @Override
    public Statement apply(Statement var1, Description var2) {
      return null;
    }

    public String getId() {
      return networkId;
    }

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
