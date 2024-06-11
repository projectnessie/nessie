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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.representations.idm.ClientScopeRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.projectnessie.testing.keycloak.CustomKeycloakContainer;

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

  public static final String KEYCLOAK_SERVICE_CLIENT = "keycloak.service.client";
  public static final String KEYCLOAK_WEB_APP_CLIENT = "keycloak.web-app.client";
  public static final String TOKEN_USER_ROLES = "keycloak.token.user-roles";
  public static final String TOKEN_ADMIN_ROLES = "keycloak.token.admin-roles";
  public static final String KEYCLOAK_CLIENT_SCOPES = "keycloak.client.scopes";

  private static CustomKeycloakContainer keycloak;

  public static CustomKeycloakContainer getKeycloak() {
    return keycloak;
  }

  private DevServicesContext context;
  private String serviceClientId;
  private String webAppClientId;
  private List<String> tokenUserRoles;
  private List<String> tokenAdminRoles;
  private List<String> clientScopes;

  private CustomKeycloakContainer.KeycloakConfig containerConfig;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }

  @Override
  public void init(Map<String, String> initArgs) {
    serviceClientId = initArg(initArgs, KEYCLOAK_SERVICE_CLIENT, "quarkus-service-app");
    webAppClientId = initArg(initArgs, KEYCLOAK_WEB_APP_CLIENT, "quarkus-web-app");
    tokenUserRoles = List.of(initArg(initArgs, TOKEN_USER_ROLES, "user").split(","));
    tokenAdminRoles = List.of(initArg(initArgs, TOKEN_ADMIN_ROLES, "user,admin").split(","));
    clientScopes =
        List.of(
            initArg(initArgs, KEYCLOAK_CLIENT_SCOPES, "openid,email,profile,catalog,sign")
                .split(","));

    CustomKeycloakContainer.KeycloakConfig.Builder configBuilder =
        CustomKeycloakContainer.builder().fromProperties(initArgs);
    context.containerNetworkId().ifPresent(configBuilder::dockerNetworkId);
    containerConfig = configBuilder.realmConfigure(this::configureRealm).build();
  }

  private static String initArg(Map<String, String> initArgs, String initArg, String defaultValue) {
    return initArgs.getOrDefault(initArg, System.getProperty(initArg, defaultValue));
  }

  @Override
  public Map<String, String> start() {
    keycloak = containerConfig.createContainer();

    keycloak.start();

    return Map.of(
        "keycloak.url",
        keycloak.getAuthServerUrl(), // TODO check if this is needed
        "quarkus.oidc.auth-server-url",
        keycloak.getExternalRealmUri().toString(),
        "quarkus.oidc.client-id",
        serviceClientId,
        "quarkus.oidc.credentials.secret",
        CustomKeycloakContainer.CLIENT_SECRET);
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
        CustomKeycloakContainer.CLIENT_SECRET,
        new TestInjector.AnnotatedAndMatchesType(KeycloakClientSecret.class, String.class));
  }

  private void configureRealm(RealmRepresentation realm) {
    realm.getRoles().getRealm().add(new RoleRepresentation("user", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("admin", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("confidential", null, false));

    realm
        .getClients()
        .add(CustomKeycloakContainer.createServiceClient(serviceClientId, clientScopes));
    realm.getClients().add(CustomKeycloakContainer.createWebAppClient(webAppClientId));

    realm.getUsers().add(CustomKeycloakContainer.createUser("alice", tokenUserRoles));
    realm.getUsers().add(CustomKeycloakContainer.createUser("admin", tokenAdminRoles));

    List<ClientScopeRepresentation> scopes =
        clientScopes.stream()
            .map(
                scopeName -> {
                  ClientScopeRepresentation scope = new ClientScopeRepresentation();
                  scope.setName(scopeName);
                  scope.setProtocol("openid-connect");
                  return scope;
                })
            .collect(Collectors.toList());
    realm.setClientScopes(scopes);
  }

  @Override
  public void stop() {
    try {
      if (keycloak != null) {
        keycloak.stop();
      }
    } finally {
      keycloak = null;
    }
  }
}
