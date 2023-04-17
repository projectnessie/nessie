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

import dasniko.testcontainers.keycloak.KeycloakContainer;
import io.quarkus.runtime.configuration.ConfigurationException;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.RolesRepresentation;
import org.keycloak.representations.idm.UserRepresentation;

/**
 * This is a copy of, and a drop-in replacement for, the Keycloak test resource {@code
 * io.quarkus.test.keycloak.server.KeycloakTestResourceLifecycleManager} from {@code
 * io.quarkus:quarkus-test-keycloak-server}.
 *
 * <p>This class exists because the original one does not work with recent versions of Keycloak. It
 * introduces the following changes compared to the original:
 *
 * <ul>
 *   <li>Use {@link KeycloakContainer} from {@code dasniko/testcontainers-keycloak} instead of
 *       {@code GenericContainer};
 *   <li>Use {@link Keycloak} from {@code org.keycloak:keycloak-admin-client} instead of RestAssured
 *       to interact with the Keycloak server;
 *   <li>Token exchange is enabled;
 *   <li>Some static methods were removed.
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
public class KeycloakTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  private static final String KEYCLOAK_REALM = System.getProperty("keycloak.realm", "quarkus");
  private static final String KEYCLOAK_SERVICE_CLIENT =
      System.getProperty("keycloak.service.client", "quarkus-service-app");
  private static final String KEYCLOAK_WEB_APP_CLIENT =
      System.getProperty("keycloak.web-app.client", "quarkus-web-app");
  private static final Boolean KEYCLOAK_USE_HTTPS =
      Boolean.valueOf(System.getProperty("keycloak.use.https", "true"));
  private static final String KEYCLOAK_VERSION = System.getProperty("keycloak.version");
  private static final String KEYCLOAK_DOCKER_IMAGE = System.getProperty("keycloak.docker.image");

  private static final String TOKEN_USER_ROLES =
      System.getProperty("keycloak.token.user-roles", "user");
  private static final String TOKEN_ADMIN_ROLES =
      System.getProperty("keycloak.token.admin-roles", "user,admin");

  private static KeycloakContainer keycloak;
  private static Keycloak keycloakAdminClient;

  @Override
  public Map<String, String> start() {
    String keycloakDockerImage;
    if (KEYCLOAK_DOCKER_IMAGE != null) {
      keycloakDockerImage = KEYCLOAK_DOCKER_IMAGE;
    } else if (KEYCLOAK_VERSION != null) {
      keycloakDockerImage = "quay.io/keycloak/keycloak:" + KEYCLOAK_VERSION;
    } else {
      throw new ConfigurationException(
          "Please set either 'keycloak.docker.image' or 'keycloak.version' system property");
    }

    keycloak =
        new KeycloakContainer(keycloakDockerImage).withFeaturesEnabled("preview", "token-exchange");

    if (KEYCLOAK_USE_HTTPS) {
      keycloak.useTls();
    }

    keycloak.start();

    keycloakAdminClient = keycloak.getKeycloakAdminClient();
    RealmRepresentation realm = createRealm();
    keycloakAdminClient.realms().create(realm);

    String keycloakServerUrl = keycloak.getAuthServerUrl();
    Map<String, String> conf = new HashMap<>();
    conf.put("keycloak.url", keycloakServerUrl);
    conf.put("quarkus.oidc.auth-server-url", keycloakServerUrl + "realms/" + KEYCLOAK_REALM);

    return conf;
  }

  private static RealmRepresentation createRealm() {
    RealmRepresentation realm = new RealmRepresentation();

    realm.setRealm(KEYCLOAK_REALM);
    realm.setEnabled(true);
    realm.setUsers(new ArrayList<>());
    realm.setClients(new ArrayList<>());
    realm.setAccessTokenLifespan(3);
    realm.setSsoSessionMaxLifespan(3);

    RolesRepresentation roles = new RolesRepresentation();
    List<RoleRepresentation> realmRoles = new ArrayList<>();

    roles.setRealm(realmRoles);
    realm.setRoles(roles);

    realm.getRoles().getRealm().add(new RoleRepresentation("user", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("admin", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("confidential", null, false));

    realm.getClients().add(createServiceClient(KEYCLOAK_SERVICE_CLIENT));
    realm.getClients().add(createWebAppClient(KEYCLOAK_WEB_APP_CLIENT));

    realm.getUsers().add(createUser("alice", getUserRoles()));
    realm.getUsers().add(createUser("admin", getAdminRoles()));
    realm.getUsers().add(createUser("jdoe", Arrays.asList("user", "confidential")));

    return realm;
  }

  //  private static String getAdminAccessToken() {
  //    return createRequestSpec()
  //        .param("grant_type", "password")
  //        .param("username", "admin")
  //        .param("password", "admin")
  //        .param("client_id", "admin-cli")
  //        .when()
  //        .post(keycloakServerUrl + "/realms/master/protocol/openid-connect/token")
  //        .as(AccessTokenResponse.class)
  //        .getToken();
  //  }

  private static ClientRepresentation createServiceClient(String clientId) {
    ClientRepresentation client = new ClientRepresentation();

    client.setClientId(clientId);
    client.setPublicClient(false);
    client.setSecret("secret");
    client.setDirectAccessGrantsEnabled(true);
    client.setServiceAccountsEnabled(true);
    client.setEnabled(true);

    return client;
  }

  private static ClientRepresentation createWebAppClient(String clientId) {
    ClientRepresentation client = new ClientRepresentation();

    client.setClientId(clientId);
    client.setPublicClient(false);
    client.setSecret("secret");
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
      RealmResource realm = keycloakAdminClient.realm(KEYCLOAK_REALM);
      realm.remove();
    } finally {
      keycloak.stop();
    }
  }

  private static List<String> getAdminRoles() {
    return Arrays.asList(TOKEN_ADMIN_ROLES.split(","));
  }

  private static List<String> getUserRoles() {
    return Arrays.asList(TOKEN_USER_ROLES.split(","));
  }
}
