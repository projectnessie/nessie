/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.commands;

import static java.lang.String.format;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.ClientScopeRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.nessie.cli.cli.NotConnectedException;
import org.projectnessie.nessie.cli.cmdspec.ImmutableConnectCommandSpec;
import org.projectnessie.testing.keycloak.CustomKeycloakContainer;

@ExtendWith(SoftAssertionsExtension.class)
public class ITAuthAndIcebergREST extends WithNessie {

  @InjectSoftAssertions protected SoftAssertions soft;

  private static final String serviceClientId = "quarkus-service-app";
  private static CustomKeycloakContainer keycloak;
  protected static String realmName;

  @BeforeAll
  public static void start() throws Exception {
    CustomKeycloakContainer.KeycloakConfig.Builder configBuilder =
        CustomKeycloakContainer.builder();
    CustomKeycloakContainer.KeycloakConfig containerConfig =
        configBuilder.realmConfigure(ITAuthAndIcebergREST::configureRealm).build();

    keycloak = containerConfig.createContainer();
    keycloak.start();

    realmName = containerConfig.realmName();

    String bearerToken =
        keycloak.createBearerToken(
            realmName, serviceClientId, CustomKeycloakContainer.CLIENT_SECRET);

    Map<String, String> serverConfig = new HashMap<>();
    serverConfig.put("quarkus.oidc.auth-server-url", keycloak.getExternalRealmUri().toString());
    serverConfig.put("quarkus.oidc.client-id", serviceClientId);
    serverConfig.put("quarkus.oidc.credentials.secret", CustomKeycloakContainer.CLIENT_SECRET);
    serverConfig.put("nessie.server.authentication.enabled", "true");

    Map<String, String> clientConfig = new HashMap<>();
    clientConfig.put(CONF_NESSIE_AUTH_TYPE, "BEARER");
    clientConfig.put(CONF_NESSIE_AUTH_TOKEN, bearerToken);

    setupObjectStoreAndNessie(serverConfig, clientConfig);
  }

  @AfterAll
  public static void stop() {
    keycloak.stop();
  }

  private static void configureRealm(RealmRepresentation realm) {
    realm.getRoles().getRealm().add(new RoleRepresentation("user", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("admin", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("confidential", null, false));

    List<String> clientScopes = List.of("openid", "email", "profile", "catalog", "sign");
    ClientRepresentation clientRepresentation =
        CustomKeycloakContainer.createServiceClient(serviceClientId, clientScopes);
    realm.getClients().add(clientRepresentation);

    realm.getUsers().add(CustomKeycloakContainer.createUser("alice", List.of("user")));

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

  @Test
  public void tokenAuth() throws Exception {
    String bearerToken =
        keycloak.createBearerToken(
            realmName, serviceClientId, CustomKeycloakContainer.CLIENT_SECRET);

    try (NessieCliTester cli = new NessieCliTester()) {
      soft.assertThatThrownBy(cli::mandatoryNessieApi).isInstanceOf(NotConnectedException.class);

      soft.assertThatThrownBy(
              () -> cli.execute(ImmutableConnectCommandSpec.builder().uri(icebergUri).build()))
          .isInstanceOf(NessieNotAuthorizedException.class);

      soft.assertThatThrownBy(
              () -> cli.execute(ImmutableConnectCommandSpec.builder().uri(nessieApiUri).build()))
          .isInstanceOf(NessieNotAuthorizedException.class);

      soft.assertThatCode(
              () ->
                  cli.execute(
                      ImmutableConnectCommandSpec.builder()
                          .uri(icebergUri)
                          .putParameter("token", bearerToken)
                          .build()))
          .doesNotThrowAnyException();
      soft.assertThat(cli.capturedOutput())
          .containsExactly(
              format("Connecting to %s ...", icebergUri),
              format("Successfully connected to Iceberg REST at %s", icebergUri),
              format("Connecting to Nessie REST at %s/ ...", nessieApiUri),
              format(
                  "Successfully connected to Nessie REST at %s/ - Nessie API version 2, spec version 2.2.0",
                  nessieApiUri));

      soft.assertThatCode(
              () ->
                  cli.execute(
                      ImmutableConnectCommandSpec.builder()
                          .uri(icebergUri)
                          .putParameter(CONF_NESSIE_AUTH_TOKEN, bearerToken)
                          .build()))
          .doesNotThrowAnyException();
      soft.assertThat(cli.capturedOutput())
          .containsExactly(
              format("Connecting to %s ...", icebergUri),
              format("Successfully connected to Iceberg REST at %s", icebergUri),
              format("Connecting to Nessie REST at %s/ ...", nessieApiUri),
              format(
                  "Successfully connected to Nessie REST at %s/ - Nessie API version 2, spec version 2.2.0",
                  nessieApiUri));
    }
  }
}
