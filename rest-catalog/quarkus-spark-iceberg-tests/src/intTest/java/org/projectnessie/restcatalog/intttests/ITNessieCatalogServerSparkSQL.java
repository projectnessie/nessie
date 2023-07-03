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
package org.projectnessie.restcatalog.intttests;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.testing.keycloak.CustomKeycloakContainer.CLIENT_SECRET;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.nessierunner.common.ProcessHandler;
import org.projectnessie.spark.extensions.AbstractNessieSparkSqlExtensionTest;
import org.projectnessie.testing.keycloak.CustomKeycloakContainer;
import org.projectnessie.testing.nessie.NessieContainer;
import org.testcontainers.containers.Network;

public class ITNessieCatalogServerSparkSQL extends AbstractNessieSparkSqlExtensionTest {
  public static final String SCOPE = "profile";
  // Keycloak container
  static CustomKeycloakContainer keycloakContainer;

  // Nessie Core container
  static NessieContainer nessieCoreContainer;

  // Nessie Catalog

  static String javaExec =
      requireNonNull(
          System.getProperty("javaExec"), "Required system property 'javaExec' is not set");
  static String nessieCatalogServerJar =
      requireNonNull(
          System.getProperty("nessie-catalog-server-jar"),
          "Required system property 'nessie-catalog-server-jar' is not set");

  static final ProcessHandler nessieCatalogProcess = new ProcessHandler();
  static String nessieCatalogUrl;
  static String nessieCatalogIcebergUrl;

  @BeforeAll
  static void startAll() throws Exception {
    String networkId = Network.SHARED.getId();

    CustomKeycloakContainer.KeycloakConfig config =
        CustomKeycloakContainer.builder()
            .dockerNetworkId(networkId)
            .fromProperties(emptyMap())
            .realmConfigure(ITNessieCatalogServerSparkSQL::configureRealm)
            .build();

    keycloakContainer = config.createContainer();
    keycloakContainer.start();

    NessieContainer.NessieConfig nessieConfig =
        NessieContainer.builder()
            .dockerNetworkId(networkId)
            .fromProperties(emptyMap())
            .authEnabled(true)
            .keycloakContainerSupplier(() -> keycloakContainer)
            .build();
    nessieCoreContainer = nessieConfig.createContainer();
    nessieCoreContainer.start();

    nessieCatalogProcess.start(
        new ProcessBuilder(
            javaExec,
            "-XX:SelfDestructTimer=30",
            "-Dquarkus.http.port=0",
            "-Dnessie.server.authentication.enabled=true",
            "-Dquarkus.oidc.auth-server-url=" + keycloakContainer.getExternalRealmUri(),
            "-Dquarkus.oidc.token-issuer=" + keycloakContainer.getTokenIssuerUri(),
            "-Dquarkus.oidc.client-id=" + serviceClientId,
            "-Dquarkus.oidc.credentials.secret=" + CLIENT_SECRET,
            "-Dnessie.iceberg.nessie-client.\"nessie.uri\"="
                + nessieCoreContainer.getExternalNessieBaseUri()
                + "v2/",
            "-Dnessie.iceberg.nessie-client.\"nessie.authentication.oauth2.client-id\"="
                + serviceClientId,
            "-Dnessie.iceberg.nessie-client.\"nessie.authentication.oauth2.client-secret\"="
                + CLIENT_SECRET,
            "-jar",
            nessieCatalogServerJar));
    nessieCatalogUrl = nessieCatalogProcess.getListenUrl().replace("0.0.0.0", "localhost");
    nessieCatalogIcebergUrl = nessieCatalogUrl + "/iceberg";
  }

  private static final String serviceClientId = "quarkus-service-app";
  private static final String webAppClientId = "quarkus-web-app";
  private static final List<String> tokenUserRoles = List.of("user");
  private static final List<String> tokenAdminRoles = List.of("user", "admin");

  private static void configureRealm(RealmRepresentation realm) {
    realm.getRoles().getRealm().add(new RoleRepresentation("user", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("admin", null, false));
    realm.getRoles().getRealm().add(new RoleRepresentation("confidential", null, false));

    realm.getClients().add(CustomKeycloakContainer.createServiceClient(serviceClientId));
    realm.getClients().add(CustomKeycloakContainer.createWebAppClient(webAppClientId));

    realm.getUsers().add(CustomKeycloakContainer.createUser("alice", tokenUserRoles));
    realm.getUsers().add(CustomKeycloakContainer.createUser("admin", tokenAdminRoles));
    realm
        .getUsers()
        .add(CustomKeycloakContainer.createUser("jdoe", Arrays.asList("user", "confidential")));
  }

  @AfterAll
  static void stopNessieCatalog() {
    nessieCatalogProcess.stop();
  }

  @AfterAll
  static void stopNessieCore() {
    try {
      if (nessieCoreContainer != null) {
        nessieCoreContainer.stop();
      }
    } finally {
      nessieCoreContainer = null;
    }
  }

  @AfterAll
  static void stopKeycloak() {
    try {
      if (keycloakContainer != null) {
        keycloakContainer.stop();
      }
    } finally {
      keycloakContainer = null;
    }
  }

  @Test
  public void smokeTest() throws Exception {
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.setConf(new Configuration());
      Map<String, String> props = new HashMap<>();
      props.put(CatalogProperties.URI, nessieCatalogIcebergUrl);
      props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
      props.put(OAuth2Properties.CREDENTIAL, serviceClientId + ":" + CLIENT_SECRET);
      props.put(OAuth2Properties.SCOPE, SCOPE);
      props.put("prefix", "main");
      catalog.initialize("nessie-iceberg-rest", props);

      catalog.listNamespaces(Namespace.empty());
    }
  }

  @Override
  protected String nessieApiUri() {
    return nessieCoreContainer.getExternalNessieBaseUri().resolve("v2").toString();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.remove("ref");
    r.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
    r.put(CatalogProperties.URI, nessieCatalogIcebergUrl);
    r.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
    r.put(OAuth2Properties.CREDENTIAL, serviceClientId + ":" + CLIENT_SECRET);
    r.put(OAuth2Properties.SCOPE, SCOPE);
    r.put("prefix", "main");
    r.put("warehouse", "warehouse");
    return r;
  }

  @Override
  protected HttpClientBuilder configureNessieHttpClient(HttpClientBuilder httpClientBuilder) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(NessieConfigConstants.CONF_NESSIE_URI, nessieCatalogUrl + "/api/v1");
    configMap.put(NessieConfigConstants.CONF_NESSIE_AUTH_TYPE, "OAUTH2");
    configMap.put(NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID, serviceClientId);
    configMap.put(NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET, CLIENT_SECRET);
    configMap.put(NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES, SCOPE);
    configMap.put(
        NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
        nessieCatalogIcebergUrl + "/v1/oauth/tokens");
    return httpClientBuilder.fromConfig(configMap::get);
  }
}
