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
package org.projectnessie.restcatalog.server;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KEYCLOAK_DOCKER_NETWORK_ID;
import static org.projectnessie.restcatalog.server.resources.NessieDockerTestResourceLifecycleManager.NESSIE_DOCKER_AUTH_ENABLED;
import static org.projectnessie.restcatalog.server.resources.NessieDockerTestResourceLifecycleManager.NESSIE_DOCKER_NETWORK_ID;

import com.google.common.collect.ImmutableList;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Tag;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientId;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientSecret;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakTokenEndpointUri;
import org.projectnessie.restcatalog.server.resources.NessieDockerTestResourceLifecycleManager;
import org.projectnessie.restcatalog.server.resources.NessieDockerTestResourceLifecycleManager.NessieDockerUri;
import org.projectnessie.restcatalog.server.resources.WarehouseTestLocation;
import org.testcontainers.containers.Network;

public abstract class BaseNessieRestCatalogTests extends CatalogTests<RESTCatalog> {

  @NessieDockerUri URI nessieUri;
  @KeycloakTokenEndpointUri URI oidcTokenEndpointUri;
  @KeycloakClientId String oidcClientId;
  @KeycloakClientSecret String oidcClientSecret;

  @BeforeEach
  public void clearRepo() throws Exception {
    try (NessieApiV2 controlClient = buildControlClient()) {
      Branch main = controlClient.getDefaultBranch();
      Branch empty =
          Branch.of(
              main.getName(), "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d");

      controlClient.getAllReferences().stream()
          .forEach(
              ref -> {
                try {
                  if (ref instanceof Branch) {
                    if (!ref.getName().equals(main.getName())) {
                      controlClient.deleteBranch().branch((Branch) ref).delete();
                    }
                  } else if (ref instanceof Tag) {
                    controlClient.deleteTag().tag((Tag) ref).delete();
                  }
                } catch (NessieConflictException | NessieNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });

      checkState(
          controlClient.assignBranch().branch(main).assignTo(empty).assignAndGet().equals(empty));
    }
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected RESTCatalog catalog() {
    try (NessieApiV2 controlClient = buildControlClient()) {
      RESTCatalog catalog = new RESTCatalog();
      catalog.setConf(new Configuration());
      catalog.initialize(
          "nessie-iceberg-rest",
          Map.of(
              // REST Catalog server URI
              CatalogProperties.URI,
              restCatalogServerUri(),
              // FileIO
              CatalogProperties.FILE_IO_IMPL,
              "org.apache.iceberg.io.ResolvingFileIO",
              // Credentials
              OAuth2Properties.CREDENTIAL,
              oidcClientId + ":" + oidcClientSecret,
              // OAUth2 Scope
              OAuth2Properties.SCOPE,
              "profile",
              // Prefix (branch)
              "prefix",
              requireNonNull(controlClient.getConfig().getDefaultBranch())));
      return catalog;
    }
  }

  private String restCatalogServerUri() {
    return format(
        "http://127.0.0.1:%d/iceberg/",
        Objects.requireNonNull(
            Integer.getInteger("quarkus.http.test-port"),
            "System property not set correctly: quarkus.http.test-port"));
  }

  private NessieApiV2 buildControlClient() {
    Map<String, String> config =
        Map.of(
            // Nessie URI
            CONF_NESSIE_URI,
            nessieUri.toString(),
            // Authentication type: OAUTH2
            CONF_NESSIE_AUTH_TYPE,
            OAuth2AuthenticationProvider.AUTH_TYPE_VALUE,
            // OAUTH2 token endpoint URI
            CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
            oidcTokenEndpointUri.toString(),
            // OAUTH2 client ID
            CONF_NESSIE_OAUTH2_CLIENT_ID,
            oidcClientId,
            // OAUTH2 client secret
            CONF_NESSIE_OAUTH2_CLIENT_SECRET,
            oidcClientSecret);
    return HttpClientBuilder.builder().fromConfig(config::get).build(NessieApiV2.class);
  }

  static class Profile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
      return ImmutableList.of(
          new TestResourceEntry(
              KeycloakTestResourceLifecycleManager.class,
              Map.of(KEYCLOAK_DOCKER_NETWORK_ID, Network.SHARED.getId())),
          new TestResourceEntry(
              NessieDockerTestResourceLifecycleManager.class,
              Map.of(
                  NESSIE_DOCKER_AUTH_ENABLED,
                  "true",
                  NESSIE_DOCKER_NETWORK_ID,
                  Network.SHARED.getId())),
          new TestResourceEntry(WarehouseTestLocation.class));
    }
  }
}
