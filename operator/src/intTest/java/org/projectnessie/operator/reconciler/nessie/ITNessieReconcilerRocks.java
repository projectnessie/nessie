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
package org.projectnessie.operator.reconciler.nessie;

import static org.projectnessie.operator.events.EventReason.CreatingConfigMap;
import static org.projectnessie.operator.events.EventReason.CreatingDeployment;
import static org.projectnessie.operator.events.EventReason.CreatingMgmtService;
import static org.projectnessie.operator.events.EventReason.CreatingPersistentVolumeClaim;
import static org.projectnessie.operator.events.EventReason.CreatingService;
import static org.projectnessie.operator.events.EventReason.CreatingServiceAccount;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.net.URI;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticatorConfig;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager;
import org.projectnessie.operator.testinfra.KeycloakContainerLifecycleManager;
import org.projectnessie.operator.testinfra.KeycloakContainerLifecycleManager.ExternalRealmUri;
import org.projectnessie.operator.testinfra.KeycloakContainerLifecycleManager.InternalRealmUri;
import org.projectnessie.testing.keycloak.CustomKeycloakContainer;

@QuarkusIntegrationTest
@WithTestResource(value = KeycloakContainerLifecycleManager.class, parallel = true)
@WithTestResource(value = K3sContainerLifecycleManager.class, parallel = true)
class ITNessieReconcilerRocks extends AbstractNessieReconcilerIntegrationTests {

  private static final String PREFIX = "/org/projectnessie/operator/inttests/fixtures/rocks/";

  @InternalRealmUri private URI keycloakInternalRealmUri;
  @ExternalRealmUri private URI keycloakExternalRealmUri;

  @Override
  protected Nessie newPrimary() {
    return load(client.resources(Nessie.class), PREFIX + "nessie.yaml")
        .edit()
        .editSpec()
        .editAuthentication()
        .withOidcAuthServerUrl(String.valueOf(keycloakInternalRealmUri))
        .endAuthentication()
        .endSpec()
        .build();
  }

  @Override
  protected void assertResourcesCreated() {
    checkServiceAccount(
        load(client.serviceAccounts(), PREFIX + "service-account.yaml"),
        get(client.serviceAccounts(), "nessie-test-sa"));
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml"),
        get(client.configMaps(), "nessie-test"),
        "quarkus.oidc.auth-server-url",
        keycloakInternalRealmUri);
    checkPvc(
        load(client.persistentVolumeClaims(), PREFIX + "pvc.yaml"),
        get(client.persistentVolumeClaims(), "nessie-test"));
    checkDeployment(
        overrideConfigChecksum(load(client.apps().deployments(), PREFIX + "deployment.yaml")),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service-mgmt.yaml"),
        get(client.services(), "nessie-test-mgmt"));
    checkEvents(
        CreatingServiceAccount,
        CreatingPersistentVolumeClaim,
        CreatingConfigMap,
        CreatingDeployment,
        CreatingService,
        CreatingMgmtService,
        ReconcileSuccess);
    checkNotCreated(client.network().v1().ingresses());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.autoscaling().v2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
    checkNotCreated(client.monitoring().serviceMonitors());
  }

  @Override
  protected void setUpFunctionalTest() {
    OAuth2AuthenticatorConfig config =
        OAuth2AuthenticatorConfig.builder()
            .issuerUrl(keycloakExternalRealmUri)
            .clientId(KeycloakContainerLifecycleManager.CLIENT_ID)
            .clientSecret(CustomKeycloakContainer.CLIENT_SECRET)
            .addScopes("email", "profile")
            .build();
    nessieClient =
        nessieNodePortClient(OAuth2AuthenticationProvider.create(config)).build(NessieApiV2.class);
  }
}
