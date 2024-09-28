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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.operator.events.EventReason.CreatingConfigMap;
import static org.projectnessie.operator.events.EventReason.CreatingDeployment;
import static org.projectnessie.operator.events.EventReason.CreatingMgmtService;
import static org.projectnessie.operator.events.EventReason.CreatingService;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;
import static org.projectnessie.operator.testinfra.MongoContainerLifecycleManager.DATABASE_NAME;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager;
import org.projectnessie.operator.testinfra.MongoContainerLifecycleManager;
import org.projectnessie.operator.testinfra.MongoContainerLifecycleManager.MongoConnectionString;

@QuarkusIntegrationTest
@WithTestResource(value = MongoContainerLifecycleManager.class, parallel = true)
@WithTestResource(value = K3sContainerLifecycleManager.class, parallel = true)
class ITNessieReconcilerMongo extends AbstractNessieReconcilerIntegrationTests {

  private static final String PREFIX = "/org/projectnessie/operator/inttests/fixtures/mongo/";

  @MongoConnectionString private String connectionString;

  @BeforeEach
  void createRequiredResources() {
    create(client.secrets(), PREFIX + "secret.yaml");
    create(client.serviceAccounts(), PREFIX + "service-account.yaml");
  }

  @Override
  protected Nessie newPrimary() {
    return load(client.resources(Nessie.class), PREFIX + "nessie.yaml")
        .edit()
        .editSpec()
        .editVersionStore()
        .editMongoDb()
        .withConnectionString(connectionString)
        .withDatabase(DATABASE_NAME)
        .endMongoDb()
        .endVersionStore()
        .endSpec()
        .build();
  }

  @Override
  protected void assertResourcesCreated() {
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml"),
        get(client.configMaps(), "nessie-test"),
        "quarkus.mongodb.connection-string",
        connectionString);
    checkDeployment(
        overrideConfigChecksum(load(client.apps().deployments(), PREFIX + "deployment.yaml")),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service-mgmt.yaml"),
        get(client.services(), "nessie-test-mgmt"));
    checkEvents(
        CreatingConfigMap,
        CreatingDeployment,
        CreatingService,
        CreatingMgmtService,
        ReconcileSuccess);
    checkNotCreated(client.persistentVolumeClaims());
    checkNotCreated(client.network().v1().ingresses());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.monitoring().serviceMonitors());
    checkNotCreated(client.autoscaling().v2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
  }

  @Override
  protected void assertResourcesDeleted() {
    super.assertResourcesDeleted();
    assertThat(get(client.secrets(), "nessie-db-credentials")).isNotNull();
    assertThat(get(client.serviceAccounts(), "nessie-test-custom-service-account")).isNotNull();
  }
}
