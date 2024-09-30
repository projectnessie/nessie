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
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.projectnessie.operator.events.EventReason.CreatingConfigMap;
import static org.projectnessie.operator.events.EventReason.CreatingDeployment;
import static org.projectnessie.operator.events.EventReason.CreatingHPA;
import static org.projectnessie.operator.events.EventReason.CreatingMgmtService;
import static org.projectnessie.operator.events.EventReason.CreatingService;
import static org.projectnessie.operator.events.EventReason.CreatingServiceAccount;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager;
import org.projectnessie.operator.testinfra.PostgresContainerLifecycleManager;
import org.projectnessie.operator.testinfra.PostgresContainerLifecycleManager.JdbcUrl;

@QuarkusIntegrationTest
@WithTestResource(value = PostgresContainerLifecycleManager.class, parallel = true)
@WithTestResource(value = K3sContainerLifecycleManager.class, parallel = true)
class ITNessieReconcilerJdbc extends AbstractNessieReconcilerIntegrationTests {

  private static final String PREFIX = "/org/projectnessie/operator/inttests/fixtures/jdbc/";

  @JdbcUrl private String jdbcUrl;

  @BeforeEach
  void createRequiredResources() {
    create(client.secrets(), PREFIX + "secret.yaml");
  }

  @Override
  protected Nessie newPrimary() {
    return load(client.resources(Nessie.class), PREFIX + "nessie.yaml")
        .edit()
        .editSpec()
        .editVersionStore()
        .editJdbc()
        .withUrl(jdbcUrl)
        .endJdbc()
        .endVersionStore()
        .endSpec()
        .build();
  }

  @Override
  protected void assertResourcesCreated() {
    checkServiceAccount(
        load(client.serviceAccounts(), PREFIX + "service-account.yaml"),
        get(client.serviceAccounts(), "nessie-test-custom-service-account"));
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml"),
        get(client.configMaps(), "nessie-test"),
        "quarkus.datasource.postgresql.jdbc.url",
        jdbcUrl);
    checkDeployment(
        overrideConfigChecksum(load(client.apps().deployments(), PREFIX + "deployment.yaml")),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service-mgmt.yaml"),
        get(client.services(), "nessie-test-mgmt"));
    checkAutoscaler(
        load(client.autoscaling().v2().horizontalPodAutoscalers(), PREFIX + "autoscaler.yaml"),
        get(client.autoscaling().v2().horizontalPodAutoscalers(), "nessie-test"));
    checkReplicasManagedByHPA();
    checkEvents(
        CreatingServiceAccount,
        CreatingConfigMap,
        CreatingDeployment,
        CreatingService,
        CreatingMgmtService,
        CreatingHPA,
        ReconcileSuccess);
    checkNotCreated(client.persistentVolumeClaims());
    checkNotCreated(client.network().v1().ingresses());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.monitoring().serviceMonitors());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
  }

  private void checkReplicasManagedByHPA() {
    Deployment actual = get(client.apps().deployments(), "nessie-test");
    assertThat(actual).isNotNull();
    assertThat(actual.getSpec().getReplicas()).isEqualTo(2);
    ManagedFieldsEntry fields =
        actual.getMetadata().getManagedFields().stream()
            .filter(m -> m.getManager().equals("nessie-controller"))
            .findFirst()
            .orElseThrow();
    assertThat(fields.getFieldsV1().getAdditionalProperties()).containsKey("f:spec");
    assertThat(fields.getFieldsV1().getAdditionalProperties().get("f:spec"))
        .asInstanceOf(MAP)
        .doesNotContainKey("f:replicas");
  }

  @Override
  protected void assertResourcesDeleted() {
    super.assertResourcesDeleted();
    assertThat(get(client.secrets(), "nessie-db-credentials")).isNotNull();
    assertThat(get(client.serviceAccounts(), "nessie-test-custom-service-account")).isNull();
  }
}
