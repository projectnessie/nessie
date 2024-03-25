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
import static org.projectnessie.operator.events.EventReason.CreatingService;
import static org.projectnessie.operator.events.EventReason.CreatingServiceAccount;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.CassandraContainerLifecycleManager;
import org.projectnessie.operator.testinfra.CassandraContainerLifecycleManager.CassandraContactPoint;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = CassandraContainerLifecycleManager.class,
    parallel = true,
    restrictToAnnotatedClass = true)
class ITNessieReconcilerCassandra extends AbstractNessieReconcilerIntegrationTests {

  private static final String PREFIX = "/org/projectnessie/operator/it/nessie/cassandra/";

  @CassandraContactPoint private String contactPoint;

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
        .editCassandra()
        .withContactPoints(contactPoint)
        .endCassandra()
        .endVersionStore()
        .endSpec()
        .build();
  }

  @Override
  protected void assertResourcesCreated() {
    checkServiceAccount(
        load(client.serviceAccounts(), PREFIX + "service-account.yaml"),
        get(client.serviceAccounts(), "nessie-test"));
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml")
            .edit()
            .addToData("QUARKUS_CASSANDRA_CONTACT_POINTS", contactPoint)
            .build(),
        get(client.configMaps(), "nessie-test"));
    checkDeployment(
        load(client.apps().deployments(), PREFIX + "deployment.yaml"),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkEvents(
        CreatingServiceAccount,
        CreatingConfigMap,
        CreatingDeployment,
        CreatingService,
        ReconcileSuccess);
    checkNotCreated(client.persistentVolumeClaims());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.network().v1().ingresses());
    checkNotCreated(client.monitoring().serviceMonitors());
    checkNotCreated(client.autoscaling().v2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
  }

  @Override
  protected void assertResourcesDeleted() {
    assertThat(get(client.serviceAccounts(), "nessie-test")).isNull();
    assertThat(get(client.apps().deployments(), "nessie-test")).isNull();
    assertThat(get(client.services(), "nessie-test")).isNull();
    assertThat(getPrimaryEventList().getItems()).isEmpty();
    assertThat(client.resource(primary).get()).isNull();
    // Secret should not be deleted as it is not owned by the Nessie resource
    assertThat(get(client.secrets(), "nessie-db-credentials")).isNotNull();
  }
}
