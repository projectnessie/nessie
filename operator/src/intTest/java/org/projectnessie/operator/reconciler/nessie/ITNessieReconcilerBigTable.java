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
import static org.projectnessie.operator.events.EventReason.CreatingServiceMonitor;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;
import static org.projectnessie.operator.testinfra.BigTableContainerLifecycleManager.BIGTABLE_PORT;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.Pod;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.BigTableContainerLifecycleManager;
import org.projectnessie.operator.testinfra.BigTableContainerLifecycleManager.BigTableHost;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = BigTableContainerLifecycleManager.class,
    parallel = true,
    restrictToAnnotatedClass = true)
class ITNessieReconcilerBigTable extends AbstractNessieReconcilerIntegrationTests {

  private static final String PREFIX = "/org/projectnessie/operator/it/nessie/bigtable/";

  @BigTableHost private String bigTableHost;

  @BeforeEach
  void createRequiredResources() {
    create(client.secrets(), PREFIX + "secret.yaml");
  }

  @Override
  protected Nessie newPrimary() {
    Nessie nessie = load(client.resources(Nessie.class), PREFIX + "nessie.yaml");
    ((ObjectNode) nessie.getSpec().advancedConfig())
        .put("nessie.version.store.persist.bigtable.emulator-host", bigTableHost)
        .put("nessie.version.store.persist.bigtable.emulator-port", BIGTABLE_PORT);
    return nessie;
  }

  @Override
  protected void assertResourcesCreated() {
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml")
            .edit()
            .addToData("NESSIE_VERSION_STORE_PERSIST_BIGTABLE_EMULATOR_HOST", bigTableHost)
            .build(),
        get(client.configMaps(), "nessie-test"));
    checkDeployment(
        load(client.apps().deployments(), PREFIX + "deployment.yaml"),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkServiceMonitor(
        load(client.monitoring().serviceMonitors(), PREFIX + "service-monitor.yaml"),
        get(client.monitoring().serviceMonitors(), "nessie-test"));
    checkEvents(
        CreatingConfigMap,
        CreatingDeployment,
        CreatingService,
        CreatingServiceMonitor,
        ReconcileSuccess);
    checkNotCreated(client.persistentVolumeClaims());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.network().v1().ingresses());
    checkNotCreated(client.autoscaling().v2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
  }

  @Override
  protected void functionalTest() throws Exception {
    super.functionalTest();
    checkRemoteDebugAndJvmOptions();
  }

  private void checkRemoteDebugAndJvmOptions() {
    Pod pod = client.pods().inNamespace(namespace.getMetadata().getName()).list().getItems().get(0);
    String logs = kubectl.logs(pod.getMetadata().getName(), pod.getMetadata().getNamespace());
    assertThat(logs)
        .contains("Listening for transport dt_socket at address: 5009")
        .contains("-XX:+PrintFlagsFinal");
  }

  @Override
  protected void assertResourcesDeleted() {
    assertThat(get(client.serviceAccounts(), "nessie-test")).isNull();
    assertThat(get(client.apps().deployments(), "nessie-test")).isNull();
    assertThat(get(client.services(), "nessie-test")).isNull();
    assertThat(get(client.monitoring().serviceMonitors(), "nessie-test")).isNull();
    assertThat(getPrimaryEventList().getItems()).isEmpty();
    assertThat(client.resource(primary).get()).isNull();
    // Secret and service account should not be deleted as it is not owned by the Nessie resource
    assertThat(get(client.secrets(), "nessie-db-credentials")).isNotNull();
    assertThat(get(client.serviceAccounts(), "default")).isNotNull();
  }
}
