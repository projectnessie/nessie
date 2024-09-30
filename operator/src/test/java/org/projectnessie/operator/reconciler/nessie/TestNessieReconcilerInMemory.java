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
import static org.projectnessie.operator.events.EventReason.AutoscalingNotAllowed;
import static org.projectnessie.operator.events.EventReason.CreatingConfigMap;
import static org.projectnessie.operator.events.EventReason.CreatingDeployment;
import static org.projectnessie.operator.events.EventReason.CreatingService;
import static org.projectnessie.operator.events.EventReason.MultipleReplicasNotAllowed;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kubernetes.client.WithKubernetesTestServer;
import org.projectnessie.operator.reconciler.AbstractReconcilerUnitTests;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.NessieBuilder;

@QuarkusTest
@TestProfile(AbstractReconcilerUnitTests.Profile.class)
@WithKubernetesTestServer
class TestNessieReconcilerInMemory extends AbstractReconcilerUnitTests<Nessie> {

  private static final String PREFIX = "/org/projectnessie/operator/tests/fixtures/inmemory/";

  @Override
  protected Nessie newPrimary() {
    return load(client.resources(Nessie.class), PREFIX + "nessie.yaml");
  }

  @Override
  protected void assertResourcesCreated() {
    checkInMemoryWarning();
    checkAutoscalingWarning();
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml"),
        get(client.configMaps(), "nessie-test"));
    emulateSideCarInjection();
    checkDeployment(
        load(client.apps().deployments(), PREFIX + "deployment.yaml"),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service-mgmt.yaml"),
        get(client.services(), "nessie-test-mgmt"));
    checkEvents(CreatingConfigMap, CreatingDeployment, CreatingService, ReconcileSuccess);
    checkNotCreated(client.serviceAccounts());
    checkNotCreated(client.persistentVolumeClaims());
    checkNotCreated(client.network().v1().ingresses());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.autoscaling().v2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
    checkNotCreated(client.monitoring().serviceMonitors());
  }

  private void checkInMemoryWarning() {
    if (primary.getSpec().size() == 2) {
      checkEvent(
          MultipleReplicasNotAllowed,
          "InMemory version store can only be used with a single replica.");
      refreshPrimary();
      client
          .resource(primary)
          .edit(p -> new NessieBuilder(p).editOrNewSpec().withSize(1).endSpec().build());
    }
  }

  private void checkAutoscalingWarning() {
    if (primary.getSpec().autoscaling().enabled()) {
      checkEvent(AutoscalingNotAllowed, "Autoscaling is not allowed with InMemory version store.");
      refreshPrimary();
      primary =
          client
              .resource(primary)
              .edit(
                  p ->
                      new NessieBuilder(p)
                          .editOrNewSpec()
                          .editOrNewAutoscaling()
                          .withEnabled(false)
                          .endAutoscaling()
                          .endSpec()
                          .build());
    }
  }

  private void emulateSideCarInjection() {
    Deployment actual = get(client.apps().deployments(), "nessie-test");
    assertThat(actual).isNotNull();
    if (actual.getSpec().getTemplate().getSpec().getInitContainers().isEmpty()) {
      client
          .resource(actual)
          .edit(
              d ->
                  d.edit()
                      .editSpec()
                      .editTemplate()
                      .editSpec()
                      .withInitContainers(
                          new ContainerBuilder()
                              .withName("sidecar")
                              .withImage("busybox")
                              .withImagePullPolicy("IfNotPresent")
                              .withCommand("sleep", "3600")
                              .build())
                      .endSpec()
                      .endTemplate()
                      .endSpec()
                      .build());
    }
  }
}
