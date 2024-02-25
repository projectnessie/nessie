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
import static org.projectnessie.operator.events.EventReason.CreatingIngress;
import static org.projectnessie.operator.events.EventReason.CreatingMgmtService;
import static org.projectnessie.operator.events.EventReason.CreatingService;
import static org.projectnessie.operator.events.EventReason.CreatingServiceAccount;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.DynamoContainerLifecycleManager;
import org.projectnessie.operator.testinfra.DynamoContainerLifecycleManager.DynamoEndpoint;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager;

@QuarkusIntegrationTest
@WithTestResource(value = DynamoContainerLifecycleManager.class, parallel = true)
@WithTestResource(
    value = K3sContainerLifecycleManager.class,
    parallel = true,
    initArgs = {
      @ResourceArg(name = "ingress", value = "true"),
    })
class ITNessieReconcilerDynamo extends AbstractNessieReconcilerIntegrationTests {

  private static final String PREFIX = "/org/projectnessie/operator/inttests/fixtures/dynamo/";

  @DynamoEndpoint private String dynamoEndpoint;

  @BeforeEach
  void createRequiredResources() {
    create(client.secrets(), PREFIX + "secret.yaml");
  }

  @Override
  protected Nessie newPrimary() {
    Nessie nessie = load(client.resources(Nessie.class), PREFIX + "nessie.yaml");
    ((ObjectNode) nessie.getSpec().advancedConfig())
        .put("quarkus.dynamodb.endpoint-override", dynamoEndpoint);
    return nessie;
  }

  @Override
  protected void assertResourcesCreated() {
    checkServiceAccount(
        load(client.serviceAccounts(), PREFIX + "service-account.yaml"),
        get(client.serviceAccounts(), "nessie-test"));
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml"),
        get(client.configMaps(), "nessie-test"),
        "quarkus.dynamodb.endpoint-override",
        dynamoEndpoint);
    emulateSideCarInjection();
    checkDeployment(
        overrideConfigChecksum(load(client.apps().deployments(), PREFIX + "deployment.yaml")),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service-mgmt.yaml"),
        get(client.services(), "nessie-test-mgmt"));
    checkIngress(
        load(client.network().v1().ingresses(), PREFIX + "ingress.yaml"),
        get(client.network().v1().ingresses(), "nessie-test"));
    checkEvents(
        CreatingServiceAccount,
        CreatingConfigMap,
        CreatingDeployment,
        CreatingService,
        CreatingMgmtService,
        CreatingIngress,
        ReconcileSuccess);
    checkNotCreated(client.persistentVolumeClaims());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.monitoring().serviceMonitors());
    checkNotCreated(client.autoscaling().v2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
  }

  @Override
  protected void setUpFunctionalTest() {
    nessieClient = nessieIngressClient().build(NessieApiV2.class);
  }

  @Override
  protected void assertResourcesDeleted() {
    super.assertResourcesDeleted();
    assertThat(get(client.secrets(), "nessie-dynamo-credentials")).isNotNull();
  }

  private void emulateSideCarInjection() {
    Deployment actual = get(client.apps().deployments(), "nessie-test");
    assertThat(actual).isNotNull();
    if (actual.getSpec().getTemplate().getSpec().getInitContainers().isEmpty()) {
      Deployment desired =
          new DeploymentBuilder()
              .withNewMetadata()
              .withName("nessie-test")
              .withNamespace(namespace.getMetadata().getName())
              .withResourceVersion(actual.getMetadata().getResourceVersion())
              .endMetadata()
              .withNewSpec()
              .withNewTemplate()
              .withNewSpec()
              .withInitContainers(
                  new ContainerBuilder()
                      .withName("sidecar")
                      .withImage("k8s.gcr.io/pause")
                      .withImagePullPolicy("IfNotPresent")
                      .build())
              .endSpec()
              .endTemplate()
              .endSpec()
              .build();
      Deployment updated =
          client
              .resource(desired)
              .fieldManager("sidecar-injector")
              .forceConflicts()
              .serverSideApply();
      assertThat(updated.getMetadata().getManagedFields())
          .extracting(ManagedFieldsEntry::getManager)
          .contains("nessie-controller", "sidecar-injector");
    }
  }
}
