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
import static org.projectnessie.operator.events.EventReason.CreatingHPA;
import static org.projectnessie.operator.events.EventReason.CreatingIngress;
import static org.projectnessie.operator.events.EventReason.CreatingMgmtService;
import static org.projectnessie.operator.events.EventReason.CreatingService;
import static org.projectnessie.operator.events.EventReason.CreatingServiceAccount;
import static org.projectnessie.operator.events.EventReason.CreatingServiceMonitor;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kubernetes.client.WithKubernetesTestServer;
import org.projectnessie.operator.reconciler.AbstractReconcilerUnitTests;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;

@QuarkusTest
@TestProfile(AbstractReconcilerUnitTests.Profile.class)
@WithKubernetesTestServer(setup = TestNessieReconcilerJdbc.Setup.class)
class TestNessieReconcilerJdbc extends AbstractReconcilerUnitTests<Nessie> {

  private static final String PREFIX = "/org/projectnessie/operator/tests/fixtures/jdbc/";

  @Override
  protected Nessie newPrimary() {
    return load(client.resources(Nessie.class), PREFIX + "nessie.yaml");
  }

  @Override
  protected void assertResourcesCreated() {
    checkServiceAccount(
        load(client.serviceAccounts(), PREFIX + "service-account.yaml"),
        get(client.serviceAccounts(), "nessie-test-custom-service-account"));
    checkConfigMap(
        load(client.configMaps(), PREFIX + "config-map.yaml"),
        get(client.configMaps(), "nessie-test"));
    checkDeployment(
        load(client.apps().deployments(), PREFIX + "deployment.yaml"),
        get(client.apps().deployments(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service.yaml"), get(client.services(), "nessie-test"));
    checkService(
        load(client.services(), PREFIX + "service-mgmt.yaml"),
        get(client.services(), "nessie-test-mgmt"));
    checkIngress(
        load(client.network().v1().ingresses(), PREFIX + "ingress.yaml"),
        get(client.network().v1().ingresses(), "nessie-test"));
    checkServiceMonitor(
        load(client.monitoring().serviceMonitors(), PREFIX + "service-monitor.yaml"),
        get(client.monitoring().serviceMonitors(), "nessie-test"));
    checkAutoscaler(
        load(client.autoscaling().v2().horizontalPodAutoscalers(), PREFIX + "autoscaler.yaml"),
        get(client.autoscaling().v2().horizontalPodAutoscalers(), "nessie-test"));
    checkEvents(
        CreatingServiceAccount,
        CreatingConfigMap,
        CreatingDeployment,
        CreatingService,
        CreatingMgmtService,
        CreatingIngress,
        CreatingServiceMonitor,
        CreatingHPA,
        ReconcileSuccess);
    checkNotCreated(client.persistentVolumeClaims());
    checkNotCreated(client.network().v1beta1().ingresses());
    checkNotCreated(client.autoscaling().v2beta2().horizontalPodAutoscalers());
    checkNotCreated(client.autoscaling().v2beta1().horizontalPodAutoscalers());
  }

  public static class Setup extends AbstractReconcilerUnitTests.Setup {
    @Override
    public void accept(KubernetesServer server) {
      reportApiSupported(server, "networking.k8s.io", "v1");
      reportApiSupported(server, "autoscaling", "v2");
      reportApiSupported(server, "monitoring.coreos.com", "v1");
    }
  }
}
