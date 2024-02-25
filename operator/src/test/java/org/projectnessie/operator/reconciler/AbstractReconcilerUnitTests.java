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
package org.projectnessie.operator.reconciler;

import static org.assertj.core.api.Assertions.assertThat;

import io.fabric8.kubernetes.api.model.APIGroupBuilder;
import io.fabric8.kubernetes.api.model.GroupVersionForDiscovery;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.openshift.client.OpenShiftClient;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

public abstract class AbstractReconcilerUnitTests<T extends HasMetadata>
    extends AbstractReconcilerTests<T> {

  @Inject
  void setClient(OpenShiftClient client) {
    this.client = client;
  }

  @Override
  protected Duration pollInterval() {
    return Duration.ofMillis(100);
  }

  @Override
  protected Duration timeout() {
    return Duration.ofSeconds(30);
  }

  @Override
  protected void setUpFunctionalTest() {
    // No functional tests possible in unit tests, the Nessie deployment is not running
  }

  @Override
  protected void functionalTest() {
    // No functional tests possible in unit tests, the Nessie deployment is not running
  }

  @Override
  protected void assertResourcesDeleted() {
    // Garbage collection of dependent resources is not implemented in MockKubernetesServer,
    // so we can't test that dependent resources are garbage-collected; see
    // https://github.com/fabric8io/kubernetes-client/issues/5607
    assertThat(client.resource(primary).get()).isNull();
  }

  @Override
  protected void checkPvc(PersistentVolumeClaim expected, PersistentVolumeClaim actual) {
    super.checkPvc(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.persistentVolumeClaims().resource(actual).patch();
    }
  }

  @Override
  protected void checkDeployment(Deployment expected, Deployment actual) {
    super.checkDeployment(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.apps().deployments().resource(actual).patchStatus();
    }
  }

  @Override
  protected void checkService(Service expected, Service actual) {
    super.checkService(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.services().resource(actual).patch();
    }
  }

  @Override
  protected void checkIngress(Ingress expected, Ingress actual) {
    super.checkIngress(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.network().v1().ingresses().resource(actual).patch();
    }
  }

  @Override
  protected void checkIngress(
      io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress expected,
      io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress actual) {
    super.checkIngress(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.network().v1beta1().ingresses().resource(actual).patch();
    }
  }

  @Override
  protected void checkAutoscaler(HorizontalPodAutoscaler expected, HorizontalPodAutoscaler actual) {
    super.checkAutoscaler(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.autoscaling().v2().horizontalPodAutoscalers().resource(actual).patchStatus();
    }
  }

  @Override
  protected void checkAutoscaler(
      io.fabric8.kubernetes.api.model.autoscaling.v2beta2.HorizontalPodAutoscaler expected,
      io.fabric8.kubernetes.api.model.autoscaling.v2beta2.HorizontalPodAutoscaler actual) {
    super.checkAutoscaler(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.autoscaling().v2beta2().horizontalPodAutoscalers().resource(actual).patchStatus();
    }
  }

  protected void checkAutoscaler(
      io.fabric8.kubernetes.api.model.autoscaling.v2beta1.HorizontalPodAutoscaler expected,
      io.fabric8.kubernetes.api.model.autoscaling.v2beta1.HorizontalPodAutoscaler actual) {
    super.checkAutoscaler(expected, actual);
    if (actual.getStatus() == null) {
      actual.setStatus(expected.getStatus());
      client.autoscaling().v2beta1().horizontalPodAutoscalers().resource(actual).patchStatus();
    }
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      // Disable SSA for tests with MockKubernetesServer, see
      // https://github.com/fabric8io/kubernetes-client/issues/5337
      return Map.of("quarkus.operator-sdk.enable-ssa", "false");
    }
  }

  public abstract static class Setup implements Consumer<KubernetesServer> {

    protected void reportApiSupported(KubernetesServer server, String group, String version) {
      server
          .expect()
          .get()
          .withPath("/apis/" + group)
          .andReturn(
              200,
              new APIGroupBuilder()
                  .withApiVersion(version)
                  .withKind("APIGroup")
                  .withVersions(new GroupVersionForDiscovery(group + "/" + version, version))
                  .build())
          .always();
    }
  }
}
