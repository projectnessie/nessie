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

import io.fabric8.kubernetes.api.model.EventList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager.Kubectl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;

public abstract class AbstractReconcilerIntegrationTests<T extends HasMetadata>
    extends AbstractReconcilerTests<T> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractReconcilerIntegrationTests.class);

  protected Kubectl kubectl;

  @Override
  protected Duration pollInterval() {
    return Duration.ofSeconds(5);
  }

  @Override
  protected Duration timeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  protected void waitForPrimaryReady() {
    LOGGER.info(
        "Waiting for {} {} to be ready", primary.getSingular(), primary.getMetadata().getName());
    kubectl.waitUntil(primary, namespace.getMetadata().getName(), "Ready", timeout());
  }

  protected EventList getPrimaryEventList() {
    return client
        .v1()
        .events()
        .inNamespace(namespace.getMetadata().getName())
        .withInvolvedObject(
            new ObjectReferenceBuilder()
                .withName(primary.getMetadata().getName())
                .withNamespace(primary.getMetadata().getNamespace())
                .withUid(primary.getMetadata().getUid())
                .build())
        .list();
  }

  @Override
  protected void dumpDiagnostics() {
    LOGGER.error("API server logs:\n{}", kubectl.apiServerLogs());
    ExecResult result = kubectl.exec("top", "pod", "--all-namespaces");
    LOGGER.error("Top pods:\n{}", result.getStdout());
    result = kubectl.exec("top", "node");
    LOGGER.error("Top nodes:\n{}", result.getStdout());
    if (client != null) {
      LOGGER.error("Dumping nodes");
      for (Node node : client.nodes().list().getItems()) {
        LOGGER.error("{}", client.getKubernetesSerialization().asYaml(node));
      }
      LOGGER.error("Dumping pods in namespace {}", namespace.getMetadata().getName());
      try {
        for (Pod pod : list(client.pods())) {
          LOGGER.error("{}", client.getKubernetesSerialization().asYaml(pod));
          LOGGER.error("Logs:\n{}", kubectl.logs(pod, false));
          LOGGER.error("Previous logs:\n{}", kubectl.logs(pod, true));
        }
      } catch (Exception e) {
        LOGGER.error("Failed to dump namespace: {}", e.getMessage());
      }
    }
  }

  @AfterEach
  protected void clearNamespace() {
    try {
      if (primary != null) {
        client.resource(primary).delete();
      }
      if (kubectl != null && namespace != null) {
        LOGGER.info("Deleting all resources in namespace {}", namespace.getMetadata().getName());
        kubectl.deleteAll(namespace.getMetadata().getName(), timeout());
      }
    } finally {
      client.resource(primary).withGracePeriod(0).delete();
    }
  }
}
