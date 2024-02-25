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
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager.Kubectl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusTestResource(value = K3sContainerLifecycleManager.class, parallel = true)
public abstract class AbstractReconcilerIntegrationTests<T extends HasMetadata>
    extends AbstractReconcilerTests<T> {

  static {
    // Needed for Ingress testing when running with JUnit runner
    System.setProperty("jdk.httpclient.allowRestrictedHeaders", "host");
  }

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractReconcilerIntegrationTests.class);

  protected Kubectl kubectl;

  @Override
  protected Duration pollInterval() {
    return Duration.ofSeconds(1);
  }

  @Override
  protected Duration timeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  protected void waitForPrimaryReady() {
    LOGGER.info(
        "Waiting for {} {} to be ready", primary.getSingular(), primary.getMetadata().getName());
    kubectl.waitUntil(primary, namespace.getMetadata().getName(), "Ready", Duration.ofMinutes(1));
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

  @AfterEach
  protected void deleteTestNamespace() {
    if (kubectl != null && namespace != null) {
      LOGGER.info("Deleting all resources in namespace {}", namespace.getMetadata().getName());
      kubectl.deleteAll(namespace.getMetadata().getName());
    }
  }
}
