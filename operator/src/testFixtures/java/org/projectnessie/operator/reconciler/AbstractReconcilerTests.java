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
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.monitoring.v1.ServiceMonitor;
import io.fabric8.openshift.client.OpenShiftClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.extractor.Extractors;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.utils.EventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractReconcilerTests<T extends HasMetadata> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReconcilerTests.class);
  private static final AtomicInteger COUNTER = new AtomicInteger();

  protected OpenShiftClient client;
  protected T primary;
  protected Namespace namespace;

  @BeforeEach
  void createTestNamespace() {
    String namespaceName = "test-" + COUNTER.incrementAndGet();
    namespace =
        new NamespaceBuilder().withNewMetadata().withName(namespaceName).endMetadata().build();
    client.namespaces().resource(namespace).create();
  }

  @Test
  void createAndDelete() {
    primary = newPrimary();
    primary.getMetadata().setNamespace(namespace.getMetadata().getName());
    LOGGER.info(
        "Creating {} {} in namespace {}",
        primary.getSingular(),
        primary.getMetadata().getName(),
        namespace.getMetadata().getName());
    primary = client.resource(primary).create();
    awaitUntilAsserted(this::assertResourcesCreated, "Failed to assert resources created");
    waitForPrimaryReady();
    setUpFunctionalTest();
    LOGGER.info(
        "Testing {} {} in namespace {}",
        primary.getSingular(),
        primary.getMetadata().getName(),
        namespace.getMetadata().getName());
    awaitUntilAsserted(this::functionalTest, "Functional test failed");
    LOGGER.info(
        "Deleting {} {} in namespace {}",
        primary.getSingular(),
        primary.getMetadata().getName(),
        namespace.getMetadata().getName());
    client.resource(primary).delete();
    awaitUntilAsserted(this::assertResourcesDeleted, "Failed to assert resources deleted");
  }

  protected abstract Duration pollInterval();

  protected abstract Duration timeout();

  protected abstract T newPrimary();

  protected void refreshPrimary() {
    primary = client.resource(primary).get();
  }

  protected void waitForPrimaryReady() {}

  protected abstract void assertResourcesCreated() throws Exception;

  protected abstract void setUpFunctionalTest();

  protected abstract void functionalTest() throws Exception;

  protected abstract void assertResourcesDeleted() throws Exception;

  protected <R extends HasMetadata> R get(
      MixedOperation<R, ?, ? extends Resource<R>> resources, String name) {
    return resources.inNamespace(namespace.getMetadata().getName()).withName(name).get();
  }

  protected <R extends HasMetadata, L extends KubernetesResourceList<R>> List<R> list(
      MixedOperation<R, L, ? extends Resource<R>> resources) {
    return resources.inNamespace(namespace.getMetadata().getName()).list().getItems();
  }

  protected <R extends HasMetadata> R load(
      MixedOperation<R, ?, ? extends Resource<R>> op, String classpathResource) {
    return loadResource(op, classpathResource).item();
  }

  protected <R extends HasMetadata> R create(
      MixedOperation<R, ?, ? extends Resource<R>> op, String classpathResource) {
    return loadResource(op, classpathResource).create();
  }

  private <R extends HasMetadata> Resource<R> loadResource(
      MixedOperation<R, ?, ? extends Resource<R>> op, String classpathResource) {
    Resource<R> resource = op.load(openStream(classpathResource));
    resource.item().getMetadata().setNamespace(namespace.getMetadata().getName());
    return resource;
  }

  private InputStream openStream(String classpathResource) {
    try (InputStream in = getClass().getResourceAsStream(classpathResource)) {
      String contents =
          new String(Objects.requireNonNull(in).readAllBytes(), StandardCharsets.UTF_8);
      contents = contents.replaceAll("@namespace@", namespace.getMetadata().getName());
      return new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void checkServiceAccount(ServiceAccount expected, ServiceAccount actual) {
    assertThat(actual).isNotNull();
    checkMeta(expected, actual);
  }

  protected void checkConfigMap(ConfigMap expected, ConfigMap actual, Object... overrides) {
    assertThat(actual).isNotNull();
    checkMeta(expected, actual);
    assertThat(actual.getData()).hasSize(1).containsKey("application.properties");
    Properties actualProperties = new Properties();
    Properties expectedProperties = new Properties();
    try {
      actualProperties.load(
          new ByteArrayInputStream(
              actual.getData().get("application.properties").getBytes(StandardCharsets.UTF_8)));
      expectedProperties.load(
          new ByteArrayInputStream(
              expected.getData().get("application.properties").getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < overrides.length; i++) {
      expectedProperties.setProperty(overrides[i].toString(), overrides[++i].toString());
    }
    assertThat(actualProperties).isEqualTo(expectedProperties);
    assertThat(actual.getBinaryData()).isNullOrEmpty();
  }

  protected void checkPvc(PersistentVolumeClaim expected, PersistentVolumeClaim actual) {
    checkDependent(expected, actual, "volumeName");
  }

  protected void checkDeployment(Deployment expected, Deployment actual) {
    checkDependent(expected, actual);
  }

  protected void checkService(Service expected, Service actual) {
    checkDependent(expected, actual, "clusterIP", "clusterIPs", "ipFamilies");
  }

  protected void checkIngress(Ingress expected, Ingress actual) {
    checkDependent(expected, actual);
  }

  protected void checkIngress(
      io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress expected,
      io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress actual) {
    checkDependent(expected, actual);
  }

  protected void checkServiceMonitor(ServiceMonitor expected, ServiceMonitor actual) {
    checkDependent(expected, actual);
  }

  protected void checkAutoscaler(HorizontalPodAutoscaler expected, HorizontalPodAutoscaler actual) {
    checkDependent(expected, actual);
  }

  protected void checkAutoscaler(
      io.fabric8.kubernetes.api.model.autoscaling.v2beta2.HorizontalPodAutoscaler expected,
      io.fabric8.kubernetes.api.model.autoscaling.v2beta2.HorizontalPodAutoscaler actual) {
    checkDependent(expected, actual);
  }

  protected void checkAutoscaler(
      io.fabric8.kubernetes.api.model.autoscaling.v2beta1.HorizontalPodAutoscaler expected,
      io.fabric8.kubernetes.api.model.autoscaling.v2beta1.HorizontalPodAutoscaler actual) {
    checkDependent(expected, actual);
  }

  protected void checkDependent(
      HasMetadata expected, HasMetadata actual, String... ignoredSpecFields) {
    assertThat(actual).isNotNull();
    checkMeta(expected, actual);
    checkSpec(expected, actual, ignoredSpecFields);
  }

  protected void checkEvents(EventReason... reasons) {
    for (EventReason reason : reasons) {
      Event event = get(client.v1().events(), EventUtils.eventName(primary, reason));
      assertThat(event).as("Expecting event with reason %s to exist", reason).isNotNull();
      assertThat(event.getType()).isEqualTo(reason.type().name());
    }
  }

  protected void checkEvent(EventReason reason, String message) {
    Event event = get(client.v1().events(), EventUtils.eventName(primary, reason));
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(reason.type().name());
    assertThat(event.getMessage()).isEqualTo(message);
  }

  protected void checkNotCreated(
      MixedOperation<?, ? extends KubernetesResourceList<?>, ?> operation) {
    try {
      assertThat(operation.inNamespace(namespace.getMetadata().getName()).list().getItems())
          .isNullOrEmpty();
    } catch (KubernetesClientException e) {
      // The resource doesn't even exist in the cluster
      assertThat(e.getStatus().getCode()).isEqualTo(404);
    }
  }

  protected void checkNotCreated(
      MixedOperation<?, ? extends KubernetesResourceList<?>, ?> operation, String name) {
    try {
      assertThat(operation.inNamespace(namespace.getMetadata().getName()).withName(name).get())
          .isNull();
    } catch (KubernetesClientException e) {
      // The resource doesn't even exist in the cluster
      assertThat(e.getStatus().getCode()).isEqualTo(404);
    }
  }

  private void awaitUntilAsserted(ThrowingRunnable code, String message) {
    try {
      await()
          .pollInterval(pollInterval())
          .atMost(timeout())
          .untilAsserted(
              () -> {
                try {
                  code.run();
                } catch (AssertionError t) {
                  throw t;
                } catch (Throwable t) {
                  throw new AssertionError(message, t);
                }
              });
    } catch (ConditionTimeoutException e) {
      LOGGER.error(message, e.getCause());
      // clear interrupt flag
      LOGGER.error("Interrupt status:  {}", Thread.interrupted());
      dumpDiagnostics();
      fail(message, e.getCause());
    }
  }

  protected void dumpDiagnostics() {}

  private static void checkMeta(HasMetadata expected, HasMetadata actual) {
    assertThat(actual.getMetadata()).isNotNull();
    assertThat(actual.getMetadata().getLabels())
        .containsAllEntriesOf(expected.getMetadata().getLabels());
    assertThat(actual.getMetadata().getAnnotations())
        .containsAllEntriesOf(expected.getMetadata().getAnnotations());
  }

  private static void checkSpec(HasMetadata expected, HasMetadata actual, String... ignoredFields) {
    assertThat(actual)
        .extracting("spec")
        .usingRecursiveComparison()
        .ignoringExpectedNullFields()
        .ignoringCollectionOrder()
        .ignoringFields(ignoredFields)
        .isNotNull()
        .isEqualTo(Extractors.byName("spec").apply(expected));
  }
}
