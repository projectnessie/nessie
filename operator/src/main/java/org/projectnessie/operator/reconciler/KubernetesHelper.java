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

import io.fabric8.kubernetes.api.model.APIGroup;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessiegc.NessieGcReconciler;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;
import org.projectnessie.operator.utils.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
public final class KubernetesHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesHelper.class);

  private static final String HELPER_CONTEXT_KEY = "kube-helper";

  public static KubernetesHelper retrieveFromContext(Context<?> context) {
    return context
        .managedDependentResourceContext()
        .getMandatory(HELPER_CONTEXT_KEY, KubernetesHelper.class);
  }

  public static void storeInContext(Context<?> context, KubernetesHelper kubernetesHelper) {
    context.managedDependentResourceContext().put(HELPER_CONTEXT_KEY, kubernetesHelper);
  }

  private final KubernetesClient client;
  private final String operatorVersion;

  @Inject
  public KubernetesHelper(
      @SuppressWarnings("CdiInjectionPointsInspection") KubernetesClient client,
      @ConfigProperty(name = "quarkus.application.version") String operatorVersion) {
    this.client = client;
    this.operatorVersion = operatorVersion;
  }

  @Startup
  public void logStartupInfo() {
    LOGGER.info("Nessie operator version: {}", getOperatorVersion());
    LOGGER.info(
        "Kubernetes cluster version: {}.{}",
        getKubernetesVersion().getMajor(),
        getKubernetesVersion().getMinor());
  }

  public VersionInfo getKubernetesVersion() {
    return client.getKubernetesVersion();
  }

  public String getOperatorVersion() {
    return operatorVersion;
  }

  /**
   * Create metadata for a dependent resource. The dependent resource name will be identical to the
   * primary resource name.
   */
  public ObjectMetaBuilder metaBuilder(HasMetadata primary) {
    return metaBuilder(primary, primary.getMetadata().getName());
  }

  /**
   * Create metadata for a dependent resource with the given name and all recommended meta labels.
   *
   * @see <a
   *     href="https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/">Recommended
   *     Labels</a>
   */
  public ObjectMetaBuilder metaBuilder(HasMetadata primary, String name) {
    ResourceUtils.validateName(name);
    return new ObjectMetaBuilder()
        .withName(name)
        .withNamespace(primary.getMetadata().getNamespace())
        .withLabels(selectorLabels(primary))
        .addToLabels(
            Map.of(
                "app.kubernetes.io/version",
                operatorVersion,
                "app.kubernetes.io/component",
                "nessie",
                "app.kubernetes.io/part-of",
                "nessie",
                "app.kubernetes.io/managed-by",
                managedBy(primary)));
  }

  /**
   * Defines the value of the "app.kubernetes.io/managed-by" label. This label is special because it
   * is used as a label selector to watch secondary dependent resources.
   */
  public static String managedBy(HasMetadata primary) {
    return switch (primary.getKind()) {
      case Nessie.KIND -> NessieReconciler.NAME;
      case NessieGc.KIND -> NessieGcReconciler.NAME;
      default ->
          throw new IllegalArgumentException("Unsupported primary resource: " + primary.getKind());
    };
  }

  /**
   * Create selector labels for the given primary resource. These labels are suitable for use when
   * selecting pods belonging to this primary, e.g. in deployments, services and service monitors.
   */
  public Map<String, String> selectorLabels(HasMetadata primary) {
    return Map.of(
        "app.kubernetes.io/name",
        primary.getSingular(),
        "app.kubernetes.io/instance",
        primary.getMetadata().getName());
  }

  public boolean isApiSupported(String apiGroup, String apiVersion) {
    APIGroup group = client.getApiGroup(apiGroup);
    boolean supported = false;
    if (group != null) {
      supported = group.getVersions().stream().anyMatch(v -> v.getVersion().equals(apiVersion));
    }
    LOGGER.debug("API {}/{} supported: {}", apiGroup, apiVersion, supported);
    return supported;
  }

  public boolean isMonitoringSupported() {
    return isApiSupported("monitoring.coreos.com", "v1");
  }

  public boolean isIngressV1Supported() {
    return isApiSupported("networking.k8s.io", "v1");
  }

  public boolean isIngressV1Beta1Supported() {
    return !isIngressV1Supported() && isApiSupported("networking.k8s.io", "v1beta1");
  }

  public boolean isAutoscalingV2Supported() {
    return isApiSupported("autoscaling", "v2");
  }

  public boolean isAutoscalingV2Beta2Supported() {
    return !isAutoscalingV2Supported() && isApiSupported("autoscaling", "v2beta2");
  }

  public boolean isAutoscalingV2Beta1Supported() {
    return !isAutoscalingV2Beta2Supported() && isApiSupported("autoscaling", "v2beta1");
  }
}
