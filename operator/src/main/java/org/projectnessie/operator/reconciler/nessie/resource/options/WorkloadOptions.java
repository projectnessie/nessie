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
package org.projectnessie.operator.reconciler.nessie.resource.options;

import static org.projectnessie.operator.reconciler.nessie.resource.options.ProbeOptions.DEFAULT_LIVENESS_PROBE_OPTIONS;
import static org.projectnessie.operator.reconciler.nessie.resource.options.ProbeOptions.DEFAULT_READINESS_PROBE_OPTIONS;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Toleration;
import io.sundr.builder.annotations.Buildable;
import java.util.List;
import java.util.Map;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record WorkloadOptions(
    @JsonPropertyDescription("The image to use for the main container.") @Default("{}")
        ImageOptions image,
    @JsonPropertyDescription("Service account options.") @Default("{}")
        ServiceAccountOptions serviceAccount,
    @JsonPropertyDescription(
            """
            The resources to allocate to the main container. \
            Note: by default, Nessie servers are configured to use 70% of the available memory.""")
        @Nullable
        @jakarta.annotation.Nullable
        ResourceRequirements resources,
    @JsonPropertyDescription(
            """
            The liveness probe options for the main container. \
            See https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/.""")
        @Default(
            """
            { "initialDelaySeconds": 2, "periodSeconds": 30, "timeoutSeconds": 10, "successThreshold": 1, "failureThreshold": 3}""")
        ProbeOptions livenessProbe,
    @JsonPropertyDescription(
            """
            The readiness probe options for the main container. \
            See https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/.""")
        @Default(
            """
            { "initialDelaySeconds": 3, "periodSeconds": 45, "timeoutSeconds": 10, "successThreshold": 1, "failureThreshold": 3}""")
        ProbeOptions readinessProbe,
    @JsonPropertyDescription(
            """
            Node labels which must match for the pod to be scheduled on that node. \
            See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector.""")
        @Default("{}")
        Map<String, String> nodeSelector,
    @JsonPropertyDescription(
            """
            Tolerations for the pod. \
            See https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/.""")
        @Default("[]")
        List<Toleration> tolerations,
    @JsonPropertyDescription(
            """
            Affinity rules for the pod. \
            See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity.""")
        @Default("{}")
        Affinity affinity,
    @JsonPropertyDescription("Additional pod labels.") @Default("{}") Map<String, String> labels,
    @JsonPropertyDescription("Additional pod annotations.") @Default("{}")
        Map<String, String> annotations,
    @JsonPropertyDescription(
            """
            Security context for the pod. \
            See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/.""")
        @Default("{}")
        PodSecurityContext podSecurityContext,
    @JsonPropertyDescription(
            """
            Security context for the container. \
            See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/.""")
        @Default("{}")
        SecurityContext containerSecurityContext) {

  public WorkloadOptions() {
    this(null, null, null, null, null, null, null, null, null, null, null, null);
  }

  public WorkloadOptions {
    image = image != null ? image : new ImageOptions();
    serviceAccount = serviceAccount != null ? serviceAccount : new ServiceAccountOptions();
    resources = resources != null ? resources : new ResourceRequirements();
    livenessProbe = livenessProbe != null ? livenessProbe : DEFAULT_LIVENESS_PROBE_OPTIONS;
    readinessProbe = readinessProbe != null ? readinessProbe : DEFAULT_READINESS_PROBE_OPTIONS;
    nodeSelector = nodeSelector != null ? Map.copyOf(nodeSelector) : Map.of();
    tolerations = tolerations != null ? tolerations : List.of();
    affinity = affinity != null ? affinity : new Affinity();
    labels = labels != null ? Map.copyOf(labels) : Map.of();
    annotations = annotations != null ? Map.copyOf(annotations) : Map.of();
    podSecurityContext = podSecurityContext != null ? podSecurityContext : new PodSecurityContext();
    containerSecurityContext =
        containerSecurityContext != null ? containerSecurityContext : new SecurityContext();
  }
}
