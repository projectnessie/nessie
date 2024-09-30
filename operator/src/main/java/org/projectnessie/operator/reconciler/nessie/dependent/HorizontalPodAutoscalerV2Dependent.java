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
package org.projectnessie.operator.reconciler.nessie.dependent;

import io.fabric8.kubernetes.api.model.autoscaling.v2.CrossVersionObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerSpecBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.MetricSpec;
import io.fabric8.kubernetes.api.model.autoscaling.v2.MetricSpecBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.MetricTargetBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.ResourceMetricSourceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.AutoscalingOptions;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class HorizontalPodAutoscalerV2Dependent
    extends AbstractHorizontalPodAutoscalerDependent<HorizontalPodAutoscaler> {

  public HorizontalPodAutoscalerV2Dependent() {
    super(HorizontalPodAutoscaler.class);
  }

  @Override
  protected HorizontalPodAutoscaler desired(Nessie nessie, Context<Nessie> context) {
    AutoscalingOptions autoscaling = nessie.getSpec().autoscaling();
    HorizontalPodAutoscalerSpecBuilder specBuilder =
        new HorizontalPodAutoscalerSpecBuilder()
            .withScaleTargetRef(
                new CrossVersionObjectReferenceBuilder()
                    .withApiVersion("apps/v1")
                    .withKind("Deployment")
                    .withName(nessie.getMetadata().getName())
                    .build())
            .withMinReplicas(autoscaling.minReplicas())
            .withMaxReplicas(autoscaling.maxReplicas());
    Integer cpu = autoscaling.targetCpuUtilizationPercentage();
    if (cpu != null && cpu > 0) {
      specBuilder.addToMetrics(metric("cpu", cpu));
    }
    Integer memory = autoscaling.targetMemoryUtilizationPercentage();
    if (memory != null && memory > 0) {
      specBuilder.addToMetrics(metric("memory", memory));
    }
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    return new HorizontalPodAutoscalerBuilder()
        .withMetadata(helper.metaBuilder(nessie).build())
        .withSpec(specBuilder.build())
        .build();
  }

  private static MetricSpec metric(String name, int percentage) {
    return new MetricSpecBuilder()
        .withType("Resource")
        .withResource(
            new ResourceMetricSourceBuilder()
                .withName(name)
                .withTarget(
                    new MetricTargetBuilder()
                        .withType("Utilization")
                        .withAverageUtilization(percentage)
                        .build())
                .build())
        .build();
  }

  public static class ActivationCondition
      extends AbstractHorizontalPodAutoscalerDependent.ActivationCondition<
          HorizontalPodAutoscaler> {

    @Override
    protected boolean isAutoscalingSupported(KubernetesHelper helper) {
      return helper.isAutoscalingV2Supported();
    }
  }
}
