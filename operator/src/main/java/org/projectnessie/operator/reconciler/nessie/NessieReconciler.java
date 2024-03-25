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

import static io.javaoperatorsdk.operator.api.reconciler.Constants.WATCH_ALL_NAMESPACES;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.quarkiverse.operatorsdk.annotations.CSVMetadata;
import io.quarkiverse.operatorsdk.annotations.CSVMetadata.Icon;
import io.quarkiverse.operatorsdk.annotations.RBACRule;
import org.projectnessie.operator.reconciler.AbstractReconciler;
import org.projectnessie.operator.reconciler.nessie.dependent.ConfigMapDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.DeploymentDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.HorizontalPodAutoscalerV2Beta1Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.HorizontalPodAutoscalerV2Beta2Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.HorizontalPodAutoscalerV2Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.IngressV1Beta1Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.IngressV1Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.PersistentVolumeClaimDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.ServiceAccountDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.ServiceDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.ServiceMonitorDependent;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.NessieStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CSVMetadata(icon = @Icon(fileName = "nessie.svg"))
@ControllerConfiguration(
    name = NessieReconciler.NAME,
    namespaces = WATCH_ALL_NAMESPACES,
    dependents = {
      @Dependent(
          name = "service-account",
          type = ServiceAccountDependent.class,
          activationCondition = ServiceAccountDependent.ActivationCondition.class),
      @Dependent(name = "config-map", type = ConfigMapDependent.class),
      @Dependent(
          name = "pvc",
          type = PersistentVolumeClaimDependent.class,
          activationCondition = PersistentVolumeClaimDependent.ActivationCondition.class,
          readyPostcondition = PersistentVolumeClaimDependent.ReadyCondition.class),
      @Dependent(
          name = "deployment",
          type = DeploymentDependent.class,
          dependsOn = "config-map",
          readyPostcondition = DeploymentDependent.ReadyCondition.class),
      @Dependent(name = "service", type = ServiceDependent.class, dependsOn = "deployment"),
      @Dependent(
          name = "ingress-v1",
          type = IngressV1Dependent.class,
          dependsOn = "service",
          activationCondition = IngressV1Dependent.ActivationCondition.class,
          readyPostcondition = IngressV1Dependent.ReadyCondition.class),
      @Dependent(
          name = "ingress-v1beta1",
          type = IngressV1Beta1Dependent.class,
          dependsOn = "service",
          activationCondition = IngressV1Beta1Dependent.ActivationCondition.class,
          readyPostcondition = IngressV1Beta1Dependent.ReadyCondition.class),
      @Dependent(
          name = "service-monitor",
          type = ServiceMonitorDependent.class,
          dependsOn = "service",
          activationCondition = ServiceMonitorDependent.ActivationCondition.class),
      @Dependent(
          name = "autoscaler-v2",
          type = HorizontalPodAutoscalerV2Dependent.class,
          dependsOn = "deployment",
          activationCondition = HorizontalPodAutoscalerV2Dependent.ActivationCondition.class),
      @Dependent(
          name = "autoscaler-v2beta2",
          type = HorizontalPodAutoscalerV2Beta2Dependent.class,
          dependsOn = "deployment",
          activationCondition = HorizontalPodAutoscalerV2Beta2Dependent.ActivationCondition.class),
      @Dependent(
          name = "autoscaler-v2beta1",
          type = HorizontalPodAutoscalerV2Beta1Dependent.class,
          dependsOn = "deployment",
          activationCondition = HorizontalPodAutoscalerV2Beta1Dependent.ActivationCondition.class),
    })
@RBACRule(apiGroups = "", resources = "events", verbs = RBACRule.ALL)
public class NessieReconciler extends AbstractReconciler<Nessie> {

  public static final String NAME = "nessie-controller";

  public static final String DEPENDENT_RESOURCES_SELECTOR = "app.kubernetes.io/managed-by=" + NAME;

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieReconciler.class);

  @Override
  protected void validate(Nessie nessie) {
    nessie.validate();
  }

  @Override
  protected boolean isReady(Nessie primary) {
    return primary.getStatus() != null && primary.getStatus().isReady();
  }

  @Override
  protected void updatePrimaryStatus(Nessie nessie, Context<Nessie> context, boolean ready) {
    if (nessie.getStatus() == null) {
      nessie.setStatus(new NessieStatus());
    }
    nessie.getStatus().setReady(ready);
    if (ready && nessie.getSpec().ingress().enabled()) {
      try {
        if (kubernetesHelper.isIngressV1Supported()) {
          IngressV1Dependent.updateStatus(nessie, context);
        } else if (kubernetesHelper.isIngressV1Beta1Supported()) {
          IngressV1Beta1Dependent.updateStatus(nessie, context);
        }
      } catch (Exception e) {
        // Can happen if ingress is misconfigured
        LOGGER.warn("Failed to compute Ingress URL", e);
        nessie.getStatus().setExposedUrl(null);
      }
    }
  }
}
