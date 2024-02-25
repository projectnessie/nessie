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
import static org.projectnessie.operator.reconciler.nessie.NessieReconciler.NESSIE_SERVICES_EVENT_SOURCE;

import io.fabric8.kubernetes.api.model.Service;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.quarkiverse.operatorsdk.annotations.RBACRule;
import java.util.Map;
import org.projectnessie.operator.reconciler.AbstractReconciler;
import org.projectnessie.operator.reconciler.nessie.dependent.ConfigMapDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.DeploymentDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.HorizontalPodAutoscalerV2Beta1Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.HorizontalPodAutoscalerV2Beta2Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.HorizontalPodAutoscalerV2Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.IngressV1Beta1Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.IngressV1Dependent;
import org.projectnessie.operator.reconciler.nessie.dependent.MainServiceDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.ManagementServiceDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.PersistentVolumeClaimDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.ServiceAccountDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.ServiceMonitorDependent;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.NessieStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      @Dependent(
          name = "service",
          type = MainServiceDependent.class,
          useEventSourceWithName = NESSIE_SERVICES_EVENT_SOURCE,
          dependsOn = "deployment"),
      @Dependent(
          name = "service-mgmt",
          type = ManagementServiceDependent.class,
          useEventSourceWithName = NESSIE_SERVICES_EVENT_SOURCE,
          dependsOn = "deployment"),
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
      @Dependent(
          name = "service-monitor",
          type = ServiceMonitorDependent.class,
          dependsOn = "service-mgmt",
          activationCondition = ServiceMonitorDependent.ActivationCondition.class),
    })
@RBACRule(apiGroups = "", resources = "events", verbs = RBACRule.ALL)
public class NessieReconciler extends AbstractReconciler<Nessie>
    implements EventSourceInitializer<Nessie> {

  public static final String NAME = "nessie-controller";

  public static final String DEPENDENT_RESOURCES_SELECTOR = "app.kubernetes.io/managed-by=" + NAME;

  public static final String NESSIE_SERVICES_EVENT_SOURCE = "NessieServicesEventSource";

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieReconciler.class);

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<Nessie> context) {
    InformerEventSource<Service, Nessie> ies =
        new InformerEventSource<>(
            InformerConfiguration.from(Service.class, context).build(), context);
    return Map.of(NESSIE_SERVICES_EVENT_SOURCE, ies);
  }

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
