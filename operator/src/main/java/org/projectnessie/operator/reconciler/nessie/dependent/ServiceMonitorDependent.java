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

import static org.projectnessie.operator.events.EventReason.CreatingServiceMonitor;
import static org.projectnessie.operator.events.EventReason.ServiceMonitorNotSupported;

import io.fabric8.openshift.api.model.monitoring.v1.ServiceMonitor;
import io.fabric8.openshift.api.model.monitoring.v1.ServiceMonitorBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class ServiceMonitorDependent
    extends CRUDKubernetesDependentResource<ServiceMonitor, Nessie> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMonitorDependent.class);

  public ServiceMonitorDependent() {
    super(ServiceMonitor.class);
  }

  @Override
  public ServiceMonitor create(ServiceMonitor desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating service monitor {} for {}",
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie,
        CreatingServiceMonitor,
        "Creating service monitor %s",
        desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  @Override
  public ServiceMonitor desired(Nessie nessie, Context<Nessie> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    return new ServiceMonitorBuilder()
        .withMetadata(
            helper.metaBuilder(nessie).addToLabels(nessie.getSpec().monitoring().labels()).build())
        .withNewSpec()
        .addNewEndpoint()
        .withPort(ManagementServiceDependent.PORT_NAME)
        .withScheme("http")
        .withInterval(nessie.getSpec().monitoring().interval())
        .withPath("/q/metrics")
        .endEndpoint()
        .withNewNamespaceSelector()
        .withMatchNames(nessie.getMetadata().getNamespace())
        .endNamespaceSelector()
        .withNewSelector()
        .withMatchLabels(helper.selectorLabels(nessie))
        .endSelector()
        .endSpec()
        .build();
  }

  public static class ActivationCondition implements Condition<ServiceMonitor, Nessie> {

    @Override
    public boolean isMet(
        DependentResource<ServiceMonitor, Nessie> dependentResource,
        Nessie nessie,
        Context<Nessie> context) {
      boolean conditionMet = nessie.getSpec().monitoring().enabled();
      KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
      if (conditionMet && !helper.isMonitoringSupported()) {
        EventService.retrieveFromContext(context)
            .fireEvent(
                nessie,
                ServiceMonitorNotSupported,
                "Service monitor creation requested, but monitoring is not supported");
        conditionMet = false;
      }
      return conditionMet;
    }
  }
}
