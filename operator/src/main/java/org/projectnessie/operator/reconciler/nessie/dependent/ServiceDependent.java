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

import static org.projectnessie.operator.events.EventReason.CreatingService;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.ServiceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class ServiceDependent extends CRUDKubernetesDependentResource<Service, Nessie> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDependent.class);

  public ServiceDependent() {
    super(Service.class);
  }

  @Override
  public Service create(Service desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating service {} for {}",
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie, CreatingService, "Creating service %s", desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  @Override
  public Service desired(Nessie nessie, Context<Nessie> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    ServiceOptions service = nessie.getSpec().service();
    return new ServiceBuilder()
        .withMetadata(
            helper
                .metaBuilder(nessie)
                .addToLabels(service.labels())
                .withAnnotations(service.annotations())
                .build())
        .withNewSpec()
        .withType(service.type().name())
        .addNewPort()
        .withName("nessie-server")
        .withProtocol("TCP")
        .withPort(service.port())
        .withNewTargetPort()
        .withValue(ServiceOptions.DEFAULT_NESSIE_PORT)
        .endTargetPort()
        .withNodePort(service.nodePort())
        .endPort()
        .withSelector(helper.selectorLabels(nessie))
        .withSessionAffinity(service.sessionAffinity().name())
        .endSpec()
        .build();
  }
}
