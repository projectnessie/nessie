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

import static org.projectnessie.operator.events.EventReason.CreatingMgmtService;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ResourceDiscriminator;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import java.util.Optional;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.dependent.ManagementServiceDependent.Discriminator;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.ServiceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(
    labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR,
    resourceDiscriminator = Discriminator.class)
public class ManagementServiceDependent extends CRUDKubernetesDependentResource<Service, Nessie> {

  public static final int PORT_NUMBER = 9000;

  public static final String PORT_NAME = "nessie-mgmt";

  public static final String SERVICE_NAME_SUFFIX = "-mgmt";

  private static final Logger LOGGER = LoggerFactory.getLogger(ManagementServiceDependent.class);

  public ManagementServiceDependent() {
    super(Service.class);
  }

  @Override
  public Service create(Service desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating management service {} for {}",
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie,
        CreatingMgmtService,
        "Creating management service %s",
        desired.getMetadata().getName() + SERVICE_NAME_SUFFIX);
    return super.create(desired, nessie, context);
  }

  @Override
  public Service desired(Nessie nessie, Context<Nessie> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    ServiceOptions service = nessie.getSpec().service();
    return new ServiceBuilder()
        .withMetadata(
            helper
                .metaBuilder(nessie, managementServiceName(nessie))
                .addToLabels(service.labels())
                .withAnnotations(service.annotations())
                .build())
        .withNewSpec()
        .withClusterIP("None")
        .addNewPort()
        .withName(PORT_NAME)
        .withProtocol("TCP")
        .withPort(PORT_NUMBER)
        .withNewTargetPort()
        .withValue(PORT_NUMBER)
        .endTargetPort()
        .endPort()
        .withSelector(helper.selectorLabels(nessie))
        .withPublishNotReadyAddresses()
        .endSpec()
        .build();
  }

  public static String managementServiceName(Nessie primary) {
    return primary.getMetadata().getName() + SERVICE_NAME_SUFFIX;
  }

  public static class Discriminator implements ResourceDiscriminator<Service, Nessie> {
    @Override
    public Optional<Service> distinguish(
        Class<Service> resource, Nessie primary, Context<Nessie> context) {
      InformerEventSource<Service, Nessie> ies =
          (InformerEventSource<Service, Nessie>)
              context.eventSourceRetriever().getResourceEventSourceFor(Service.class);
      return ies.get(
          new ResourceID(managementServiceName(primary), primary.getMetadata().getNamespace()));
    }
  }
}
