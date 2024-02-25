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

import static io.fabric8.kubernetes.api.model.HasMetadata.getVersion;
import static org.projectnessie.operator.events.EventReason.CreatingIngress;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIngressDependent<I extends HasMetadata>
    extends CRUDKubernetesDependentResource<I, Nessie> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIngressDependent.class);

  protected AbstractIngressDependent(Class<I> resourceClass) {
    super(resourceClass);
  }

  @Override
  public I create(I desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating ingress {} {} for {}",
        getVersion(resourceType()),
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie, CreatingIngress, "Creating ingress %s", desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  public abstract static class ActivationCondition<I extends HasMetadata>
      implements Condition<I, Nessie> {

    private final String networkingVersion;

    protected ActivationCondition(String networkingVersion) {
      this.networkingVersion = networkingVersion;
    }

    @Override
    public boolean isMet(
        DependentResource<I, Nessie> dependentResource, Nessie nessie, Context<Nessie> context) {
      boolean conditionMet = false;
      if (nessie.getSpec().ingress().enabled()) {
        KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
        conditionMet = helper.isApiSupported("networking.k8s.io", networkingVersion);
      }
      LOGGER.debug("Ingress {} activation condition met? {}", networkingVersion, conditionMet);
      return conditionMet;
    }
  }

  public abstract static class ReadyCondition<I extends HasMetadata>
      implements Condition<I, Nessie> {

    private final Class<I> resourceClass;

    protected ReadyCondition(Class<I> resourceClass) {
      this.resourceClass = resourceClass;
    }

    @Override
    public boolean isMet(
        DependentResource<I, Nessie> dependentResource, Nessie nessie, Context<Nessie> context) {
      boolean conditionMet =
          context.getSecondaryResource(resourceClass).map(this::checkIngressReady).orElse(false);
      LOGGER.debug("Ingress is ready? {}", conditionMet);
      return conditionMet;
    }

    protected abstract boolean checkIngressReady(I ingress);
  }
}
