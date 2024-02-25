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

import static org.projectnessie.operator.events.EventReason.CreatingHPA;

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

public class AbstractHorizontalPodAutoscalerDependent<HPA extends HasMetadata>
    extends CRUDKubernetesDependentResource<HPA, Nessie> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngressV1Dependent.class);

  protected AbstractHorizontalPodAutoscalerDependent(Class<HPA> resourceClass) {
    super(resourceClass);
  }

  @Override
  public HPA create(HPA desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating horizontal pod autoscaler {} for {}",
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie,
        CreatingHPA,
        "Creating horizontal pod autoscaler %s",
        desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  public abstract static class ActivationCondition<HPA extends HasMetadata>
      implements Condition<HPA, Nessie> {

    @Override
    public boolean isMet(
        DependentResource<HPA, Nessie> dependentResource, Nessie nessie, Context<Nessie> context) {
      if (nessie.getSpec().autoscaling().enabled()
          && nessie.getSpec().versionStore().type().supportsMultipleReplicas()) {
        KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
        return isAutoscalingSupported(helper);
      }
      return false;
    }

    protected abstract boolean isAutoscalingSupported(KubernetesHelper helper);
  }
}
