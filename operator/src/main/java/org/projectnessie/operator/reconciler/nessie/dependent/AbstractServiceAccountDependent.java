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

import static org.projectnessie.operator.events.EventReason.CreatingServiceAccount;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.resource.options.ServiceAccountOptions;
import org.projectnessie.operator.utils.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractServiceAccountDependent<P extends HasMetadata>
    extends CRUDKubernetesDependentResource<ServiceAccount, P> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractServiceAccountDependent.class);

  public AbstractServiceAccountDependent() {
    super(ServiceAccount.class);
  }

  @Override
  public ServiceAccount create(ServiceAccount desired, P primary, Context<P> context) {
    LOGGER.debug(
        "Creating service account {} for {} {}",
        desired.getMetadata().getName(),
        primary.getSingular(),
        primary.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        primary,
        CreatingServiceAccount,
        "Creating service account %s",
        desired.getMetadata().getName());
    return super.create(desired, primary, context);
  }

  protected ServiceAccount desired(
      P primary, ServiceAccountOptions serviceAccount, Context<P> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    ObjectMeta metadata =
        helper
            .metaBuilder(primary, serviceAccountName(primary, serviceAccount))
            .withAnnotations(serviceAccount.annotations())
            .build();
    return new ServiceAccountBuilder().withMetadata(metadata).build();
  }

  public static String serviceAccountName(
      HasMetadata primary, ServiceAccountOptions serviceAccount) {
    if (serviceAccount.name() != null) {
      ResourceUtils.validateName(serviceAccount.name());
      return serviceAccount.name();
    } else if (serviceAccount.create()) {
      return primary.getMetadata().getName();
    }
    return "default";
  }

  public abstract static class ActivationCondition<P extends HasMetadata>
      implements Condition<ServiceAccount, P> {

    @Override
    public boolean isMet(
        DependentResource<ServiceAccount, P> dependentResource, P primary, Context<P> context) {
      return serviceAccount(primary).create();
    }

    protected abstract ServiceAccountOptions serviceAccount(P primary);
  }
}
