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

import static org.projectnessie.operator.events.EventReason.CreatingNessieGc;
import static org.projectnessie.operator.reconciler.nessie.dependent.AbstractServiceAccountDependent.serviceAccountName;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.GcOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.GcOptionsBuilder;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGcBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class NessieGcDependent extends CRUDKubernetesDependentResource<NessieGc, Nessie> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieGcDependent.class);

  public NessieGcDependent() {
    super(NessieGc.class);
  }

  @Override
  public NessieGc create(NessieGc desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating nessiegc {} for {}",
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie, CreatingNessieGc, "Creating nessiegc %s", desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  @Override
  public NessieGc desired(Nessie nessie, Context<Nessie> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    GcOptions gc = nessie.getSpec().gc();
    if (!gc.job().serviceAccount().create() && gc.job().serviceAccount().name() == null) {
      // If no service account should be created, and no name has been provided, use the same
      // service account used for the primary, instead of the default "default" service account
      // that would be inferred otherwise by NessieGcReconciler.
      gc =
          new GcOptionsBuilder(gc)
              .editJob()
              .editServiceAccount()
              .withName(serviceAccountName(nessie, nessie.getSpec().deployment().serviceAccount()))
              .endServiceAccount()
              .endJob()
              .build();
    }
    return new NessieGcBuilder()
        .withMetadata(helper.metaBuilder(nessie, nessie.getMetadata().getName() + "-gc").build())
        .withNewSpec()
        .withNessieRef(new LocalObjectReference(nessie.getMetadata().getName()))
        .withSchedule(gc.schedule())
        .withMark(gc.mark())
        .withSweep(gc.sweep())
        .withDatasource(gc.datasource())
        .withIceberg(gc.iceberg())
        .withJob(gc.job())
        .endSpec()
        .build();
  }

  public static class ActivationCondition implements Condition<NessieGc, Nessie> {

    @Override
    public boolean isMet(
        DependentResource<NessieGc, Nessie> dependentResource,
        Nessie nessie,
        Context<Nessie> context) {
      return nessie.getSpec().gc().enabled();
    }
  }
}
