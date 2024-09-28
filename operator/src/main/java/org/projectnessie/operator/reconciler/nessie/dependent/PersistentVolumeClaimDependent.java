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

import static org.projectnessie.operator.events.EventReason.CreatingPersistentVolumeClaim;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimSpec;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimSpecBuilder;
import io.fabric8.kubernetes.api.model.VolumeResourceRequirementsBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import java.util.Map;
import java.util.Objects;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.RocksDbOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class PersistentVolumeClaimDependent
    extends CRUDKubernetesDependentResource<PersistentVolumeClaim, Nessie> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PersistentVolumeClaimDependent.class);

  public PersistentVolumeClaimDependent() {
    super(PersistentVolumeClaim.class);
  }

  @Override
  public PersistentVolumeClaim create(
      PersistentVolumeClaim desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating pvc {} for {}", desired.getMetadata().getName(), nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie, CreatingPersistentVolumeClaim, "Creating PVC %s", desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  @Override
  public PersistentVolumeClaim desired(Nessie nessie, Context<Nessie> context) {
    RocksDbOptions rocksDb = nessie.getSpec().versionStore().rocksDb();
    Objects.requireNonNull(rocksDb, "rocksDb config must not be null");
    PersistentVolumeClaimSpec volumeClaimSpec =
        new PersistentVolumeClaimSpecBuilder()
            .withAccessModes("ReadWriteOnce")
            .withStorageClassName(rocksDb.storageClassName())
            .withResources(
                new VolumeResourceRequirementsBuilder()
                    .withRequests(Map.of("storage", rocksDb.storageSize()))
                    .build())
            .build();
    if (rocksDb.selectorLabels() != null && !rocksDb.selectorLabels().isEmpty()) {
      volumeClaimSpec.setSelector(
          new LabelSelectorBuilder().withMatchLabels(rocksDb.selectorLabels()).build());
    }
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    return new PersistentVolumeClaimBuilder()
        .withMetadata(helper.metaBuilder(nessie).build())
        .withSpec(volumeClaimSpec)
        .build();
  }

  public static boolean isBound(PersistentVolumeClaim pvc) {
    return pvc.getStatus() != null && Objects.equals(pvc.getStatus().getPhase(), "Bound");
  }

  public static class ActivationCondition implements Condition<PersistentVolumeClaim, Nessie> {
    @Override
    public boolean isMet(
        DependentResource<PersistentVolumeClaim, Nessie> dependentResource,
        Nessie nessie,
        Context<Nessie> context) {
      boolean conditionMet = nessie.getSpec().versionStore().type().requiresPvc();
      LOGGER.debug("PVC activation condition met: {}", conditionMet);
      return conditionMet;
    }
  }

  public static class ReadyCondition implements Condition<PersistentVolumeClaim, Nessie> {
    @Override
    public boolean isMet(
        DependentResource<PersistentVolumeClaim, Nessie> dependentResource,
        Nessie nessie,
        Context<Nessie> context) {
      boolean conditionMet =
          context
              .getSecondaryResource(PersistentVolumeClaim.class)
              .map(PersistentVolumeClaimDependent::isBound)
              .orElse(false);
      LOGGER.debug("PVC is ready: {}", conditionMet);
      return conditionMet;
    }
  }
}
