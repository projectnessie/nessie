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
package org.projectnessie.operator.reconciler;

import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ContextInitializer;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.dependent.workflow.WorkflowReconcileResult;
import io.quarkiverse.operatorsdk.annotations.CSVMetadata;
import io.quarkiverse.operatorsdk.annotations.CSVMetadata.Icon;
import io.quarkiverse.operatorsdk.annotations.CSVMetadata.Provider;
import io.quarkiverse.operatorsdk.annotations.SharedCSVMetadata;
import jakarta.inject.Inject;
import org.projectnessie.operator.events.EventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CSVMetadata(
    bundleName = "nessie-operator",
    icon = @Icon(fileName = "nessie.svg"),
    provider = @Provider(name = "Project Nessie", url = "https://projectnessie.org"))
public abstract class AbstractReconciler<T extends HasMetadata>
    implements Reconciler<T>,
        ContextInitializer<T>,
        Cleaner<T>,
        ErrorStatusHandler<T>,
        SharedCSVMetadata {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReconciler.class);

  @Inject protected KubernetesHelper kubernetesHelper;
  @Inject protected EventService eventService;

  @Override
  public void initContext(T primary, Context<T> context) {
    LOGGER.debug("Starting reconciliation");
    if (!primary.isMarkedForDeletion()) {
      validate(primary);
    }
    KubernetesHelper.storeInContext(context, kubernetesHelper);
    EventService.storeInContext(context, eventService);
  }

  @Override
  public UpdateControl<T> reconcile(T primary, Context<T> context) {
    boolean ready =
        context
            .managedDependentResourceContext()
            .getWorkflowReconcileResult()
            .map(wrr -> checkDependentsReady(primary, wrr))
            .orElse(false);
    LOGGER.debug("Dependents ready? {}", ready);
    if (ready && !isReady(primary)) {
      eventService.fireEvent(primary, ReconcileSuccess, "Reconciled successfully");
    }
    updatePrimaryStatus(primary, context, ready);
    // Note: patch may accidentally result in duplicate elements in collections, esp. conditions
    return UpdateControl.updateStatus(primary);
  }

  @Override
  public ErrorStatusUpdateControl<T> updateErrorStatus(
      T primary, Context<T> context, Exception error) {
    LOGGER.error("Reconcile failed unexpectedly", error);
    eventService.fireErrorEvent(primary, error);
    updatePrimaryStatus(primary, context, false);
    return ErrorStatusUpdateControl.updateStatus(primary);
  }

  @Override
  public DeleteControl cleanup(T primary, Context<T> context) {
    LOGGER.debug("Resource deleted");
    eventService.clearEvents(primary);
    return DeleteControl.defaultDelete();
  }

  protected boolean checkDependentsReady(T primary, WorkflowReconcileResult wrr) {
    if (wrr.erroredDependentsExist()) {
      wrr.getErroredDependents()
          .values()
          .forEach(error -> eventService.fireErrorEvent(primary, error));
    }
    return wrr.allDependentResourcesReady();
  }

  protected abstract void validate(T primary);

  protected abstract boolean isReady(T primary);

  protected abstract void updatePrimaryStatus(T nessie, Context<T> context, boolean ready);
}
