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
package org.projectnessie.operator.reconciler.nessiegc.dependent;

import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessiegc.NessieGcReconciler;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;

@KubernetesDependent(labelSelector = NessieGcReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class JobDependent extends AbstractJobDependent<Job> {

  public JobDependent() {
    super(Job.class);
  }

  @Override
  public Job desired(NessieGc nessieGc, Context<NessieGc> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    return new JobBuilder()
        .withMetadata(
            helper
                .metaBuilder(nessieGc)
                // also apply pod labels to the job (but not pod annotations)
                .addToLabels(nessieGc.getSpec().job().labels())
                .build())
        .withSpec(newJobSpec(nessieGc))
        .build();
  }

  public static boolean isJobComplete(Job job) {
    return job.getStatus().getConditions().stream()
        .anyMatch(
            condition ->
                condition.getType().equals("Complete") && condition.getStatus().equals("True"));
  }

  public static class ActivationCondition implements Condition<CronJob, NessieGc> {
    @Override
    public boolean isMet(
        DependentResource<CronJob, NessieGc> dependentResource,
        NessieGc primary,
        Context<NessieGc> context) {
      return primary.getSpec().schedule() == null;
    }
  }

  public static class ReconcilePrecondition implements Condition<CronJob, NessieGc> {
    @Override
    public boolean isMet(
        DependentResource<CronJob, NessieGc> dependentResource,
        NessieGc primary,
        Context<NessieGc> context) {
      return context.getSecondaryResource(Nessie.class).isPresent();
    }
  }

  public static class ReadyCondition implements Condition<Job, NessieGc> {
    @Override
    public boolean isMet(
        DependentResource<Job, NessieGc> dependentResource,
        NessieGc primary,
        Context<NessieGc> context) {
      return dependentResource
          .getSecondaryResource(primary, context)
          .map(JobDependent::isJobComplete)
          .orElse(false);
    }
  }
}
