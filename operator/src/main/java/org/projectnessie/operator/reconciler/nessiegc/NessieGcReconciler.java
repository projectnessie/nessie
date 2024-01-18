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
package org.projectnessie.operator.reconciler.nessiegc;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.WATCH_ALL_NAMESPACES;

import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.quarkiverse.operatorsdk.annotations.RBACRule;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.reconciler.AbstractReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessiegc.dependent.CronJobDependent;
import org.projectnessie.operator.reconciler.nessiegc.dependent.JobDependent;
import org.projectnessie.operator.reconciler.nessiegc.dependent.NessieDependent;
import org.projectnessie.operator.reconciler.nessiegc.dependent.ServiceAccountDependent;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGcStatus;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGcStatus.CronJobState;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGcStatus.JobState;
import org.projectnessie.operator.utils.EventUtils;

@ControllerConfiguration(
    name = NessieGcReconciler.NAME,
    namespaces = WATCH_ALL_NAMESPACES,
    dependents = {
      @Dependent(
          name = "nessie",
          type = NessieDependent.class,
          reconcilePrecondition = NessieDependent.ReconcilePrecondition.class,
          useEventSourceWithName = NessieGcReconciler.NESSIE_EVENT_SOURCE),
      @Dependent(
          name = "service-account",
          type = ServiceAccountDependent.class,
          activationCondition = ServiceAccountDependent.ActivationCondition.class),
      @Dependent(
          name = "gc-job",
          type = JobDependent.class,
          activationCondition = JobDependent.ActivationCondition.class,
          reconcilePrecondition = JobDependent.ReconcilePrecondition.class,
          readyPostcondition = JobDependent.ReadyCondition.class),
      @Dependent(
          name = "cron-job",
          type = CronJobDependent.class,
          activationCondition = CronJobDependent.ActivationCondition.class,
          reconcilePrecondition = CronJobDependent.ReconcilePrecondition.class),
    })
@RBACRule(apiGroups = "", resources = "events", verbs = RBACRule.ALL)
public class NessieGcReconciler extends AbstractReconciler<NessieGc>
    implements EventSourceInitializer<NessieGc> {

  public static final String NAME = "nessie-gc-controller";

  public static final String DEPENDENT_RESOURCES_SELECTOR = "app.kubernetes.io/managed-by=" + NAME;

  private static final String NESSIE_INDEX = "NessieIndex";
  public static final String NESSIE_EVENT_SOURCE = "NessieEventSource";

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<NessieGc> context) {
    context
        .getPrimaryCache()
        .addIndexer(
            NESSIE_INDEX,
            primary ->
                List.of(
                    indexKey(
                        primary.getSpec().nessieRef().getName(),
                        primary.getMetadata().getNamespace())));
    var nessieEventSource =
        new InformerEventSource<>(
            InformerConfiguration.from(Nessie.class, context)
                .withPrimaryToSecondaryMapper(
                    (PrimaryToSecondaryMapper<NessieGc>)
                        primary ->
                            Collections.singleton(
                                new ResourceID(
                                    primary.getSpec().nessieRef().getName(),
                                    primary.getMetadata().getNamespace())))
                .withSecondaryToPrimaryMapper(
                    secondary ->
                        context
                            .getPrimaryCache()
                            .byIndex(
                                NESSIE_INDEX,
                                indexKey(
                                    secondary.getMetadata().getName(),
                                    secondary.getMetadata().getNamespace()))
                            .stream()
                            .map(ResourceID::fromResource)
                            .collect(Collectors.toSet()))
                .build(),
            context);
    return Map.of(NESSIE_EVENT_SOURCE, nessieEventSource);
  }

  private static String indexKey(String name, String namespace) {
    return name + "#" + namespace;
  }

  @Override
  protected void validate(NessieGc primary) {
    primary.validate();
  }

  @Override
  protected boolean isReady(NessieGc primary) {
    return primary.getStatus() != null && primary.getStatus().isReady();
  }

  @Override
  protected void updatePrimaryStatus(NessieGc nessieGc, Context<NessieGc> context, boolean ready) {
    NessieGcStatus oldStatus =
        nessieGc.getStatus() == null ? new NessieGcStatus() : nessieGc.getStatus();
    nessieGc.setStatus(new NessieGcStatus());
    nessieGc.getStatus().setReady(ready);
    nessieGc.getStatus().setJobState(JobState.JobPending);
    if (nessieGc.getSpec().schedule() != null) {
      nessieGc.getStatus().setCronJobState(CronJobState.CronJobPending);
    }
    if (context.getSecondaryResource(Nessie.class).isPresent()) {
      if (nessieGc.getSpec().schedule() == null) {
        context.getSecondaryResource(Job.class).ifPresent(job -> updateFromJob(nessieGc, job));
      } else {
        context
            .getSecondaryResource(CronJob.class)
            .ifPresent(cronJob -> updateFromCronJob(nessieGc, cronJob, context));
      }
    }
    JobState jobState = nessieGc.getStatus().getJobState();
    if (jobState == JobState.JobComplete) {
      eventService.fireEvent(
          nessieGc,
          EventReason.GcJobComplete,
          "NessieGc job %s completed successfully",
          nessieGc.getStatus().getJobRef().getName());
    } else if (jobState == JobState.JobFailed) {
      eventService.fireEvent(
          nessieGc,
          EventReason.GcJobFailed,
          "NessieGc job %s failed",
          nessieGc.getStatus().getJobRef().getName());
    }
    CronJobState cronJobState = nessieGc.getStatus().getCronJobState();
    if (cronJobState == CronJobState.CronJobActive) {
      if (oldStatus.getCronJobState() != CronJobState.CronJobActive) {
        eventService.fireEvent(
            nessieGc,
            EventReason.GcCronJobActive,
            "NessieGc cron job %s active",
            nessieGc.getStatus().getCronJobRef().getName());
      }
    } else if (cronJobState == CronJobState.CronJobSuspended) {
      if (oldStatus.getCronJobState() != CronJobState.CronJobSuspended) {
        eventService.fireEvent(
            nessieGc,
            EventReason.GcCronJobSuspended,
            "NessieGc cron job %s is suspended",
            nessieGc.getStatus().getCronJobRef().getName());
      }
    }
  }

  private void updateFromJob(NessieGc nessieGc, Job job) {
    NessieGcStatus status = nessieGc.getStatus();
    LocalObjectReference jobRef = new LocalObjectReference(job.getMetadata().getName());
    status.setJobRef(jobRef);
    status.setJobStatus(job.getStatus());
    boolean complete =
        status.getJobStatus() != null
            && status.getJobStatus().getConditions().stream()
                .filter(c -> c.getType().equals("Complete"))
                .anyMatch(c -> c.getStatus().equals("True"));
    boolean failed =
        status.getJobStatus() != null
            && status.getJobStatus().getConditions().stream()
                .filter(c -> c.getType().equals("Failed"))
                .anyMatch(c -> c.getStatus().equals("True"));
    if (complete) {
      status.setJobState(JobState.JobComplete);
      status.setCondition(
          new ConditionBuilder()
              .withType("Complete")
              .withStatus("True")
              .withReason("NessieGcJobComplete")
              .withMessage("GC job %s completed successfully".formatted(jobRef.getName()))
              .withLastTransitionTime(EventUtils.formatTime(ZonedDateTime.now()))
              .build());
    } else if (failed) {
      status.setJobState(JobState.JobFailed);
      status.setCondition(
          new ConditionBuilder()
              .withType("Failed")
              .withStatus("True")
              .withReason("NessieGcJobFailed")
              .withMessage("GC job %s failed".formatted(jobRef.getName()))
              .withLastTransitionTime(EventUtils.formatTime(ZonedDateTime.now()))
              .build());
    }
  }

  private void updateFromCronJob(NessieGc nessieGc, CronJob cronJob, Context<NessieGc> context) {
    NessieGcStatus status = nessieGc.getStatus();
    LocalObjectReference cronJobRef = new LocalObjectReference(cronJob.getMetadata().getName());
    status.setCronJobRef(cronJobRef);
    status.setCronJobStatus(cronJob.getStatus());
    @SuppressWarnings("resource")
    JobList jobs =
        context
            .getClient()
            .batch()
            .v1()
            .jobs()
            .inNamespace(nessieGc.getMetadata().getNamespace())
            .withLabels(kubernetesHelper.selectorLabels(nessieGc))
            .list();
    jobs.getItems().stream()
        .filter(job -> job.getStatus() != null && job.getStatus().getStartTime() != null)
        .max(Comparator.comparing(job -> job.getStatus().getStartTime()))
        .ifPresentOrElse(
            job -> updateFromJob(nessieGc, job), () -> status.setJobState(JobState.JobPending));
    boolean suspended = cronJob.getSpec().getSuspend() != null && cronJob.getSpec().getSuspend();
    if (suspended) {
      status.setCronJobState(CronJobState.CronJobSuspended);
      status.setCondition(
          new ConditionBuilder()
              .withType("Suspended")
              .withStatus("True")
              .withReason("NessieGcCronJobSuspended")
              .withMessage("GC cron job %s is suspended".formatted(cronJobRef.getName()))
              .withLastTransitionTime(EventUtils.formatTime(ZonedDateTime.now()))
              .build());
    } else {
      status.setCronJobState(CronJobState.CronJobActive);
      status.setCondition(
          new ConditionBuilder()
              .withType("Active")
              .withStatus("True")
              .withReason("NessieGcCronJobActive")
              .withMessage("GC cron job %s is active".formatted(cronJobRef.getName()))
              .withLastTransitionTime(EventUtils.formatTime(ZonedDateTime.now()))
              .build());
    }
  }
}
