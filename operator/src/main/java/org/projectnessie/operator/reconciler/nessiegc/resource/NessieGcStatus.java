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
package org.projectnessie.operator.reconciler.nessiegc.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.batch.v1.CronJobStatus;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.javaoperatorsdk.operator.api.ObservedGenerationAwareStatus;
import io.sundr.builder.annotations.Buildable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.projectnessie.operator.utils.EventUtils;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
public class NessieGcStatus extends ObservedGenerationAwareStatus {

  public enum JobState {
    JobPending,
    JobComplete,
    JobFailed
  }

  public enum CronJobState {
    CronJobPending,
    CronJobActive,
    CronJobSuspended
  }

  @PrinterColumn(name = "Ready")
  private boolean ready;

  @PrinterColumn(name = "Job State")
  private JobState jobState;

  @PrinterColumn(name = "CronJob State")
  private CronJobState cronJobState;

  @JsonInclude(Include.NON_EMPTY)
  private List<Condition> conditions = new ArrayList<>();

  private LocalObjectReference jobRef;

  private LocalObjectReference cronJobRef;

  private JobStatus jobStatus;

  private CronJobStatus cronJobStatus;

  public boolean isReady() {
    return ready;
  }

  public void setReady(boolean ready) {
    this.ready = ready;
    setCondition(
        new ConditionBuilder()
            .withLastTransitionTime(EventUtils.formatTime(ZonedDateTime.now()))
            .withType("Ready")
            .withStatus(ready ? "True" : "False")
            .withMessage(ready ? "NessieGc is ready" : "NessieGc is not ready")
            .withReason(ready ? "NessieGcReady" : "NessieGcNotReady")
            .build());
  }

  public JobState getJobState() {
    return jobState;
  }

  public void setJobState(JobState jobState) {
    this.jobState = jobState;
  }

  public CronJobState getCronJobState() {
    return cronJobState;
  }

  public void setCronJobState(CronJobState cronJobState) {
    this.cronJobState = cronJobState;
  }

  public List<Condition> getConditions() {
    return conditions;
  }

  public void setConditions(List<Condition> conditions) {
    this.conditions = conditions;
  }

  @JsonIgnore
  public void setCondition(Condition condition) {
    conditions.removeIf(c -> c.getType().equals(condition.getType()));
    conditions.add(condition);
  }

  public LocalObjectReference getJobRef() {
    return jobRef;
  }

  public void setJobRef(LocalObjectReference jobRef) {
    this.jobRef = jobRef;
  }

  public void setCronJobRef(LocalObjectReference cronJobRef) {
    this.cronJobRef = cronJobRef;
  }

  public LocalObjectReference getCronJobRef() {
    return cronJobRef;
  }

  public void setJobStatus(JobStatus jobStatus) {
    this.jobStatus = jobStatus;
  }

  public JobStatus getJobStatus() {
    return jobStatus;
  }

  public void setCronJobStatus(CronJobStatus cronJobStatus) {
    this.cronJobStatus = cronJobStatus;
  }

  public CronJobStatus getCronJobStatus() {
    return cronJobStatus;
  }
}
