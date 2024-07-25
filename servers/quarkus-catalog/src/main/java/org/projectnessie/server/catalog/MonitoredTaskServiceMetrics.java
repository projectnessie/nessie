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
package org.projectnessie.server.catalog;

import io.micrometer.core.annotation.Counted;
import jakarta.inject.Singleton;
import org.projectnessie.nessie.tasks.service.impl.TaskServiceMetrics;

@Singleton
public class MonitoredTaskServiceMetrics implements TaskServiceMetrics {
  private static final String PREFIX = "nessie.tasks";

  @Override
  @Counted(PREFIX)
  public void startNewTaskController() {}

  @Override
  public void taskAttempt() {}

  @Override
  @Counted(PREFIX)
  public void taskAttemptFinalSuccess() {}

  @Override
  @Counted(PREFIX)
  public void taskAttemptFinalFailure() {}

  @Override
  public void taskAttemptRunning() {}

  @Override
  public void taskAttemptErrorRetry() {}

  @Override
  public void taskAttemptRecover() {}

  @Override
  public void taskCreation() {}

  @Override
  @Counted(PREFIX)
  public void taskCreationRace() {}

  @Override
  public void taskCreationUnhandled() {}

  @Override
  public void taskAttemptUnhandled() {}

  @Override
  @Counted(PREFIX)
  public void taskRetryStateChangeSucceeded() {}

  @Override
  @Counted(PREFIX)
  public void taskRetryStateChangeRace() {}

  @Override
  @Counted(PREFIX)
  public void taskExecution() {}

  @Override
  @Counted(PREFIX)
  public void taskExecutionFinished() {}

  @Override
  @Counted(PREFIX)
  public void taskExecutionResult() {}

  @Override
  @Counted(PREFIX)
  public void taskExecutionResultRace() {}

  @Override
  @Counted(PREFIX)
  public void taskExecutionRetryableError() {}

  @Override
  @Counted(PREFIX)
  public void taskExecutionFailure() {}

  @Override
  @Counted(PREFIX)
  public void taskExecutionFailureRace() {}

  @Override
  public void taskExecutionUnhandled() {}

  @Override
  @Counted(PREFIX)
  public void taskLossDetected() {}

  @Override
  @Counted(PREFIX)
  public void taskLostReassigned() {}

  @Override
  @Counted(PREFIX)
  public void taskLostReassignRace() {}

  @Override
  public void taskUpdateRunningState() {}

  @Override
  public void taskRunningStateUpdated() {}

  @Override
  @Counted(PREFIX)
  public void taskRunningStateUpdateRace() {}

  @Override
  public void taskRunningStateUpdateNoLongerRunning() {}

  @Override
  @Counted(PREFIX)
  public void taskHasFinalFailure() {}

  @Override
  @Counted(PREFIX)
  public void taskHasFinalSuccess() {}
}
