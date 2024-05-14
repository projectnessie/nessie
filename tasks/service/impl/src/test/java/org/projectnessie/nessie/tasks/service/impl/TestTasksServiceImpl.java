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
package org.projectnessie.nessie.tasks.service.impl;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.projectnessie.nessie.tasks.service.TasksServiceConfig.DEFAULT_RACE_WAIT_MILLIS_MAX;
import static org.projectnessie.nessie.tasks.service.TasksServiceConfig.DEFAULT_RACE_WAIT_MILLIS_MIN;
import static org.projectnessie.nessie.tasks.service.tasktypes.BasicTaskBehavior.FRESH_LOST_RETRY_NOT_BEFORE;
import static org.projectnessie.nessie.tasks.service.tasktypes.BasicTaskBehavior.FRESH_RUNNING_RETRY_NOT_BEFORE;
import static org.projectnessie.nessie.tasks.service.tasktypes.BasicTaskBehavior.RETRYABLE_ERROR_NOT_BEFORE;
import static org.projectnessie.nessie.tasks.service.tasktypes.BasicTaskRequest.basicTaskRequest;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_REPOSITORY_ID;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.projectnessie.nessie.tasks.api.TaskObj;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.nessie.tasks.api.Tasks;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.nessie.tasks.service.tasktypes.BasicTaskObj;
import org.projectnessie.nessie.tasks.service.tasktypes.BasicTaskRequest;
import org.projectnessie.nessie.tasks.service.tasktypes.RetryableException;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.threeten.extra.MutableClock;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessieBackend(InmemoryBackendTestFactory.class)
public class TestTasksServiceImpl {
  @NessiePersist static Persist persist;
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void multipleRepos(
      @NessieStoreConfig(name = CONFIG_REPOSITORY_ID, value = "some-other") @NessiePersist
          Persist otherRepo) {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);
    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));

    Tasks tasks1 = service.forPersist(persist);
    Tasks tasks2 = service.forPersist(otherRepo);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage1 = new CompletableFuture<>();
    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage2 = new CompletableFuture<>();

    BasicTaskRequest taskRequest1 = basicTaskRequest("hello", () -> taskCompletionStage1);
    BasicTaskRequest taskRequest2 = basicTaskRequest("hello", () -> taskCompletionStage2);

    // Want the same ObjId to verify that multiple repositories work fine
    soft.assertThat(taskRequest1.objId()).isEqualTo(taskRequest2.objId());

    // Submit task request for repo 1
    CompletableFuture<BasicTaskObj> taskFuture1 = tasks1.submit(taskRequest1).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    // Submit task request for repo 2
    CompletableFuture<BasicTaskObj> taskFuture2 = tasks2.submit(taskRequest2).toCompletableFuture();
    soft.assertThat(taskFuture2).isNotSameAs(taskFuture1);
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    // 2nd request for the same task 1
    CompletableFuture<BasicTaskObj> taskFuture1b =
        tasks1.submit(basicTaskRequest("hello", () -> null)).toCompletableFuture();
    soft.assertThat(taskFuture1b).isSameAs(taskFuture1).isNotSameAs(taskFuture2);
    verifyNoMoreInteractions(metrics);

    // 2nd request for the same task 2
    CompletableFuture<BasicTaskObj> taskFuture2b =
        tasks2.submit(basicTaskRequest("hello", () -> null)).toCompletableFuture();
    soft.assertThat(taskFuture2b).isSameAs(taskFuture2).isNotSameAs(taskFuture1);
    verifyNoMoreInteractions(metrics);

    soft.assertThat(async.doWork()).isEqualTo(2); // task 1 + 2
    soft.assertThat(taskFuture1).isNotDone();
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics, times(2)).taskAttempt(); // task 1 + 2
    verify(metrics, times(2)).taskCreation(); // task 1 + 2
    verify(metrics, times(2)).taskExecution(); // task 1 + 2
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage1.complete(
        BasicTaskObj.builder()
            .id(taskRequest1.objId())
            .taskParameter(taskRequest1.taskParameter())
            .taskResult(taskRequest1.taskParameter() + " finished")
            .taskState(TaskState.successState()));
    soft.assertThat(taskFuture1).isCompleted();
    soft.assertThat(async.doWork()).isEqualTo(1); // task 2
    verify(metrics).taskExecutionFinished(); // task 1
    verify(metrics).taskExecutionResult(); // task 1
    verify(metrics).taskUpdateRunningState(); // task 2
    verify(metrics).taskRunningStateUpdated(); // task 2
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    taskCompletionStage2.complete(
        BasicTaskObj.builder()
            .id(taskRequest2.objId())
            .taskParameter(taskRequest2.taskParameter())
            .taskResult(taskRequest2.taskParameter() + " finished")
            .taskState(TaskState.successState()));
    soft.assertThat(taskFuture2).isCompleted();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verify(metrics).taskExecutionFinished(); // task 2
    verify(metrics).taskExecutionResult(); // task 2
    verifyNoMoreInteractions(metrics);
    reset(metrics);
  }

  @Test
  public void singleServiceSingleConsumer() throws Exception {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));
    Tasks tasks = service.forPersist(persist);
    BasicTaskRequest taskRequest = basicTaskRequest("hello", () -> taskCompletionStage);

    CompletableFuture<BasicTaskObj> taskFuture = tasks.submit(taskRequest).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    // 2nd request for the same task (would fail w/ the `null` completion-stage)
    CompletableFuture<BasicTaskObj> taskFuture2 =
        tasks.submit(basicTaskRequest("hello", () -> null)).toCompletableFuture();
    soft.assertThat(taskFuture2).isSameAs(taskFuture);
    verifyNoMoreInteractions(metrics);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskAttempt();
    verify(metrics).taskCreation();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.complete(
        BasicTaskObj.builder()
            .id(taskRequest.objId())
            .taskParameter(taskRequest.taskParameter())
            .taskResult(taskRequest.taskParameter() + " finished")
            .taskState(TaskState.successState()));
    soft.assertThat(taskFuture).isCompleted();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionResult();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);

    clock.add(1, ChronoUnit.SECONDS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture.get())
        .asInstanceOf(type(BasicTaskObj.class))
        .extracting(BasicTaskObj::taskResult)
        .isEqualTo("hello finished");
    verifyNoMoreInteractions(metrics);

    // Validate the optimization for the cached-object in TaskServiceImpl.execute()
    BasicTaskRequest req = basicTaskRequest("hello", () -> null);
    if (persist.getImmediate(req.objId()) != null) {
      CompletableFuture<BasicTaskObj> followUp = tasks.submit(req).toCompletableFuture();
      soft.assertThat(async.doWork()).isEqualTo(0);
      soft.assertThat(followUp).isCompleted();
      verify(metrics).taskHasFinalSuccess();
      verifyNoMoreInteractions(metrics);
      reset(metrics);
    }
  }

  @Test
  public void singleServiceSingleConsumerFailure() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));
    Tasks tasks = service.forPersist(persist);
    BasicTaskRequest taskRequest = basicTaskRequest("hello", () -> taskCompletionStage);

    CompletableFuture<BasicTaskObj> taskFuture = tasks.submit(taskRequest).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    // 2nd request for the same task (would fail w/ the `null` completion-stage)
    CompletableFuture<BasicTaskObj> taskFuture2 =
        tasks.submit(basicTaskRequest("hello", () -> null)).toCompletableFuture();
    soft.assertThat(taskFuture2).isSameAs(taskFuture);
    verifyNoMoreInteractions(metrics);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskAttempt();
    verify(metrics).taskCreation();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.completeExceptionally(new RuntimeException("failed task"));
    soft.assertThat(taskFuture).isCompletedExceptionally();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionFailure();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);

    clock.add(1, ChronoUnit.SECONDS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);

    // Validate the optimization for the cached-object in TaskServiceImpl.execute()
    BasicTaskRequest req = basicTaskRequest("hello", () -> null);
    if (persist.getImmediate(req.objId()) != null) {
      CompletableFuture<BasicTaskObj> followUp = tasks.submit(req).toCompletableFuture();
      soft.assertThat(async.doWork()).isEqualTo(0);
      soft.assertThat(followUp).isCompletedExceptionally();
      verify(metrics).taskHasFinalFailure();
      verifyNoMoreInteractions(metrics);
      reset(metrics);
    }
  }

  @Test
  public void taskReturnsIllegalNullResult() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));
    Tasks tasks = service.forPersist(persist);
    BasicTaskRequest taskRequest = basicTaskRequest("hello", () -> taskCompletionStage);

    CompletableFuture<BasicTaskObj> taskFuture = tasks.submit(taskRequest).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskAttempt();
    verify(metrics).taskCreation();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.complete(null);
    soft.assertThat(taskFuture).isCompletedExceptionally();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionFailure();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThatThrownBy(taskFuture::get)
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Local task execution return a null object, which is illegal");
  }

  @Test
  public void runningTaskUpdateRace() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));
    Tasks tasks = service.forPersist(persist);
    BasicTaskRequest taskRequest = basicTaskRequest("hello", () -> taskCompletionStage);

    CompletableFuture<BasicTaskObj> taskFuture = tasks.submit(taskRequest).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskAttempt();
    verify(metrics).taskCreation();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    Mockito.doAnswer(
            invocation -> {
              BasicTaskObj obj =
                  persist.fetchTypedObj(taskRequest.objId(), BasicTaskObj.TYPE, BasicTaskObj.class);
              TaskObj updated =
                  BasicTaskObj.builder()
                      .from(obj)
                      .versionToken(obj.versionToken() + "_concurrent_update")
                      .build();
              checkState(persist.updateConditional(obj, updated));
              return null;
            })
        .when(metrics)
        .taskUpdateRunningState();

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdateRace();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.complete(
        BasicTaskObj.builder()
            .id(taskRequest.objId())
            .taskParameter(taskRequest.taskParameter())
            .taskResult(taskRequest.taskParameter() + " finished")
            .taskState(TaskState.successState()));
    soft.assertThat(taskFuture).isCompletedExceptionally();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionResultRace();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThatThrownBy(taskFuture::get)
        .cause()
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageMatching(
            "Failed to update successful task execution result for basic:[0-9a-f]+ in database \\(race condition\\), exposing as a failure");

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);
  }

  @Test
  public void successfulTaskUpdateRace() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));
    Tasks tasks = service.forPersist(persist);
    BasicTaskRequest taskRequest = basicTaskRequest("hello", () -> taskCompletionStage);

    CompletableFuture<BasicTaskObj> taskFuture = tasks.submit(taskRequest).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskAttempt();
    verify(metrics).taskCreation();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    Mockito.doAnswer(
            invocation -> {
              BasicTaskObj obj =
                  persist.fetchTypedObj(taskRequest.objId(), BasicTaskObj.TYPE, BasicTaskObj.class);
              TaskObj updated =
                  BasicTaskObj.builder()
                      .from(obj)
                      .versionToken(obj.versionToken() + "_concurrent_update")
                      .build();
              checkState(persist.updateConditional(obj, updated));
              return null;
            })
        .when(metrics)
        .taskExecutionFinished();

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.complete(
        BasicTaskObj.builder()
            .id(taskRequest.objId())
            .taskParameter(taskRequest.taskParameter())
            .taskResult(taskRequest.taskParameter() + " finished")
            .taskState(TaskState.successState()));
    soft.assertThat(taskFuture).isCompletedExceptionally();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionResultRace();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThatThrownBy(taskFuture::get)
        .cause()
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageMatching(
            "Failed to update successful task execution result for basic:[0-9a-f]+ in database \\(race condition\\), exposing as a failure");

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);
  }

  @Test
  public void failedTaskUpdateRace() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));
    Tasks tasks = service.forPersist(persist);
    BasicTaskRequest taskRequest = basicTaskRequest("hello", () -> taskCompletionStage);

    CompletableFuture<BasicTaskObj> taskFuture = tasks.submit(taskRequest).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskAttempt();
    verify(metrics).taskCreation();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    Mockito.doAnswer(
            invocation -> {
              BasicTaskObj obj =
                  persist.fetchTypedObj(taskRequest.objId(), BasicTaskObj.TYPE, BasicTaskObj.class);
              TaskObj updated =
                  BasicTaskObj.builder()
                      .from(obj)
                      .versionToken(obj.versionToken() + "_concurrent_update")
                      .build();
              checkState(persist.updateConditional(obj, updated));
              return null;
            })
        .when(metrics)
        .taskExecutionFinished();

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.completeExceptionally(new RuntimeException("foo bar"));
    soft.assertThat(taskFuture).isCompletedExceptionally();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionFailureRace();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThatThrownBy(taskFuture::get)
        .cause()
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageMatching(
            "Failed to update failure task execution result for basic:[0-9a-f]+ in database \\(race condition\\)");

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);
  }

  @Test
  public void twoServicesDistributed() throws Exception {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics1 = mock(TaskServiceMetrics.class);
    TaskServiceMetrics metrics2 = mock(TaskServiceMetrics.class);

    TasksServiceImpl service1 = new TasksServiceImpl(async, metrics1, tasksServiceConfig(1));
    Tasks tasks1 = service1.forPersist(persist);
    TasksServiceImpl service2 = new TasksServiceImpl(async, metrics2, tasksServiceConfig(2));
    Tasks tasks2 = service2.forPersist(persist);

    BasicTaskRequest taskRequest1 = basicTaskRequest("hello", () -> taskCompletionStage);
    CompletableFuture<BasicTaskObj> taskFuture1 = tasks1.submit(taskRequest1).toCompletableFuture();
    verify(metrics1).startNewTaskController();
    verifyNoMoreInteractions(metrics1);
    reset(metrics1);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture1).isNotDone();
    verify(metrics1).taskAttempt();
    verify(metrics1).taskCreation();
    verify(metrics1).taskExecution();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);

    clock.add(250, ChronoUnit.MILLIS);
    BasicTaskRequest taskRequest2 = basicTaskRequest("hello", () -> taskCompletionStage);
    CompletableFuture<BasicTaskObj> taskFuture2 = tasks2.submit(taskRequest2).toCompletableFuture();
    verify(metrics2).startNewTaskController();
    verify(metrics2).startNewTaskController();
    verifyNoMoreInteractions(metrics2);
    reset(metrics2);

    soft.assertThat(async.doWork()).isEqualTo(2);
    soft.assertThat(taskFuture1).isNotDone();
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskUpdateRunningState();
    verify(metrics1).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics1);
    verify(metrics2).taskAttempt();
    verify(metrics2).taskAttemptRunning();
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture1).isNotDone();
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskUpdateRunningState();
    verify(metrics1).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.complete(
        BasicTaskObj.builder()
            .id(taskRequest1.objId())
            .taskParameter(taskRequest1.taskParameter())
            .taskResult(taskRequest1.taskParameter() + " finished")
            .taskState(TaskState.successState()));
    soft.assertThat(taskFuture1).isCompleted();
    // Clock did not advance to when the state for the task object is refreshed, so the 2nd future
    // is still not done.
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskExecutionFinished();
    verify(metrics1).taskExecutionResult();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture1).isCompleted();
    // Clock did not advance to when the state for the task object is refreshed, so the 2nd future
    // is still not done.
    soft.assertThat(taskFuture2).isNotDone();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(FRESH_RUNNING_RETRY_NOT_BEFORE);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture1).isCompleted();
    // Clock advance enough to refresh, so the 2nd future is done now.
    soft.assertThat(taskFuture2).isCompleted();
    verifyNoMoreInteractions(metrics1);
    verify(metrics2).taskAttempt();
    verify(metrics2).taskAttemptFinalSuccess();
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);

    clock.add(1, ChronoUnit.SECONDS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);

    soft.assertThat(taskFuture1.get())
        .asInstanceOf(type(BasicTaskObj.class))
        .isEqualTo(taskFuture2.get())
        .extracting(BasicTaskObj::taskResult)
        .isEqualTo("hello finished");
  }

  @Test
  public void singleServiceSingleConsumerRetryableError() throws Exception {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async = new TestingTasksAsync(clock);

    AtomicReference<CompletableFuture<BasicTaskObj.Builder>> taskCompletionStage =
        new AtomicReference<>(new CompletableFuture<>());

    TaskServiceMetrics metrics = mock(TaskServiceMetrics.class);
    TasksServiceImpl service = new TasksServiceImpl(async, metrics, tasksServiceConfig(1));
    Tasks tasks = service.forPersist(persist);
    BasicTaskRequest taskRequest = basicTaskRequest("hello", taskCompletionStage::get);

    CompletableFuture<BasicTaskObj> taskFuture = tasks.submit(taskRequest).toCompletableFuture();
    verify(metrics).startNewTaskController();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    // 2nd request for the same task (would fail w/ the `null` completion-stage)
    CompletableFuture<BasicTaskObj> taskFuture2 =
        tasks.submit(basicTaskRequest("hello", () -> null)).toCompletableFuture();
    soft.assertThat(taskFuture2).isSameAs(taskFuture);

    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskAttempt();
    verify(metrics).taskCreation();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture).isNotDone();
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.get().completeExceptionally(new RetryableException("retryable"));
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionRetryableError();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    soft.assertThat(taskFuture).isNotDone();
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);

    clock.add(RETRYABLE_ERROR_NOT_BEFORE);

    // need a new CompletableFuture that can be used when the task's execution is re-triggered
    taskCompletionStage.set(new CompletableFuture<>());

    soft.assertThat(taskFuture).isNotDone();
    soft.assertThat(async.doWork()).isEqualTo(1);
    verify(metrics).taskAttempt();
    verify(metrics).taskAttemptErrorRetry();
    verify(metrics).taskRetryStateChangeSucceeded();
    verify(metrics).taskExecution();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async.doWork()).isEqualTo(1);
    verify(metrics).taskUpdateRunningState();
    verify(metrics).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    taskCompletionStage
        .get()
        .complete(
            BasicTaskObj.builder()
                .id(taskRequest.objId())
                .taskParameter(taskRequest.taskParameter())
                .taskResult(taskRequest.taskParameter() + " finished")
                .taskState(TaskState.successState()));
    verify(metrics).taskExecutionFinished();
    verify(metrics).taskExecutionResult();
    verifyNoMoreInteractions(metrics);
    reset(metrics);

    clock.add(1, ChronoUnit.SECONDS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);

    soft.assertThat(taskFuture.get())
        .asInstanceOf(type(BasicTaskObj.class))
        .extracting(BasicTaskObj::taskResult)
        .isEqualTo("hello finished");

    clock.add(1, ChronoUnit.SECONDS);
    soft.assertThat(async.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics);
  }

  @Test
  public void twoServicesDistributedTaskFailure() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async1 = new TestingTasksAsync(clock);
    TestingTasksAsync async2 = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage = new CompletableFuture<>();

    TaskServiceMetrics metrics1 = mock(TaskServiceMetrics.class);
    TaskServiceMetrics metrics2 = mock(TaskServiceMetrics.class);

    TasksServiceImpl service1 = new TasksServiceImpl(async1, metrics1, tasksServiceConfig(1));
    Tasks tasks1 = service1.forPersist(persist);
    TasksServiceImpl service2 = new TasksServiceImpl(async2, metrics2, tasksServiceConfig(2));
    Tasks tasks2 = service2.forPersist(persist);

    BasicTaskRequest taskRequest1 = basicTaskRequest("hello", () -> taskCompletionStage);
    CompletableFuture<BasicTaskObj> taskFuture1 = tasks1.submit(taskRequest1).toCompletableFuture();
    verify(metrics1).startNewTaskController();
    verifyNoMoreInteractions(metrics1);
    reset(metrics1);

    soft.assertThat(async1.doWork()).isEqualTo(1);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture1).isNotDone();
    verify(metrics1).taskAttempt();
    verify(metrics1).taskCreation();
    verify(metrics1).taskExecution();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    BasicTaskRequest taskRequest2 = basicTaskRequest("hello", () -> taskCompletionStage);
    CompletableFuture<BasicTaskObj> taskFuture2 = tasks2.submit(taskRequest2).toCompletableFuture();
    verify(metrics2).startNewTaskController();
    verifyNoMoreInteractions(metrics2);
    reset(metrics2);

    soft.assertThat(async1.doWork()).isEqualTo(1);
    soft.assertThat(async2.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture1).isNotDone();
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskUpdateRunningState();
    verify(metrics1).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics1);
    verify(metrics2).taskAttempt();
    verify(metrics2).taskAttemptRunning();
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async1.doWork()).isEqualTo(1);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture1).isNotDone();
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskUpdateRunningState();
    verify(metrics1).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);

    clock.add(250, ChronoUnit.MILLIS);
    taskCompletionStage.completeExceptionally(new RuntimeException("failed task"));
    soft.assertThat(taskFuture1).isCompletedExceptionally();
    // Clock did not advance to when the state for the task object is refreshed, so the 2nd future
    // is still not done.
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskExecutionFinished();
    verify(metrics1).taskExecutionFailure();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async1.doWork()).isEqualTo(0);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture1).isCompletedExceptionally();
    // Clock did not advance to when the state for the task object is refreshed, so the 2nd future
    // is still not done.
    soft.assertThat(taskFuture2).isNotDone();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);

    clock.add(1000, ChronoUnit.MILLIS);
    soft.assertThat(async1.doWork()).isEqualTo(0);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture1).isCompletedExceptionally();
    // Clock did not advance to when the state for the task object is refreshed, so the 2nd future
    // is still not done.
    soft.assertThat(taskFuture2).isNotDone();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async1.doWork()).isEqualTo(0);
    soft.assertThat(async2.doWork()).isEqualTo(1);
    // Clock advance enough to refresh, so the 2nd future is done now.
    soft.assertThat(taskFuture2).isCompletedExceptionally();
    verifyNoMoreInteractions(metrics1);
    verify(metrics2).taskAttempt();
    verify(metrics2).taskAttemptFinalFailure();
    verifyNoMoreInteractions(metrics2);
    reset(metrics2);

    clock.add(1, ChronoUnit.SECONDS);
    soft.assertThat(async1.doWork()).isEqualTo(0);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);

    soft.assertThatThrownBy(taskFuture2::get)
        .isInstanceOf(ExecutionException.class)
        .cause()
        // This exception is generated via TaskBehavior.stateAsException()
        .isInstanceOf(Exception.class)
        .hasMessage("java.lang.RuntimeException: failed task");
  }

  @Test
  public void twoServicesDistributedLostTask() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("UTC"));
    TestingTasksAsync async1 = new TestingTasksAsync(clock);
    TestingTasksAsync async2 = new TestingTasksAsync(clock);

    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage1 = new CompletableFuture<>();
    CompletableFuture<BasicTaskObj.Builder> taskCompletionStage2 = new CompletableFuture<>();

    TaskServiceMetrics metrics1 = mock(TaskServiceMetrics.class);
    TaskServiceMetrics metrics2 = mock(TaskServiceMetrics.class);

    TasksServiceImpl service1 = new TasksServiceImpl(async1, metrics1, tasksServiceConfig(1));
    Tasks tasks1 = service1.forPersist(persist);
    TasksServiceImpl service2 = new TasksServiceImpl(async2, metrics2, tasksServiceConfig(2));
    Tasks tasks2 = service2.forPersist(persist);

    BasicTaskRequest taskRequest1 = basicTaskRequest("hello", () -> taskCompletionStage1);
    CompletableFuture<BasicTaskObj> taskFuture1 = tasks1.submit(taskRequest1).toCompletableFuture();
    verify(metrics1).startNewTaskController();
    verifyNoMoreInteractions(metrics1);
    reset(metrics1);

    soft.assertThat(async1.doWork()).isEqualTo(1);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture1).isNotDone();
    verify(metrics1).taskAttempt();
    verify(metrics1).taskCreation();
    verify(metrics1).taskExecution();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);

    clock.add(250, ChronoUnit.MILLIS);
    BasicTaskRequest taskRequest2 = basicTaskRequest("hello", () -> taskCompletionStage2);
    CompletableFuture<BasicTaskObj> taskFuture2 = tasks2.submit(taskRequest2).toCompletableFuture();
    verify(metrics2).startNewTaskController();
    verifyNoMoreInteractions(metrics2);
    reset(metrics2);

    soft.assertThat(async1.doWork()).isEqualTo(1);
    soft.assertThat(async2.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture1).isNotDone();
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskUpdateRunningState();
    verify(metrics1).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics1);
    verify(metrics2).taskAttempt();
    verify(metrics2).taskAttemptRunning();
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async1.doWork()).isEqualTo(1);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture1).isNotDone();
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics1).taskUpdateRunningState();
    verify(metrics1).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
    reset(metrics1);

    // Simulate that service #1 died --> do not run any more tasks there

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(taskFuture2).isNotDone();

    clock.add(FRESH_LOST_RETRY_NOT_BEFORE);
    soft.assertThat(async2.doWork()).isEqualTo(1);
    verify(metrics2).taskAttempt();
    verify(metrics2).taskAttemptRunning();
    verify(metrics2).taskLossDetected();
    verify(metrics2).taskLostReassigned();
    verify(metrics2).taskExecution();
    verifyNoMoreInteractions(metrics2);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async2.doWork()).isEqualTo(1);
    soft.assertThat(taskFuture2).isNotDone();
    verify(metrics2).taskUpdateRunningState();
    verify(metrics2).taskRunningStateUpdated();
    verifyNoMoreInteractions(metrics2);
    reset(metrics2);

    taskCompletionStage2.complete(
        BasicTaskObj.builder()
            .id(taskRequest2.objId())
            .taskParameter(taskRequest2.taskParameter())
            .taskResult(taskRequest2.taskParameter() + " finished")
            .taskState(TaskState.successState()));
    verify(metrics2).taskExecutionFinished();
    verify(metrics2).taskExecutionResult();
    verifyNoMoreInteractions(metrics2);
    reset(metrics2);

    clock.add(250, ChronoUnit.MILLIS);
    soft.assertThat(async2.doWork()).isEqualTo(0);
    soft.assertThat(taskFuture2).isDone();
    verifyNoMoreInteractions(metrics1);
    verifyNoMoreInteractions(metrics2);
  }

  static TasksServiceConfig tasksServiceConfig(int inst) {
    return TasksServiceConfig.tasksServiceConfig(
        "instance#" + inst, DEFAULT_RACE_WAIT_MILLIS_MIN, DEFAULT_RACE_WAIT_MILLIS_MAX);
  }
}
