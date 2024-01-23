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
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedStage;
import static java.util.concurrent.CompletableFuture.failedStage;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.immutables.value.Value;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskObj;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.nessie.tasks.api.TaskStatus;
import org.projectnessie.nessie.tasks.api.Tasks;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.nessie.tasks.async.TasksAsync;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TasksServiceImpl implements TasksService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TasksServiceImpl.class);

  private final String name;
  private final TasksAsync async;
  private final TaskServiceMetrics metrics;

  private final long raceWaitMillisMin;
  private final long raceWaitMillisMax;

  private final ConcurrentMap<TaskKey, CompletionStage<TaskObj>> currentTasks =
      new ConcurrentHashMap<>();

  private volatile boolean shutdown;

  public TasksServiceImpl() {
    this(null, null, null);
  }

  @Inject
  public TasksServiceImpl(
      @TasksServiceExecutor TasksAsync async,
      TaskServiceMetrics metrics,
      TasksServiceConfig config) {
    this.async = async;
    this.metrics = metrics;
    this.name = config.name();
    this.raceWaitMillisMin = config.raceWaitMillisMin();
    this.raceWaitMillisMax = config.raceWaitMillisMax();
  }

  @Override
  public CompletionStage<Void> shutdown() {
    shutdown = true;
    return CompletableFuture.allOf(
            currentTasks.values().stream()
                .map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new))
        .thenApply(x -> null);
  }

  @Override
  public Tasks forPersist(Persist persist) {
    return new TasksImpl(persist);
  }

  <T extends TaskObj, B extends TaskObj.Builder> CompletionStage<T> submit(
      Persist persist, TaskRequest<T, B> taskRequest) {
    ObjId objId = taskRequest.objId();

    // Try to get the object and immediately return if it has a final state. We expect to hit final
    // states way more often, so preventing the concurrent-hash-map interactions and especially the
    // asynchronous task handling improves the implementation.
    // TODO using `fetchObj()` would be wrong here, because it is *synchronous* and can block.
    //  Options:
    //  a) remove this "optimization"
    //  b) add a `getObjIfCached(ObjId)` --> chosen for now
    //  c) add a `fetchObjAsync()`, but adding async variants to all database implementations will
    //     be tricky
    Obj obj = persist.getImmediate(taskRequest.objId());
    if (obj != null) {
      T taskObj = castObj(taskRequest, obj);
      TaskStatus status = taskObj.taskState().status();
      switch (status) {
        case FAILURE:
          metrics.taskHasFinalFailure();
          return failedStage(taskRequest.behavior().stateAsException(taskObj));
        case SUCCESS:
          metrics.taskHasFinalSuccess();
          return completedStage(taskObj);
        default:
          // task object exists but has a non-final state, handle it asynchronously
          checkState(!status.isFinal(), "Expect non-final task status");
          break;
      }
    }

    // Ensure that only one "base" completable future exists for each obj-id.
    TaskKey taskKey = TaskKey.taskKey(persist.config().repositoryId(), objId);

    // The shutdown-check can be racy, if shutdown() is called after the `if` but before the access
    // to `currentTasks`. But since `shutdown()` is usually only relevant for tests, that trade-off
    // is acceptable.
    if (shutdown) {
      return CompletableFuture.failedStage(
          new IllegalStateException("Tasks service already shutdown"));
    }

    @SuppressWarnings("unchecked")
    CompletionStage<T> r =
        (CompletionStage<T>)
            currentTasks.computeIfAbsent(
                taskKey,
                id -> {
                  metrics.startNewTaskController();
                  ExecParams execParams = new ExecParams(persist, taskRequest);
                  LOGGER.trace("{}: Starting new local task controller for {}", name, execParams);
                  async.call(() -> tryLocal(execParams));
                  return execParams.resultFuture;
                });
    return r;
  }

  private void finalResult(ExecParams params, TaskObj result) {
    try {
      params.resultFuture.complete(result);
    } finally {
      removeFromCurrentTasks(params);
    }
  }

  private void finalFailure(ExecParams params, Throwable t) {
    try {
      params.resultFuture.completeExceptionally(t);
    } finally {
      removeFromCurrentTasks(params);
    }
  }

  private void removeFromCurrentTasks(ExecParams params) {
    TaskKey taskKey = TaskKey.taskKey(params.persist.config().repositoryId(), params.objId());
    currentTasks.remove(taskKey);
  }

  private void tryLocal(ExecParams params) {
    // Called from a thread pool, need to lock.
    params.lock.lock();
    try {
      metrics.taskAttempt();
      LOGGER.trace("{}: Task evaluation attempt for {}", name, params);

      TaskObj obj = castObj(params.taskRequest, params.persist.fetchObj(params.objId()));
      // keep in mind: `obj` might be a locally cached instance that is not in sync w/ the
      // database!

      TaskState state = obj.taskState();
      LOGGER.trace("{}: Evaluating task for {} with state {}", name, params, state);

      switch (state.status()) {
        case SUCCESS:
          metrics.taskAttemptFinalSuccess();
          finalResult(params, obj);
          break;
        case FAILURE:
          metrics.taskAttemptFinalFailure();
          finalFailure(params, params.taskRequest.behavior().stateAsException(obj));
          break;
        case RUNNING:
          metrics.taskAttemptRunning();
          checkRunningTask(params, state, obj);
          break;
        case ERROR_RETRY:
          metrics.taskAttemptErrorRetry();
          maybeAttemptErrorRetry(params, state, obj);
          break;
        default:
          throw new IllegalStateException("Unknown task status " + state.status());
      }

    } catch (ObjNotFoundException e) {
      LOGGER.trace("{}: Task for {} does not yet exist, creating", name, params);

      try {
        metrics.taskCreation();
        TaskBehavior<TaskObj, TaskObj.Builder> behavior = params.taskRequest.behavior();
        TaskObj obj =
            withNewVersionToken(
                params
                    .taskRequest
                    .applyRequestToObjBuilder(behavior.newObjBuilder())
                    .id(params.taskRequest.objId())
                    .type(params.taskRequest.objType())
                    .taskState(behavior.runningTaskState(async.clock(), null)));

        if (params.persist.storeObj(obj)) {
          LOGGER.trace("{}: Task creation for {} succeeded", name, params);
          issueLocalTaskExecution(params, obj);
        } else {
          LOGGER.trace("{}: Task creation for {} failed, retrying", name, params);

          // Another process stored the task-obj for the task-request, reschedule but do not loop to
          // be "nice" and give other requests the ability to run.
          metrics.taskCreationRace();
          reattemptAfterRace(params);
        }
      } catch (Throwable t) {
        // Unhandled failure
        LOGGER.error(
            "{}: Unhandled state while storing initial task execution state for {}",
            name,
            params,
            t);
        metrics.taskCreationUnhandled();
        finalFailure(params, t);
      }
    } catch (Throwable t) {
      // Unhandled failure
      LOGGER.error("{}: Unhandled state during local task attempt for {}", name, params, t);
      metrics.taskAttemptUnhandled();
      finalFailure(params, t);
    } finally {
      params.lock.unlock();
    }
  }

  // Called while ExecParams is locked from tryLocal()
  private void checkRunningTask(ExecParams params, TaskState state, TaskObj obj)
      throws ObjTooLargeException {
    Instant now = async.clock().instant();
    if (now.compareTo(requireNonNull(state.lostNotBefore())) >= 0) {
      metrics.taskLossDetected();
      LOGGER.warn("{}: Detected lost task for {}", name, params);
      TaskBehavior<TaskObj, TaskObj.Builder> behavior = params.taskRequest.behavior();
      TaskObj retryState =
          withNewVersionToken(
              behavior
                  .newObjBuilder()
                  .from(obj)
                  .taskState(behavior.runningTaskState(async.clock(), obj)));

      if (params.persist.updateConditional(obj, retryState)) {
        metrics.taskLostReassigned();
        issueLocalTaskExecution(params, retryState);
      } else {
        metrics.taskLostReassignRace();
        reattemptAfterRace(params);
      }
    } else {
      async.schedule(() -> tryLocal(params), state.retryNotBefore());
    }
  }

  // Called while ExecParams is locked from tryLocal()
  private void maybeAttemptErrorRetry(ExecParams params, TaskState state, TaskObj obj)
      throws ObjTooLargeException {
    Instant now = async.clock().instant();
    if (now.compareTo(requireNonNull(state.retryNotBefore())) >= 0) {
      TaskBehavior<TaskObj, TaskObj.Builder> behavior = params.taskRequest.behavior();
      TaskObj retryState =
          withNewVersionToken(
              behavior
                  .newObjBuilder()
                  .from(obj)
                  .taskState(behavior.runningTaskState(async.clock(), obj)));

      if (params.persist.updateConditional(obj, retryState)) {
        metrics.taskRetryStateChangeSucceeded();
        issueLocalTaskExecution(params, retryState);
      } else {
        metrics.taskRetryStateChangeRace();
        reattemptAfterRace(params);
      }
    } else {
      async.schedule(() -> tryLocal(params), state.retryNotBefore());
    }
  }

  private void reattemptAfterRace(ExecParams params) {
    long raceWaitMillis =
        ThreadLocalRandom.current().nextLong(raceWaitMillisMin, raceWaitMillisMax);
    async.schedule(
        () -> tryLocal(params), async.clock().instant().plus(raceWaitMillis, ChronoUnit.MILLIS));
  }

  // Called while ExecParams is locked from tryLocal()
  private void issueLocalTaskExecution(ExecParams params, TaskObj obj) {
    LOGGER.debug("{}: Starting local task execution for {}", name, params);
    metrics.taskExecution();

    params.runningObj = obj;
    scheduleTaskRunningUpdate(params, obj);

    params
        .taskRequest
        .submitExecution()
        .whenComplete(
            (resultBuilder, failure) -> localTaskFinished(params, resultBuilder, failure));
  }

  private void localTaskFinished(
      ExecParams params, TaskObj.Builder resultBuilder, Throwable failure) {
    // Called from a thread pool, need to lock.
    params.lock.lock();
    try {
      TaskObj expected = params.runningObj;

      params.cancelRunningStateUpdate();

      metrics.taskExecutionFinished();

      if (expected == null) {
        unexpectedNullExpectedState(params, resultBuilder, failure);
        return;
      }

      if (resultBuilder != null) {
        TaskObj r = withNewVersionToken(resultBuilder);

        LOGGER.trace("{}, Task execution for {} succeeded, updating database", name, params);

        // Task execution succeeded with a final result
        if (params.persist.updateConditional(expected, r)) {
          metrics.taskExecutionResult();
          // Database updated with final result
          LOGGER.debug(
              "{}: Task execution success result for {} updated in database, returning final result",
              name,
              params);
          finalResult(params, r);
        } else {
          metrics.taskExecutionResultRace();
          // Another process updated the database state in the meantime.
          String msg =
              format(
                  "Failed to update successful task execution result for %s in database (race condition), exposing as a failure",
                  params);
          LOGGER.warn("{}: {}", name, msg);
          finalFailure(params, new ConcurrentModificationException(msg, failure));
        }
      } else if (failure == null) {
        failure =
            new NullPointerException("Local task execution return a null object, which is illegal");
      }
      if (failure != null) {
        LOGGER.trace("{}: Task execution for {} failed, updating database", name, params);

        TaskBehavior<TaskObj, TaskObj.Builder> behavior = params.taskRequest.behavior();
        TaskState newState = behavior.asErrorTaskState(async.clock(), expected, failure);
        checkState(newState.status().isError());
        TaskObj updatedObj =
            withNewVersionToken(behavior.newObjBuilder().from(expected).taskState(newState));
        if (params.persist.updateConditional(expected, updatedObj)) {
          // Database updated with final result
          if (newState.status().isRetryable()) {
            metrics.taskExecutionRetryableError();
            LOGGER.debug(
                "{}: Task execution raised retryable error for {} updated in database, retrying",
                name,
                params);
            reattemptAfterRetryableError(params, newState.retryNotBefore());
          } else {
            metrics.taskExecutionFailure();
            LOGGER.debug(
                "{}: Task execution ended in final failure for {} updated in database, returning final result",
                name,
                params);
            finalFailure(params, failure);
          }
        } else {
          metrics.taskExecutionFailureRace();
          String msg =
              format(
                  "Failed to update failure task execution result for %s in database (race condition)",
                  params);
          LOGGER.warn("{}: {}", name, msg);
          finalFailure(params, new ConcurrentModificationException(msg, failure));
        }
      }

    } catch (Throwable t2) {
      // Unhandled failure
      LOGGER.error(
          "{}: Unhandled state while evaluating task execution result for {}", name, params, t2);
      metrics.taskExecutionUnhandled();
      finalFailure(params, t2);
    } finally {
      params.lock.unlock();
    }
  }

  private void unexpectedNullExpectedState(
      ExecParams params, TaskObj.Builder resultBuilder, Throwable failure) {
    // Oops ... no clue how that might have happened, but handle it just in case.
    String res;
    if (failure != null) {
      res = "exceptionally";
    } else if (resultBuilder != null) {
      res = "successfully";
    } else {
      res = "with an illegal null result";
    }
    String msg =
        format(
            "Task execution for %s finished %s, but the expected task obj state is null. Cannot persist the task execution result.",
            params, res);
    LOGGER.error("{}, {}", name, msg);
    Exception ex = new IllegalStateException(msg);
    if (failure != null) {
      ex.addSuppressed(failure);
    }
    finalFailure(params, ex);
  }

  private void scheduleTaskRunningUpdate(ExecParams params, TaskObj current) {
    // Called while holding the ExecParams.lock
    Instant scheduleNotBefore =
        params.taskRequest.behavior().performRunningStateUpdateAt(async.clock(), current);
    params.runningUpdateScheduled =
        async.schedule(() -> updateRunningState(params), scheduleNotBefore);
  }

  private void updateRunningState(ExecParams params) {
    // Called from a thread pool, need to lock.
    params.lock.lock();
    try {

      TaskObj current = params.runningObj;
      if (current == null) {
        // Local task execution finished, do nothing.
        LOGGER.trace(
            "{}: Local task execution has finished, no need to update running state for {}",
            name,
            params);
        return;
      }

      metrics.taskUpdateRunningState();
      TaskState state = current.taskState();
      if (state.status() == TaskStatus.RUNNING) {
        TaskBehavior<TaskObj, TaskObj.Builder> behavior = params.taskRequest.behavior();
        TaskObj updated =
            withNewVersionToken(
                behavior
                    .newObjBuilder()
                    .from(current)
                    .taskState(behavior.runningTaskState(async.clock(), null)));
        if (updated.taskState().status() != TaskStatus.RUNNING) {
          throw new IllegalStateException(
              format(
                  "TaskBehavior.runningTaskState() implementation %s returned illegal status %s, must return RUNNING",
                  behavior.getClass().getName(), updated.taskState().status()));
        }

        try {
          if (params.persist.updateConditional(current, updated)) {
            params.runningObj = updated;
            metrics.taskRunningStateUpdated();
            // Current state successfully updated in database, reschedule running task update
            LOGGER.trace(
                "{}: Successfully updated state for locally running task for {}", name, params);
            scheduleTaskRunningUpdate(params, updated);
          } else {
            metrics.taskRunningStateUpdateRace();
            // Ran into a (remote) race, retry running-update
            LOGGER.warn(
                "{}: Race on database update while updating running state for {}. The result of the local task "
                    + "execution might be lost. When the local task execution finishes, it may also run into an "
                    + "update-race, indicating that the task-result is lost.",
                name,
                params);
            return; // don't re-schedule, there's no chance that another update will succeed.
          }
        } catch (Throwable t) {
          LOGGER.error("{}: Unexpected exception updating task state for {}", name, params, t);
          // re-schedule ... and pray
          scheduleTaskRunningUpdate(params, current);
        }
      } else {
        metrics.taskRunningStateUpdateNoLongerRunning();
        LOGGER.trace(
            "{}: Task for {} no longer running, skipping further local running state updates",
            name,
            params);
      }
    } finally {
      params.lock.unlock();
    }
  }

  private void reattemptAfterRetryableError(ExecParams params, Instant retryNotBefore) {
    async.schedule(() -> tryLocal(params), retryNotBefore);
  }

  private static final class ExecParams {
    final Persist persist;
    final CompletableFuture<TaskObj> resultFuture;
    final TaskRequest<TaskObj, TaskObj.Builder> taskRequest;

    final Lock lock = new ReentrantLock();

    TaskObj runningObj;
    CompletionStage<Void> runningUpdateScheduled;

    @SuppressWarnings("unchecked")
    ExecParams(Persist persist, TaskRequest<?, ?> taskRequest) {
      this.persist = persist;
      this.resultFuture = new CompletableFuture<>();
      this.taskRequest = (TaskRequest<TaskObj, TaskObj.Builder>) taskRequest;
    }

    ObjId objId() {
      return taskRequest.objId();
    }

    void cancelRunningStateUpdate() {
      // Cancel scheduled running-state update
      CompletionStage<Void> handle = runningUpdateScheduled;
      if (handle != null) {
        runningObj = null;
        runningUpdateScheduled = null;
        // Don't interrupt, not all implementations support that (Vert.X won't, see
        // https://github.com/eclipse-vertx/vert.x/issues/3334)
        handle.toCompletableFuture().cancel(false);
      }
    }

    @Override
    public String toString() {
      return taskRequest.objType().name() + ':' + taskRequest.objId();
    }
  }

  private TaskObj withNewVersionToken(TaskObj.Builder builder) {
    return builder.versionToken(ObjId.randomObjId().toString()).build();
  }

  private static <T extends TaskObj, B extends TaskObj.Builder> T castObj(
      TaskRequest<T, B> taskRequest, Obj obj) {
    Class<? extends Obj> clazz = taskRequest.behavior().objType().targetClass();
    try {
      @SuppressWarnings("unchecked")
      T taskObj = (T) clazz.cast(obj);
      return taskObj;
    } catch (ClassCastException e) {
      throw new ClassCastException(
          "Failed to cast obj of type "
              + obj.type().name()
              + " to the task request's expected type "
              + clazz.getName());
    }
  }

  final class TasksImpl implements Tasks {
    final Persist persist;

    public TasksImpl(Persist persist) {
      this.persist = persist;
    }

    @Override
    public <T extends TaskObj, B extends TaskObj.Builder> CompletionStage<T> submit(
        TaskRequest<T, B> taskRequest) {
      return TasksServiceImpl.this.submit(persist, taskRequest);
    }
  }

  @Value.Immutable
  interface TaskKey {
    @Value.Parameter(order = 1)
    String repositoryId();

    @Value.Parameter(order = 2)
    ObjId objId();

    static TaskKey taskKey(String repositoryId, ObjId objId) {
      return ImmutableTaskKey.of(repositoryId, objId);
    }
  }
}
