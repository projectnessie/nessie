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
package org.projectnessie.nessie.tasks.api;

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import org.immutables.value.Value;
import org.projectnessie.nessie.tasks.api.JacksonSerializers.InstantAsLongDeserializer;
import org.projectnessie.nessie.tasks.api.JacksonSerializers.InstantAsLongSerializer;

/** Task state as a value object. */
@Value.Immutable
@JsonSerialize(as = ImmutableTaskState.class)
@JsonDeserialize(as = ImmutableTaskState.class)
public interface TaskState {
  /** The current task status. */
  @Value.Parameter(order = 1)
  TaskStatus status();

  /**
   * Represents the earliest timestamp when a retryable error can be retried or the value/state of a
   * running task can be refreshed. Only valid for {@link TaskStatus#RUNNING RUNNING} and {@link
   * TaskStatus#ERROR_RETRY ERROR_RETRY}.
   */
  @Value.Parameter(order = 2)
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonDeserialize(using = InstantAsLongDeserializer.class)
  @JsonSerialize(using = InstantAsLongSerializer.class)
  Instant retryNotBefore();

  /**
   * Represents the earliest timestamp when a task service can assume that a {@link
   * TaskStatus#RUNNING RUNNING} task is lost and should be re-started. Only valid for {@link
   * TaskStatus#RUNNING RUNNING}.
   */
  @Value.Parameter(order = 3)
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonDeserialize(using = InstantAsLongDeserializer.class)
  @JsonSerialize(using = InstantAsLongSerializer.class)
  Instant lostNotBefore();

  /**
   * Represents an error message. Only valid for {@link TaskStatus#FAILURE FAILURE} and {@link
   * TaskStatus#ERROR_RETRY ERROR_RETRY}.
   */
  @Value.Parameter(order = 4)
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String message();

  /**
   * An error code associated with this task state. Error codes are machine-readable representations
   * of failures and are meant to be interpreted by the {@link TaskBehavior} implementation that
   * manages the related task.
   *
   * <p>On the other hand, the value of {@link #message()} is meant to be interpreted by humans.
   *
   * <p>Storing error codes in {@link TaskState} is intended to allow reporting task errors /
   * exceptions to the caller the same way whether the task fails for the first time in the server
   * that executed it, or it is reloaded from storage in another server.
   *
   * @see TaskBehavior#stateAsException(TaskObj)
   */
  @Value.Parameter(order = 5)
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String errorCode();

  TaskState SUCCESS = ImmutableTaskState.of(TaskStatus.SUCCESS, null, null, null, null);

  static TaskState successState() {
    return SUCCESS;
  }

  static TaskState runningState(@Nonnull Instant retryNotBefore, @Nonnull Instant lostNotBefore) {
    return ImmutableTaskState.of(TaskStatus.RUNNING, retryNotBefore, lostNotBefore, null, null);
  }

  static TaskState retryableErrorState(
      @Nonnull Instant retryNotBefore, @Nonnull String message, @Nonnull String errorCode) {
    return ImmutableTaskState.of(TaskStatus.ERROR_RETRY, retryNotBefore, null, message, errorCode);
  }

  static TaskState failureState(@Nonnull String message, String errorCode) {
    return ImmutableTaskState.of(TaskStatus.FAILURE, null, null, message, errorCode);
  }

  static TaskState taskState(
      TaskStatus taskStatus,
      Instant retryNotBefore,
      Instant lostNotBefore,
      String message,
      String errorCode) {
    return ImmutableTaskState.of(taskStatus, retryNotBefore, lostNotBefore, message, errorCode);
  }

  @Value.Check
  default void check() {
    switch (status()) {
      case SUCCESS:
        checkState(retryNotBefore() == null, "retryNotBefore must be null for SUCCESS");
        checkState(lostNotBefore() == null, "retryNotBefore must be null for SUCCESS");
        break;
      case FAILURE:
        checkState(retryNotBefore() == null, "retryNotBefore must be null for FAILURE");
        checkState(lostNotBefore() == null, "lostNotBefore must be null for FAILURE");
        checkState(message() != null, "message must not be null for FAILURE");
        break;
      case RUNNING:
        checkState(retryNotBefore() != null, "retryNotBefore must not be null for RUNNING");
        checkState(lostNotBefore() != null, "lostNotBefore must not be null for RUNNING");
        break;
      case ERROR_RETRY:
        checkState(retryNotBefore() != null, "retryNotBefore must not be null for ERROR_RETRY");
        checkState(lostNotBefore() == null, "lostNotBefore must be null for ERROR_RETRY");
        checkState(message() != null, "message must not be null for ERROR_RETRY");
        break;
      default:
        throw new IllegalStateException("Unknown task status " + status());
    }
  }
}
