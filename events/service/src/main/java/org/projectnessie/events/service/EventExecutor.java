/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.events.service;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * An executor for delivering events to blocking subscribers, and for scheduling retries after
 * failed delivery attempts.
 */
public interface EventExecutor extends AutoCloseable {

  /**
   * Submits the delivery of an event to a blocking subscriber.
   *
   * @implSpec The delivery should be submitted to a thread pool that is optimized for blocking
   *     operations. If the calling thread is allowed to block, then the delivery action is allowed
   *     be executed synchronously on the calling thread.
   * @param blockingDelivery the (potentially blocking) delivery action to execute.
   * @return a future that completes when the delivery has been executed.
   */
  CompletionStage<Void> submitBlocking(Runnable blockingDelivery);

  /**
   * Schedules a retry to be executed after a delay. This method should be used to schedule retries
   * only.
   *
   * @implSpec The retry can be scheduled on any thread, including an event loop thread that must
   *     not block; if the delivery is blocking, the retry would in turn call {@link
   *     #submitBlocking(Runnable)} to perform the actual delivery.
   * @param retry the retry to execute.
   * @param delay the delay after which the retry should be executed.
   * @return a future that completes when the retry has been executed.
   */
  CompletionStage<Void> scheduleRetry(Runnable retry, Duration delay);
}
