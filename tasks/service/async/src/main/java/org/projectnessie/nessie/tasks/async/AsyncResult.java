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
package org.projectnessie.nessie.tasks.async;

import static java.util.Optional.empty;

import java.time.Instant;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface AsyncResult<V> {

  @Value.Parameter(order = 1)
  Optional<V> result();

  @Value.Parameter(order = 2)
  Optional<Throwable> failure();

  @Value.Parameter(order = 3)
  Optional<Instant> retryEarliest();

  static <V> AsyncResult<V> completed(V value) {
    return ImmutableAsyncResult.of(Optional.of(value), empty(), empty());
  }

  static <V> AsyncResult<V> runningDelay(Instant retryEarliest) {
    return ImmutableAsyncResult.of(empty(), empty(), Optional.of(retryEarliest));
  }

  static <V> AsyncResult<V> permanentFailure(Throwable throwable) {
    return ImmutableAsyncResult.of(empty(), Optional.of(throwable), empty());
  }

  static <V> AsyncResult<V> retryableError(Instant retryEarliest) {
    return ImmutableAsyncResult.of(empty(), empty(), Optional.of(retryEarliest));
  }
}
