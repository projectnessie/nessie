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
package org.projectnessie.nessie.tasks.service;

import static com.google.common.base.Preconditions.checkState;

import org.immutables.value.Value;

@Value.Immutable
public interface TasksServiceConfig {

  long DEFAULT_RACE_WAIT_MILLIS_MIN = 50L;
  long DEFAULT_RACE_WAIT_MILLIS_MAX = 200L;

  @Value.Parameter(order = 1)
  String name();

  @Value.Parameter(order = 2)
  @Value.Default
  default long raceWaitMillisMin() {
    return DEFAULT_RACE_WAIT_MILLIS_MIN;
  }

  @Value.Parameter(order = 3)
  @Value.Default
  default long raceWaitMillisMax() {
    return DEFAULT_RACE_WAIT_MILLIS_MAX;
  }

  static TasksServiceConfig tasksServiceConfig(
      String name, long raceWaitMillisMin, long raceWaitMillisMax) {
    return ImmutableTasksServiceConfig.of(name, raceWaitMillisMin, raceWaitMillisMax);
  }

  @Value.Check
  default void check() {
    checkState(raceWaitMillisMin() < raceWaitMillisMax());
    checkState(raceWaitMillisMin() > 0L);
  }
}
