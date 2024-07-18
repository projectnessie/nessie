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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;

@ConfigMapping(prefix = "nessie.catalog.service")
public interface CatalogServiceConfig {
  /** Advanced property, defines the maximum number of concurrent imports from object stores. */
  @WithName("imports.max-concurrent")
  @WithDefault("32")
  int maxConcurrentImports();

  /** Advanced property, defines the maximum number of threads for async tasks like imports. */
  @WithName("tasks.threads.max")
  @WithDefault("-1")
  int tasksMaxThreads();

  /** Advanced thread pool setting for async tasks like imports. */
  @WithName("tasks.threads.keep-alive")
  @WithDefault("PT2S")
  Duration tasksThreadsKeepAlive();

  /** Advanced thread pool setting for async tasks like imports. */
  @WithName("tasks.minimum-delay")
  @WithDefault("PT0.001S")
  Duration tasksMinimumDelay();

  /** Advanced thread pool setting for async tasks like imports. */
  @WithName("race.wait.min")
  @WithDefault("PT0.005S")
  Duration raceWaitMin();

  /** Advanced thread pool setting for async tasks like imports. */
  @WithName("race.wait.max")
  @WithDefault("PT0.250S")
  Duration raceWaitMax();
}
