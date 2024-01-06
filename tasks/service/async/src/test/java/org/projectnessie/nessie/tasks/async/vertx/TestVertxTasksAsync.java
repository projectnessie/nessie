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
package org.projectnessie.nessie.tasks.async.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.time.Clock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.nessie.tasks.async.BaseTasksAsync;
import org.projectnessie.nessie.tasks.async.TasksAsync;

public class TestVertxTasksAsync extends BaseTasksAsync {
  private static Vertx vertx;

  @BeforeAll
  public static void setup() {
    vertx =
        Vertx.builder()
            .with(new VertxOptions().setWorkerPoolSize(3).setEventLoopPoolSize(2))
            .build();
  }

  @AfterAll
  public static void stop() {
    vertx.close().result();
  }

  @Override
  protected TasksAsync tasksAsync() {
    return new VertxTasksAsync(vertx, Clock.systemUTC(), 1L);
  }
}
