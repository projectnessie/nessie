/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.tools.daemon;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.quarkus.runtime.ApplicationLifecycleManager;
import io.quarkus.runtime.Quarkus;

public class ServerHolder implements AutoCloseable {

  private final ExecutorService executor;
  private final ClassLoader classLoader;
  private Future<?> job;
  private Exception throwable = null;

  public ServerHolder(ClassLoader classLoader) throws ClassNotFoundException {
    this.classLoader = classLoader;
    executor = Executors.newSingleThreadExecutor();
  }

  /**
   * excecutes Quarkus.run in its own thread with its own classpath.
   *
   * <p>
   *   We save both the resulting future and any thrown exceptions for reporting and shutdown.
   * </p>
   */
  public void start() {
    job = executor.submit(() -> {
      try {
        Thread.currentThread().setContextClassLoader(classLoader);
        ApplicationLifecycleManager.setDefaultExitCodeHandler(i -> {
        });
        Quarkus.run();
      } catch (Exception t) {
        throwable = t;
        throw t;
      }
    });
  }

  public boolean isRunning() {
    return !job.isDone() && !job.isCancelled();
  }

  public Exception error() {
    return throwable;
  }

  @Override
  public void close() throws Exception {
    Quarkus.blockingExit();
    job.cancel(true);
    executor.shutdownNow();
  }
}
