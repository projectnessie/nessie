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
package org.projectnessie.quarkus.providers;

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.event.Observes;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ScheduledExecutorService;

/** Helper class to stop some "static final" thread references, leading to class-loader OOMs. */
@SuppressWarnings("unused")
public class ShutdownHandlers {

  public void onShutdown(@Observes ShutdownEvent event) {
    mongoDefaultBufferPollPruner();
  }

  /**
   * Shutdown the "static final" {@link ScheduledExecutorService} referencing the periodic call to
   * {@code com.mongodb.internal.connection.PowerOfTwoBufferPool#prune()}.
   */
  private void mongoDefaultBufferPollPruner() {
    try {
      Class<?> c = Class.forName("com.mongodb.internal.connection.PowerOfTwoBufferPool");
      Field fieldDefault = c.getDeclaredField("DEFAULT");
      Object defaultBufferPool = fieldDefault.get(null);
      Method disablePruning = c.getDeclaredMethod("disablePruning");
      disablePruning.setAccessible(true);
      disablePruning.invoke(defaultBufferPool);
    } catch (ReflectiveOperationException ignore) {
      //
    }
  }
}
