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
package org.projectnessie.tools.admin.cli;

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.event.Observes;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownHelper.class);

  public void onShutdown(@Observes ShutdownEvent event) {
    try {
      Class<?> clazz = Class.forName("io.opentelemetry.api.GlobalOpenTelemetry");
      Method resetForTest = clazz.getDeclaredMethod("resetForTest");
      resetForTest.invoke(null);
    } catch (ReflectiveOperationException e) {
      LOGGER.error("Failed to reset GlobalOpenTelemetry for test", e);
    }
  }
}
