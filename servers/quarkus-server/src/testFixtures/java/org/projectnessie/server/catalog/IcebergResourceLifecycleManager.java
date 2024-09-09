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
package org.projectnessie.server.catalog;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attempt to clean up per-test resources registered against JVM global instances, threads and
 * related objects that are effectively held forever and cause OOMs.
 */
public class IcebergResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergResourceLifecycleManager.class);

  @Override
  public Map<String, String> start() {
    return Map.of();
  }

  @Override
  public void stop() {
    ThreadPools.getDeleteWorkerPool().shutdown();
    ThreadPools.getWorkerPool().shutdown();

    // Stop Hadoop's stats-cleaner thread, would never stop.
    try {
      Field statsDataCleanerThreadField =
          FileSystem.Statistics.class.getDeclaredField("STATS_DATA_CLEANER");
      statsDataCleanerThreadField.setAccessible(true);
      Thread statsDataCleanerThread = (Thread) statsDataCleanerThreadField.get(null);
      statsDataCleanerThread.interrupt();
    } catch (Exception e) {
      LOGGER.error(
          "Failed to interrupt Thread org.apache.hadoop.fs.FileSystem.Statistics.STATS_DATA_CLEANER",
          e);
    }

    // Stop other thread pools, registered for shutdown via Guava's
    // MoreExecutors.Application.addDelayedShutdownHook.
    // This also stops Iceberg's thread pools, those would be cleaned up at JVM shutdown,
    // see org.apache.iceberg.util.ThreadPools.
    try {
      Class<?> c = Class.forName("java.lang.ApplicationShutdownHooks");
      Field hooksField = c.getDeclaredField("hooks");
      hooksField.setAccessible(true);
      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<Object, Object> hooks = (Map) hooksField.get(null);
      List<Thread> guavaDelayedShutdownHooks =
          hooks.keySet().stream()
              .map(Thread.class::cast)
              .filter(t -> t.getName().startsWith("DelayedShutdownHook-for-"))
              .toList();
      for (Thread hook : guavaDelayedShutdownHooks) {
        try {
          hook.run();
        } catch (Exception e) {
          LOGGER.error("Failed to run delayed shutdown hook " + hook.getName(), e);
        } finally {
          Runtime.getRuntime().removeShutdownHook(hook);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to interrupt cleanup application shutdown hooks", e);
    }
  }
}
