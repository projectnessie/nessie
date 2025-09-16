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

import com.github.benmanes.caffeine.cache.Cache;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clean up per-test resources registered against JVM global instances, threads and other statically
 * held objects that are effectively held forever. Some of these can eventually cause OOMs
 * especially in Quarkus unit tests.
 */
public abstract class IcebergResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergResourceLifecycleManager.class);

  @Override
  public Map<String, String> start() {
    return Map.of();
  }

  /**
   * Calls {@link #cleanupHadoopFileSystemStatisticsCleanerThread()}, {@link
   * #stopIcebergWorkerPools()}, {@link #cleanupS3V4RestSignerClient()} and {@link
   * #cleanupShutdownHooks()}.
   */
  public static class ForUnitTests extends IcebergResourceLifecycleManager {
    @Override
    public void stop() {
      // Handling S3V4RestSignerClient in Quarkus unit tests is probably not necessary, as the class
      // loaders for each unit are isolated.
      // But it does not harm, so better to be on the "safe side".
      cleanupS3V4RestSignerClient();
      stopIcebergWorkerPools();
      cleanupHadoopFileSystemStatisticsCleanerThread();
      cleanupShutdownHooks();
    }
  }

  /**
   * Only clean up the {@link S3V4RestSignerClient} static fields which keep auth information, which
   * breaks tests running with different auth setups. This is caused by <a
   * href="https://github.com/apache/iceberg/pull/13215">PR 13215</a> since Iceberg 1.10. This
   * implementation does not stop any thread pool or anything else that's handled for {@link
   * ForUnitTests unit tests}.
   */
  public static class ForIntegrationTests extends IcebergResourceLifecycleManager {
    @Override
    public void stop() {
      cleanupS3V4RestSignerClient();
    }
  }

  /** Stops the delete and "common" worker pools in {@link ThreadPools}. */
  public static void stopIcebergWorkerPools() {
    ThreadPools.getDeleteWorkerPool().shutdown();
    ThreadPools.getWorkerPool().shutdown();
    LOGGER.info("Stopped Iceberg's delete + worker thread pools");
  }

  /** Stop Hadoop's stats-cleaner, it would never stop on its own. */
  public static void cleanupHadoopFileSystemStatisticsCleanerThread() {
    try {
      Field statsDataCleanerThreadField =
          FileSystem.Statistics.class.getDeclaredField("STATS_DATA_CLEANER");
      statsDataCleanerThreadField.setAccessible(true);
      Thread statsDataCleanerThread = (Thread) statsDataCleanerThreadField.get(null);
      statsDataCleanerThread.interrupt();
      LOGGER.info("Stopped Hadoop's stats-cleaner thread");
    } catch (Exception e) {
      LOGGER.error(
          "Failed to interrupt Thread org.apache.hadoop.fs.FileSystem.Statistics.STATS_DATA_CLEANER",
          e);
    }
  }

  /**
   * Stop other thread pools, registered for shutdown via Guava's {@link
   * com.google.common.util.concurrent.MoreExecutors#addDelayedShutdownHook}. This also stops
   * Iceberg's thread pools, those would be cleaned up at JVM shutdown, see {@link
   * org.apache.iceberg.util.ThreadPools}.
   */
  @SuppressWarnings("CallToThreadRun")
  public static void cleanupShutdownHooks() {
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
          LOGGER.error("Failed to run delayed shutdown hook {}", hook.getName(), e);
        } finally {
          Runtime.getRuntime().removeShutdownHook(hook);
        }
      }
      LOGGER.info("Interrupt cleanup application shutdown hooks");
    } catch (Exception e) {
      LOGGER.error("Failed to interrupt cleanup application shutdown hooks", e);
    }
  }

  public static void cleanupS3V4RestSignerClient() {
    try {
      Class<S3V4RestSignerClient> c = S3V4RestSignerClient.class;

      Field f = c.getDeclaredField("authManager");
      f.setAccessible(true);
      AuthManager authManager = (AuthManager) f.get(null);
      if (authManager != null) {
        f.set(null, null);
        authManager.close();
      }

      f = c.getDeclaredField("httpClient");
      f.setAccessible(true);
      RESTClient httpClient = (RESTClient) f.get(null);
      if (httpClient != null) {
        f.set(null, null);
        httpClient.close();
      }

      f = c.getDeclaredField("SIGNED_COMPONENT_CACHE");
      f.setAccessible(true);
      Cache<?, ?> cache = (Cache<?, ?>) f.get(null);
      if (cache != null) {
        cache.invalidateAll();
      }

      LOGGER.info("Cleaned up {}", c.getName());
    } catch (Exception e) {
      LOGGER.error("Failed to clean up {}", S3V4RestSignerClient.class.getName(), e);
    }
  }
}
