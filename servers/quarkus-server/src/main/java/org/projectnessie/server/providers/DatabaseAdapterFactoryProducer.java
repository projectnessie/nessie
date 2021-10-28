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
package org.projectnessie.server.providers;

import io.quarkus.runtime.Startup;
import java.io.IOError;
import java.util.concurrent.TimeUnit;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.server.config.VersionStoreConfig;
import org.projectnessie.server.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.server.providers.StoreType.Literal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Selects the {@link DatabaseAdapterFactory} for the configured store-type. */
@Default
public class DatabaseAdapterFactoryProducer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DatabaseAdapterFactoryProducer.class);

  private final Instance<DatabaseAdapterFactory> databaseAdapterFactory;
  private final VersionStoreConfig storeConfig;

  private static final long START_RETRY_MIN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(2);
  private volatile long lastUnsuccessfulStart = 0L;

  /**
   * Database adapter factory selector/producer.
   *
   * @param databaseAdapterFactory a CDI injector for {@link DatabaseAdapterFactory}
   * @param storeConfig the version store configuration
   */
  @Inject
  public DatabaseAdapterFactoryProducer(
      @Any Instance<DatabaseAdapterFactory> databaseAdapterFactory,
      VersionStoreConfig storeConfig) {
    this.databaseAdapterFactory = databaseAdapterFactory;
    this.storeConfig = storeConfig;
  }

  /** Database adapter factory selector/producer. */
  @Produces
  @Singleton
  @Startup
  public DatabaseAdapterFactory selectDatabaseAdapterFactory() {
    VersionStoreType versionStoreType = storeConfig.getVersionStoreType();

    if (System.nanoTime() - lastUnsuccessfulStart < START_RETRY_MIN_INTERVAL_NANOS) {
      LOGGER.warn("{} version store failed to start recently, try again later.", versionStoreType);
      throw new RuntimeException(
          String.format(
              "%s version store failed to start recently, try again later.", versionStoreType));
    }

    try {
      LOGGER.info("Using {} Version store", versionStoreType);

      DatabaseAdapterFactory databaseAdapter =
          databaseAdapterFactory.select(new Literal(versionStoreType)).get();
      lastUnsuccessfulStart = 0L;
      return databaseAdapter;
    } catch (RuntimeException | IOError e) {
      lastUnsuccessfulStart = System.nanoTime();
      LOGGER.error("Failed to configure/start {} version store", versionStoreType, e);
      throw e;
    }
  }
}
