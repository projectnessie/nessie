/*
 * Copyright (C) 2022 Dremio
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

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/** Version store configuration. */
@StaticInitSafe
@ConfigMapping(prefix = "nessie.version.store")
public interface VersionStoreConfig {

  enum VersionStoreType {
    IN_MEMORY,
    ROCKSDB,
    DYNAMODB,
    MONGODB,
    CASSANDRA,
    JDBC,
    BIGTABLE
  }

  @WithName("type")
  @WithDefault("IN_MEMORY")
  VersionStoreType getVersionStoreType();

  /**
   * Whether events for the version-store are enabled (enabled by default). In order for events to
   * be published, it's not enough to enable them in the configuration; you also need to provide at
   * least one implementation of Nessie's EventListener SPI.
   */
  @WithName("events.enable")
  @WithDefault("true")
  boolean isEventsEnabled();

  @StaticInitSafe
  @ConfigMapping(prefix = "nessie.version.store.rocks")
  interface RocksVersionStoreConfig {
    @WithName("db-path")
    @WithDefault("/tmp/nessie-rocksdb")
    String getDbPath();
  }
}
