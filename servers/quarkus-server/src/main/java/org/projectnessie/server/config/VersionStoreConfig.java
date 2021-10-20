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
package org.projectnessie.server.config;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/** Version store configuration. */
@ConfigMapping(prefix = "nessie.version.store")
public interface VersionStoreConfig {

  @RegisterForReflection
  enum VersionStoreType {
    DYNAMO,
    INMEMORY,
    ROCKS,
    MONGO,
  }

  @WithName("type")
  @WithDefault("INMEMORY")
  VersionStoreType getVersionStoreType();

  /**
   * Whether calls against the version-store are traced with OpenTracing/OpenTelemetry (Jaeger),
   * enabled by default.
   */
  @WithName("trace.enable")
  @WithDefault("true")
  boolean isTracingEnabled();

  /** Whether metrics for the version-store are enabled (enabled by default). */
  @WithName("metrics.enable")
  @WithDefault("true")
  boolean isMetricsEnabled();

  @ConfigMapping(prefix = "nessie.version.store.rocks")
  interface RocksVersionStoreConfig {
    @WithName("db-path")
    @WithDefault("/tmp/nessie-rocksdb")
    String getDbPath();
  }
}
