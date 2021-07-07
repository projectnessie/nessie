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

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.config.ConfigMapping;

/** Version store configuration. */
@ConfigMapping(prefix = "nessie.version.store")
public interface VersionStoreConfig {

  @RegisterForReflection
  public enum VersionStoreType {
    DYNAMO,
    INMEMORY,
    JGIT
  }

  @ConfigProperty(name = "type", defaultValue = "INMEMORY")
  VersionStoreType getVersionStoreType();

  /**
   * Whether calls against the version-store are traced with OpenTracing/OpenTelemetry (Jaeger),
   * enabled by default.
   */
  @ConfigProperty(name = "trace.enable", defaultValue = "true")
  boolean isTracingEnabled();

  /** Whether metrics for the version-store are enabled (enabled by default). */
  @ConfigProperty(name = "metrics.enable", defaultValue = "true")
  boolean isMetricsEnabled();
}
