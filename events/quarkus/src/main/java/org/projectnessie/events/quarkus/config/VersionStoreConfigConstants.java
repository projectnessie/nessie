/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.events.quarkus.config;

/**
 * Constants from the version store configuration.
 *
 * <p>This module does not depend on nessie-quarkus-common, so we cannot reference or use the
 * VersionStoreConfig class. But this module reacts to a few config options defined below.
 */
public class VersionStoreConfigConstants {

  /** See {@code org.projectnessie.quarkus.config.VersionStoreConfig.isEventsEnabled()}. */
  public static final String NESSIE_VERSION_STORE_EVENTS_ENABLE =
      "nessie.version.store.events.enable";

  /** See {@code org.projectnessie.quarkus.config.VersionStoreConfig.isTracingEnabled()}. */
  public static final String NESSIE_VERSION_STORE_TRACE_ENABLE =
      "nessie.version.store.trace.enable";

  /** See {@code org.projectnessie.quarkus.config.VersionStoreConfig.isMetricsEnabled()}. */
  public static final String NESSIE_VERSION_STORE_METRICS_ENABLE =
      "nessie.version.store.metrics.enable";
}
