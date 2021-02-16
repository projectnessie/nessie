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

import static org.projectnessie.versioned.impl.TieredVersionStoreConfig.DEFAULT_COMMIT_RETRY_COUNT;
import static org.projectnessie.versioned.impl.TieredVersionStoreConfig.DEFAULT_P2_COMMIT_RETRY_COUNT;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.arc.config.ConfigProperties;

@ConfigProperties(prefix = "nessie.version.store.tiered")
public interface TieredVersionStoreConfig {

  @ConfigProperty(name = "commit-retry-count", defaultValue = DEFAULT_COMMIT_RETRY_COUNT)
  int commitRetryCount();

  @ConfigProperty(name = "p2-commit-retry-count", defaultValue = DEFAULT_P2_COMMIT_RETRY_COUNT)
  int p2CommitRetryCount();
}
