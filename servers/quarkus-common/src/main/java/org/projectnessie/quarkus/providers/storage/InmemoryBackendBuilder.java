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
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.IN_MEMORY;

import jakarta.enterprise.context.Dependent;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendConfig;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;

/** In-memory version store factory. */
@StoreType(IN_MEMORY)
@Dependent
public class InmemoryBackendBuilder implements BackendBuilder {
  @Override
  public Backend buildBackend() {
    InmemoryBackendConfig cfg = InmemoryBackendConfig.builder().build();
    InmemoryBackendFactory factory = new InmemoryBackendFactory();
    return factory.buildBackend(cfg);
  }
}
