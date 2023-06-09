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
package org.projectnessie.tools.compatibility.jersey;

import static org.projectnessie.tools.compatibility.jersey.ServerConfigExtension.SERVER_CONFIG;

import java.util.Map;
import org.projectnessie.versioned.persist.tests.SystemPropertiesConfigurer;
import org.projectnessie.versioned.storage.common.config.ImmutableAdjustable;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.Logics;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;

public final class Backends {
  private Backends() {}

  public static <CONFIG> Backend createBackend(String backendName) {
    BackendFactory<CONFIG> factory = PersistLoader.findFactoryByName(backendName);
    Backend backend = factory.buildBackend(factory.newConfigInstance());
    backend.setupSchema();
    return backend;
  }

  public static Persist createPersist(
      Backend backend, boolean initializeRepository, Map<String, String> configuration) {
    PersistFactory persistFactory = backend.createFactory();
    StoreConfig storeConfig =
        SystemPropertiesConfigurer.configureFromPropertiesGeneric(
            ImmutableAdjustable.builder().build(), StoreConfig.class, configuration::get);
    Persist persist = persistFactory.newPersist(storeConfig);

    if (initializeRepository) {
      persist.erase();
      Logics.repositoryLogic(persist).initialize(SERVER_CONFIG.getDefaultBranch());
    }

    return persist;
  }
}
