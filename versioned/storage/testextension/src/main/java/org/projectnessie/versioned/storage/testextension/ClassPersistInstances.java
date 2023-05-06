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
package org.projectnessie.versioned.storage.testextension;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.testextension.PersistExtension.KEY_REUSABLE_BACKEND;
import static org.projectnessie.versioned.storage.testextension.PersistExtension.NAMESPACE;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.projectnessie.versioned.storage.cache.CacheBackend;
import org.projectnessie.versioned.storage.cache.PersistCaches;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;

final class ClassPersistInstances {

  private final List<Persist> persistInstances = new ArrayList<>();
  private final CacheBackend cacheBackend;
  private final Backend backend;
  private final PersistFactory persistFactory;

  ClassPersistInstances(ExtensionContext context) {
    Store rootStore = context.getRoot().getStore(NAMESPACE);
    ReusableTestBackend reusableTestBackend =
        rootStore.getOrComputeIfAbsent(
            KEY_REUSABLE_BACKEND, k -> new ReusableTestBackend(), ReusableTestBackend.class);

    NessiePersistCache nessiePersistCache =
        PersistExtension.annotationInstance(context, NessiePersistCache.class);
    cacheBackend =
        nessiePersistCache != null ? PersistCaches.newBackend(nessiePersistCache.capacity()) : null;

    backend = reusableTestBackend.backend(context);

    backend.setupSchema();

    persistFactory = backend.createFactory();
  }

  PersistFactory persistFactory() {
    return persistFactory;
  }

  Backend backend() {
    return backend;
  }

  void registerPersist(Persist persist) {
    persistInstances.add(persist);
  }

  void reinitialize() {
    persistInstances.forEach(p -> reinit(p, true));
  }

  static void reinit(Persist persist, boolean initialize) {
    persist.erase();
    if (initialize) {
      RepositoryLogic setup = repositoryLogic(persist);
      setup.initialize("main");
    }
  }

  public Persist newPersist(StoreConfig config) {
    Persist persist = persistFactory.newPersist(config);

    if (cacheBackend != null) {
      persist = cacheBackend.wrap(persist);
    }

    return persist;
  }
}
