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
package org.projectnessie.services;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;

abstract class BaseParams {
  public static final String DEFAULT_BRANCH_NAME = "main";

  Backend backend;
  VersionStore versionStore;
  BackendTestFactory backendTestFactory;

  protected void init(String backendName) throws Exception {
    Set<String> known = new HashSet<>();
    for (BackendTestFactory candidate : ServiceLoader.load(BackendTestFactory.class)) {
      String name = candidate.getName();
      known.add(name);
      if (backendName.equals(name)) {
        backendTestFactory = candidate;
        break;
      }
    }
    if (backendTestFactory == null) {
      throw new IllegalArgumentException(
          "Could not find backend named " + backendName + ", known backends: " + known);
    }

    backendTestFactory.start();

    backend = backendTestFactory.createNewBackend();
    backend.setupSchema();
    PersistFactory factory = backend.createFactory();
    Persist persist = factory.newPersist(StoreConfig.Adjustable.empty());
    repositoryLogic(persist).initialize(DEFAULT_BRANCH_NAME);
    versionStore = new VersionStoreImpl(persist);
  }

  protected void tearDown() throws Exception {
    if (backend != null) {
      try {
        backend.close();
      } finally {
        backend = null;
      }
    }
    if (backendTestFactory != null) {
      try {
        backendTestFactory.stop();
      } finally {
        backendTestFactory = null;
      }
    }
  }
}
