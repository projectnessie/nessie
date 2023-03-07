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
package org.projectnessie.quarkus.cli;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.util.Map;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.mongodb.MongoDBBackendTestFactory;

/**
 * A JUnit5 extension that sets up the execution environment for Nessie CLI tests.
 *
 * <p>MongoDB storage is used.
 *
 * <p>A {@link Persist} instance is created and injected into tests for manipulating the test Nessie
 * repository
 *
 * <p>The test Nessie repository is erased and re-created for each test case.
 */
public class NessieCliPersistTestExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {

  private static final Namespace NAMESPACE = Namespace.create(NessieCliPersistTestExtension.class);
  private static final String MONGO_KEY = "mongo";
  private static final String PERSIST_KEY = "adapter";

  @Override
  public void beforeAll(ExtensionContext context) {
    // Maintain one MongoDB instance for all tests (store it in the root context).
    BackendHolder mongo =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(MONGO_KEY, key -> createBackend(), BackendHolder.class);

    // Quarkus runtime will pick up relevant values from java system properties.
    mongo.config.forEach(System::setProperty);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    // Use one Backend instance for all tests (adapters are stateless)
    BackendHolder backend = context.getStore(NAMESPACE).get(MONGO_KEY, BackendHolder.class);
    if (backend == null) {
      throw new IllegalStateException("MongoDB was not initialized");
    }

    context
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            PERSIST_KEY,
            x -> {
              PersistFactory persistFactory = backend.backend.createFactory();
              Persist persist =
                  persistFactory.newPersist(
                      StoreConfig.Adjustable.empty()
                          .withRepositoryId(BaseConfigProfile.TEST_REPO_ID));

              // ... but reset the repo for each test.
              persist.erase();
              repositoryLogic(persist).initialize("main");
              return persist;
            },
            Persist.class);
  }

  static final class BackendHolder implements CloseableResource {
    final Backend backend;
    final Map<String, String> config;

    BackendHolder(Backend backend, Map<String, String> config) {
      this.backend = backend;
      this.config = config;
    }

    @Override
    public void close() throws Exception {
      backend.close();
    }
  }

  private BackendHolder createBackend() {
    try {
      MongoDBBackendTestFactory backendTestFactory = new MongoDBBackendTestFactory();
      backendTestFactory.start();
      Backend backend = backendTestFactory.createNewBackend();

      return new BackendHolder(backend, backendTestFactory.getQuarkusConfig());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().isAssignableFrom(Persist.class);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    Persist persist = context.getStore(NAMESPACE).get(PERSIST_KEY, Persist.class);

    if (persist == null) {
      throw new IllegalStateException("Persist was not initialized");
    }

    return persist;
  }
}
