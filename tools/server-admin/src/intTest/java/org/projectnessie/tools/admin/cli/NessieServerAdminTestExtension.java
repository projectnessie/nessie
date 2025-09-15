/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.admin.cli;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.UniqueId;
import org.projectnessie.junit.engine.MultiEnvTestExtension;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.common.config.StoreConfig.Adjustable;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;

/**
 * A JUnit5 extension that sets up the execution environment for Nessie CLI tests.
 *
 * <p>A {@link Persist} instance is created and injected into tests for manipulating the test Nessie
 * repository
 *
 * <p>The test Nessie repository is erased and re-created for each test case.
 */
public class NessieServerAdminTestExtension
    implements BeforeAllCallback,
        BeforeEachCallback,
        AfterAllCallback,
        ParameterResolver,
        MultiEnvTestExtension {

  private static final Namespace NAMESPACE = Namespace.create(NessieServerAdminTestExtension.class);
  private static final String BACKEND = "backend";
  private static final String BACKENDS_SYSTEM_PROPERTY = "backends";

  @Override
  public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
    String backends = System.getProperty(BACKENDS_SYSTEM_PROPERTY);
    if (backends != null && !backends.isBlank()) {
      return Arrays.asList(backends.split(","));
    }
    return Arrays.stream(NessieServerAdminTestBackends.values()).map(Enum::name).toList();
  }

  @Override
  public String segmentType() {
    return BACKEND;
  }

  static NessieServerAdminTestBackends environment(ExtensionContext context) {
    return UniqueId.parse(context.getUniqueId()).getSegments().stream()
        .filter(s -> BACKEND.equals(s.getType()))
        .map(UniqueId.Segment::getValue)
        .findFirst()
        .map(NessieServerAdminTestBackends::valueOf)
        .orElseThrow();
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    BackendHolder backend = getOrCreateBackend(context);

    // Quarkus runtime will pick up relevant values from java system properties.
    backend.config.forEach(System::setProperty);
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    BackendHolder backend = getOrCreateBackend(context);
    backend.config.keySet().forEach(System::clearProperty);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    // Use one Persist instance for all tests (instances are stateless)
    Persist persist = getOrCreateBackend(context).persist();
    // ... but reset the repo for each test.
    persist.erase();
    repositoryLogic(persist).initialize("main");
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().isAssignableFrom(Persist.class);
  }

  @Override
  public Persist resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    try {
      return getOrCreateBackend(context).persist();
    } catch (Exception e) {
      throw new ParameterResolutionException("Failed to create Persist instance", e);
    }
  }

  private BackendHolder getOrCreateBackend(ExtensionContext context) throws Exception {
    Store store = context.getRoot().getStore(NAMESPACE);
    NessieServerAdminTestBackends environment = environment(context);
    BackendHolder previous = store.get(BACKEND, BackendHolder.class);
    if (previous != null && previous.environment() != environment) {
      // The environment has changed, remove the previous entry.
      store.remove(BACKEND);
      previous.close();
    }
    return store.getOrComputeIfAbsent(
        BACKEND, key -> createBackendHolder(environment), BackendHolder.class);
  }

  private BackendHolder createBackendHolder(NessieServerAdminTestBackends environment) {
    try {
      BackendTestFactory factory = environment.backendFactory();
      factory.start();
      Backend backend = factory.createNewBackend();
      backend.setupSchema();
      Persist persist =
          backend
              .createFactory()
              .newPersist(Adjustable.empty().withRepositoryId(BaseConfigProfile.TEST_REPO_ID));
      Map<String, String> config = new HashMap<>(factory.getQuarkusConfig());
      config.putAll(environment.quarkusConfig());
      return new BackendHolder(environment, persist, backend, factory, config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  record BackendHolder(
      NessieServerAdminTestBackends environment,
      Persist persist,
      Backend backend,
      BackendTestFactory factory,
      Map<String, String> config)
      implements AutoCloseable {

    @Override
    public void close() throws Exception {
      try {
        backend.close();
      } finally {
        factory.stop();
      }
    }
  }
}
