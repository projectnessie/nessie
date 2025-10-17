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
package org.projectnessie.junit.engine;

import static org.projectnessie.junit.engine.JUnitCompat.newDefaultJupiterConfiguration;

import java.util.Optional;
import org.junit.jupiter.engine.config.CachingJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.discovery.DiscoverySelectorResolver;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.jupiter.engine.execution.LauncherStoreFacade;
import org.junit.jupiter.engine.support.JupiterThrowableCollectorFactory;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.hierarchical.HierarchicalTestEngine;
import org.junit.platform.engine.support.hierarchical.HierarchicalTestExecutorService;
import org.junit.platform.engine.support.hierarchical.ThrowableCollector;
import org.junit.platform.engine.support.store.NamespacedHierarchicalStore;

public class ThreadPerTestClassExecutionTestEngine
    extends HierarchicalTestEngine<JupiterEngineExecutionContext> {

  @Override
  public String getId() {
    return JupiterEngineDescriptor.ENGINE_ID;
  }

  /** Returns {@code org.junit.jupiter} as the group ID. */
  @Override
  public Optional<String> getGroupId() {
    return Optional.of("org.junit.jupiter");
  }

  /** Returns {@code junit-jupiter-engine} as the artifact ID. */
  @Override
  public Optional<String> getArtifactId() {
    return Optional.of("junit-jupiter-engine");
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId uniqueId) {
    JupiterConfiguration configuration =
        new CachingJupiterConfiguration(newDefaultJupiterConfiguration(discoveryRequest));
    JupiterEngineDescriptor engineDescriptor = new JupiterEngineDescriptor(uniqueId, configuration);
    new DiscoverySelectorResolver().resolveSelectors(discoveryRequest, engineDescriptor);
    return engineDescriptor;
  }

  @Override
  protected HierarchicalTestExecutorService createExecutorService(ExecutionRequest request) {
    return new ThreadPerTestClassExecutionExecutorService();
  }

  @Override
  protected JupiterEngineExecutionContext createExecutionContext(ExecutionRequest request) {
    try {
      return createExecutionContext513(request);
    } catch (Exception e) {
      return createExecutionContext514(request);
    }
  }

  private JupiterEngineExecutionContext createExecutionContext514(ExecutionRequest request) {
    // LauncherStoreFacade moved to a different package in JUnit 5.14.
    return new JupiterEngineExecutionContext(
        request.getEngineExecutionListener(),
        getJupiterConfiguration(request),
        new LauncherStoreFacade(request.getStore()));
  }

  private JupiterEngineExecutionContext createExecutionContext513(ExecutionRequest request)
      throws Exception {
    var classLauncherStoreFacade =
        Class.forName("org.junit.jupiter.engine.descriptor.LauncherStoreFacade");
    var ctorLauncherStoreFacade =
        classLauncherStoreFacade.getDeclaredConstructor(NamespacedHierarchicalStore.class);
    var launcherStoreFacade = ctorLauncherStoreFacade.newInstance(request.getStore());
    var ctorJupiterEngineExecutionContext =
        JupiterEngineExecutionContext.class.getDeclaredConstructor(
            EngineExecutionListener.class, JupiterConfiguration.class, classLauncherStoreFacade);
    return ctorJupiterEngineExecutionContext.newInstance(
        request.getEngineExecutionListener(),
        getJupiterConfiguration(request),
        launcherStoreFacade);
  }

  @Override
  protected ThrowableCollector.Factory createThrowableCollectorFactory(ExecutionRequest request) {
    return JupiterThrowableCollectorFactory::createThrowableCollector;
  }

  private JupiterConfiguration getJupiterConfiguration(ExecutionRequest request) {
    JupiterEngineDescriptor engineDescriptor =
        (JupiterEngineDescriptor) request.getRootTestDescriptor();
    return engineDescriptor.getConfiguration();
  }
}
