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
package org.projectnessie.junit.engine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a JUnit5 Test Engine that delegates test discovery to {@link JupiterTestEngine} and
 * replicates the discovered tests for execution in multiple test environments.
 *
 * <p>When multiple {@link MultiEnvTestExtension}s are applied to the same test, performs a cartesian
 * product of available environments and their IDs.
 *
 * <p>For example:
 * <ul>
 *   <li>MultiEnvTestExtension segmentA with IDs 1, 2, 3</li>
 *   <li>MultiEnvTestExtension segmentB with IDs 1, 2</li>
 *   <li>MultiEnvTestExtension segmentC with ID 1</li>
 * </ul>
 * will result in the following IDs:
 * <ul>
 *   <li>[engine:nessie-multi-env][segmentA:1][segmentB:1][segmentC:1]</li>
 *   <li>[engine:nessie-multi-env][segmentA:1][segmentB:2][segmentC:1]</li>
 *   <li>[engine:nessie-multi-env][segmentA:2][segmentB:1][segmentC:1]</li>
 *   <li>[engine:nessie-multi-env][segmentA:2][segmentB:2][segmentC:1]</li>
 *   <li>[engine:nessie-multi-env][segmentA:3][segmentB:1][segmentC:1]</li>
 *   <li>[engine:nessie-multi-env][segmentA:3][segmentB:2][segmentC:1]</li>
 * </ul>
 *
 * <p>Actual test environments are expected to be managed by JUnit 5 extensions, implementing the
 * {@link MultiEnvTestExtension} interface.
 */
public class MultiEnvTestEngine implements TestEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiEnvTestEngine.class);
  private static final MultiEnvExtensionRegistry FILTER_REGISTRY = new MultiEnvExtensionRegistry();

  public static final String ENGINE_ID = "nessie-multi-env";

  private final JupiterTestEngine delegate = new JupiterTestEngine();

  static MultiEnvExtensionRegistry registry() {
    return FILTER_REGISTRY;
  }

  @Override
  public String getId() {
    return ENGINE_ID;
  }

  @Override
  public void execute(ExecutionRequest request) {
    delegate.execute(request);
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId uniqueId) {
    try {
      TestDescriptor originalRoot = delegate.discover(discoveryRequest, uniqueId);
      List<TestDescriptor> originalChildren = new ArrayList<>(originalRoot.getChildren());

      // Scan for multi-env test extensions
      final MultiEnvExtensionRegistry currentRegistry = new MultiEnvExtensionRegistry();
      originalRoot.accept(
          descriptor -> {
            if (descriptor instanceof ClassBasedTestDescriptor) {
              Class<?> testClass = ((ClassBasedTestDescriptor) descriptor).getTestClass();
              FILTER_REGISTRY.registerExtensions(testClass);
              currentRegistry.registerExtensions(testClass);
            }
          });

      // Note: this also removes the reference to parent from the child
      originalChildren.forEach(originalRoot::removeChild);

      // Append each extension's IDs in a new, nested, layer
      MultiEnvTestDescriptorTree tree = new MultiEnvTestDescriptorTree(originalRoot, discoveryRequest.getConfigurationParameters());
      currentRegistry.stream()
          .sorted(Comparator.comparing(ext -> ext.getClass().getSimpleName()))
          .forEach(tree::appendDescriptorsForExtension);

      // Migrate the actual tests from the root to each of the leaves of the new tree
      tree.addOriginalChildrenToFinishedTree(discoveryRequest);

      return tree.getRootNode();
    } catch (Exception e) {
      LOGGER.error("Failed to discover tests", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<String> getGroupId() {
    return Optional.of("org.projectnessie.nessie");
  }

  @Override
  public Optional<String> getArtifactId() {
    return Optional.of("nessie-multi-env-test-engine");
  }
}
