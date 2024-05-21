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

import static org.projectnessie.junit.engine.MultiEnvAnnotationUtils.segmentTypeOf;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.jupiter.engine.config.CachingJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.NestedClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.TestMethodTestDescriptor;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.UniqueId.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a JUnit5 Test Engine that delegates test discovery to {@link JupiterTestEngine} and
 * replicates the discovered tests for execution in multiple test environments.
 *
 * <p>When multiple {@link MultiEnvTestExtension}s are applied to the same test, performs a
 * cartesian product of available environments and their IDs.
 *
 * <p>For example:
 *
 * <ul>
 *   <li>MultiEnvTestExtension segmentA with IDs 1, 2, 3
 *   <li>MultiEnvTestExtension segmentB with IDs 1, 2
 *   <li>MultiEnvTestExtension segmentC with ID 1
 * </ul>
 *
 * will result in the following IDs:
 *
 * <ul>
 *   <li>[engine:nessie-multi-env][segmentA:1][segmentB:1][segmentC:1]
 *   <li>[engine:nessie-multi-env][segmentA:1][segmentB:2][segmentC:1]
 *   <li>[engine:nessie-multi-env][segmentA:2][segmentB:1][segmentC:1]
 *   <li>[engine:nessie-multi-env][segmentA:2][segmentB:2][segmentC:1]
 *   <li>[engine:nessie-multi-env][segmentA:3][segmentB:1][segmentC:1]
 *   <li>[engine:nessie-multi-env][segmentA:3][segmentB:2][segmentC:1]
 * </ul>
 *
 * <p>Actual test environments are expected to be managed by JUnit 5 extensions, implementing the
 * {@link MultiEnvTestExtension} interface.
 */
public class MultiEnvTestEngine implements TestEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiEnvTestEngine.class);

  public static final String ENGINE_ID = "nessie-multi-env";

  private static final SegmentTypes ROOT_KEY = new SegmentTypes(Collections.emptyList());
  private static final MultiEnvExtensionRegistry registry = new MultiEnvExtensionRegistry();
  private static final boolean FAIL_ON_MISSING_ENVIRONMENTS =
      !Boolean.getBoolean("org.projectnessie.junit.engine.ignore-empty-environments");

  private final JupiterTestEngine delegate = new JupiterTestEngine();

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

      AtomicBoolean foundAtLeastOneEnvironmentId = new AtomicBoolean();
      List<String> processedExtensionNames = new ArrayList<>();

      ListMultimap<SegmentTypes, TestDescriptor> nodeCache =
          MultimapBuilder.hashKeys().arrayListValues().build();
      nodeCache.put(ROOT_KEY, originalRoot);

      // Scan existing nodes for multi-env test extensions
      originalRoot.accept(
          testDescriptor -> {
            if (testDescriptor instanceof ClassTestDescriptor) {
              Class<?> testClass = ((ClassBasedTestDescriptor) testDescriptor).getTestClass();

              registry.registerExtensions(testClass);

              List<? extends MultiEnvTestExtension> orderedMultiEnvExtensionsOnTest =
                  registry.stream(testClass)
                      .sorted(
                          Comparator.comparing(MultiEnvTestExtension::segmentPriority)
                              .reversed()
                              .thenComparing(MultiEnvAnnotationUtils::segmentTypeOf))
                      .collect(Collectors.toList());

              if (orderedMultiEnvExtensionsOnTest.isEmpty()) {
                return;
              }

              // Construct an intermediate tree of the cartesian product of applied extensions.
              // Multiple nodes will exist at a given level - one for each combination of possible
              // environment IDs (including expanding parent nodes).
              List<TestDescriptor> parentNodes;
              SegmentTypes currentPosition = ROOT_KEY;
              for (MultiEnvTestExtension extension : orderedMultiEnvExtensionsOnTest) {
                processedExtensionNames.add(extension.getClass().getSimpleName());
                parentNodes = nodeCache.get(currentPosition);
                currentPosition = currentPosition.append(segmentTypeOf(extension));

                for (TestDescriptor parentNode : parentNodes) {
                  for (String environmentId :
                      extension.allEnvironmentIds(discoveryRequest.getConfigurationParameters())) {
                    foundAtLeastOneEnvironmentId.set(true);
                    UniqueId newId =
                        parentNode.getUniqueId().append(segmentTypeOf(extension), environmentId);
                    MultiEnvTestDescriptor newChild =
                        new MultiEnvTestDescriptor(newId, environmentId);
                    parentNode.addChild(newChild);
                    nodeCache.put(currentPosition, newChild);
                  }
                }
              }

              // Add this test into each known node at the current level
              List<String> currentSegmentTypes = currentPosition.get();
              for (TestDescriptor nodeAtCurrentPosition : nodeCache.get(currentPosition)) {
                String environmentNames =
                    nodeAtCurrentPosition.getUniqueId().getSegments().stream()
                        .filter(s -> currentSegmentTypes.contains(s.getType()))
                        .map(Segment::getValue)
                        .collect(Collectors.joining(","));

                putTestIntoParent(
                    testDescriptor, nodeAtCurrentPosition, environmentNames, discoveryRequest);
              }
            }
          });

      // Note: this also removes the reference to parent from the child
      originalChildren.forEach(originalRoot::removeChild);

      if (FAIL_ON_MISSING_ENVIRONMENTS
          && !processedExtensionNames.isEmpty()
          && !foundAtLeastOneEnvironmentId.get()) {
        throw new IllegalStateException(
            String.format(
                "%s was enabled, but test extensions %s did not discover any environment IDs.",
                MultiEnvTestEngine.class.getSimpleName(),
                Arrays.toString(processedExtensionNames.toArray())));
      }

      return originalRoot;
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

  /** Immutable key of segment types for the intermediate cartesian product tree. */
  private static class SegmentTypes {

    private final List<String> components;

    public SegmentTypes(List<String> components) {
      this.components = components;
    }

    public List<String> get() {
      return new ArrayList<>(components);
    }

    public SegmentTypes append(String component) {
      List<String> newComponents = new ArrayList<>(components);
      newComponents.add(component);
      return new SegmentTypes(newComponents);
    }

    @Override
    public String toString() {
      return components.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SegmentTypes segmentTypes = (SegmentTypes) o;
      return Objects.equals(components, segmentTypes.components);
    }

    @Override
    public int hashCode() {
      return Objects.hash(components);
    }
  }

  private static void putTestIntoParent(
      TestDescriptor test,
      TestDescriptor parent,
      String environmentNames,
      EngineDiscoveryRequest discoveryRequest) {
    JupiterConfiguration nodeConfiguration =
        new CachingJupiterConfiguration(
            new MultiEnvJupiterConfiguration(
                discoveryRequest.getConfigurationParameters(), environmentNames));

    parent.addChild(nodeWithIdAsChildOf(test, parent.getUniqueId(), nodeConfiguration));
  }

  /**
   * Returns a new TestDescriptor node as if it were a child of the provided parent ID. Recursively
   * generates new children with appropriate IDs, if any.
   */
  private static TestDescriptor nodeWithIdAsChildOf(
      TestDescriptor originalNode, UniqueId parentId, JupiterConfiguration configuration) {
    UniqueId newId = parentId.append(originalNode.getUniqueId().getLastSegment());

    TestDescriptor nodeWithNewId;
    if (originalNode instanceof ClassTestDescriptor) {
      nodeWithNewId =
          new ClassTestDescriptor(
              newId, ((ClassTestDescriptor) originalNode).getTestClass(), configuration);
    } else if (originalNode instanceof NestedClassTestDescriptor) {
      nodeWithNewId =
          new NestedClassTestDescriptor(
              newId, ((NestedClassTestDescriptor) originalNode).getTestClass(), configuration);
    } else if (originalNode instanceof TestMethodTestDescriptor) {
      nodeWithNewId =
          new TestMethodTestDescriptor(
              newId,
              ((TestMethodTestDescriptor) originalNode).getTestClass(),
              ((TestMethodTestDescriptor) originalNode).getTestMethod(),
              configuration);
    } else {
      throw new IllegalArgumentException(
          String.format("Unable to process node of type %s.", originalNode.getClass().getName()));
    }

    for (TestDescriptor originalChild : originalNode.getChildren()) {
      TestDescriptor newChild = nodeWithIdAsChildOf(originalChild, newId, configuration);
      nodeWithNewId.addChild(newChild);
    }

    return nodeWithNewId;
  }
}
