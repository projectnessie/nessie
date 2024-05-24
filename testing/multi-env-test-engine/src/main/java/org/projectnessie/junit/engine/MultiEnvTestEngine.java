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

import static org.projectnessie.junit.engine.MultiEnvAnnotationUtils.findNestedMultiEnvTestExtensionsOn;
import static org.projectnessie.junit.engine.MultiEnvAnnotationUtils.segmentTypeOf;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.jupiter.engine.config.CachingJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.discovery.DiscoverySelectorResolver;
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

  private static final SegmentTypes ROOT_KEY =
      SegmentTypes.newBuilder().components(Collections.emptyList()).build();
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
              List<String> currentSegmentTypes = currentPosition.components();
              for (TestDescriptor currentNode : nodeCache.get(currentPosition)) {
                // Add suffix to test names for usages that do not know how to read UniqueIds
                String environment =
                    currentNode.getUniqueId().getSegments().stream()
                        .filter(s -> currentSegmentTypes.contains(s.getType()))
                        .map(Segment::getValue)
                        .collect(Collectors.joining(","));

                JupiterConfiguration nodeConfiguration =
                    new CachingJupiterConfiguration(
                        new MultiEnvJupiterConfiguration(
                            discoveryRequest.getConfigurationParameters(), environment));

                // Find tests as if they existed as children of the current node
                JupiterEngineDescriptor discoverResult =
                    new JupiterEngineDescriptor(currentNode.getUniqueId(), nodeConfiguration);
                new DiscoverySelectorResolver().resolveSelectors(discoveryRequest, discoverResult);

                List<? extends TestDescriptor> classBasedChildren =
                    discoverResult.getChildren().stream()
                        .filter(child -> child instanceof ClassBasedTestDescriptor)
                        .collect(Collectors.toList());
                for (TestDescriptor child : classBasedChildren) {
                  Class<?> childTestClass = ((ClassBasedTestDescriptor) child).getTestClass();

                  Set<String> segmentTypesOnChild =
                      findNestedMultiEnvTestExtensionsOn(childTestClass)
                          .map(MultiEnvAnnotationUtils::segmentTypeOf)
                          .collect(Collectors.toUnmodifiableSet());

                  if (segmentTypesOnChild.equals(new HashSet<>(currentPosition.components()))) {
                    currentNode.addChild(child);
                  }
                }
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
  @Value.Immutable
  @Value.Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
  interface SegmentTypes {

    List<String> components();

    default SegmentTypes append(String component) {
      return newBuilder().components(components()).addComponents(component).build();
    }

    class Builder extends ImmutableSegmentTypes.Builder {}

    static Builder newBuilder() {
      return new Builder();
    }
  }
}
