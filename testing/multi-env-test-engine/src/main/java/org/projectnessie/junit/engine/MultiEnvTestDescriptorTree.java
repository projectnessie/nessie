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

package org.projectnessie.junit.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.engine.config.CachingJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.discovery.DiscoverySelectorResolver;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;

public class MultiEnvTestDescriptorTree {

  private static final boolean FAIL_ON_MISSING_ENVIRONMENTS =
    !Boolean.getBoolean("org.projectnessie.junit.engine.ignore-empty-environments");

  private final TestDescriptor rootNode;
  private final ConfigurationParameters configurationParameters;
  private final AtomicBoolean foundAtLeastOneEnvironmentId = new AtomicBoolean();
  private final List<String> processedExtensionNames = new ArrayList<>();
  private List<TestDescriptor> latestLeafNodes;

  public MultiEnvTestDescriptorTree(TestDescriptor rootNode, ConfigurationParameters configurationParameters) {
    this.rootNode = rootNode;
    this.latestLeafNodes = Collections.singletonList(rootNode);
    this.configurationParameters = configurationParameters;
  }

  public void appendDescriptorsForExtension(MultiEnvTestExtension multiEnvTestExtension) {
    processedExtensionNames.add(multiEnvTestExtension.getClass().getSimpleName());

    List<TestDescriptor> newLeafNodes = new ArrayList<>();
    for (TestDescriptor parentNode : latestLeafNodes) {
      for (String environmentId : multiEnvTestExtension.allEnvironmentIds(configurationParameters)) {
        foundAtLeastOneEnvironmentId.set(true);
        UniqueId newNodeId = parentNode.getUniqueId().append(multiEnvTestExtension.segmentType(), environmentId);
        MultiEnvTestDescriptor newNode = new MultiEnvTestDescriptor(newNodeId, environmentId);
        parentNode.addChild(newNode);
        newLeafNodes.add(newNode);
      }
    }

    latestLeafNodes = newLeafNodes;
  }

  public void addOriginalChildrenToFinishedTree(EngineDiscoveryRequest discoveryRequest) {
    for (TestDescriptor leafNode : latestLeafNodes) {
      JupiterConfiguration nodeConfiguration =
        new CachingJupiterConfiguration(
          new MultiEnvJupiterConfiguration(
            configurationParameters, "TODO")); // TODO
      JupiterEngineDescriptor discoverResult =
        new JupiterEngineDescriptor(leafNode.getUniqueId(), nodeConfiguration);
      new DiscoverySelectorResolver()
        .resolveSelectors(discoveryRequest, discoverResult);
      for (TestDescriptor childWithProperId : discoverResult.getChildren()) {
        leafNode.addChild(childWithProperId);
      }
    }
  }

  public TestDescriptor getRootNode() {
    if (FAIL_ON_MISSING_ENVIRONMENTS && !processedExtensionNames.isEmpty() && !foundAtLeastOneEnvironmentId.get()) {
      throw new IllegalStateException(
        String.format(
          "%s was enabled, but test extensions %s did not discover any environment IDs.",
          MultiEnvTestEngine.class.getSimpleName(), Arrays.toString(processedExtensionNames.toArray())));
    }

    return rootNode;
  }
}
