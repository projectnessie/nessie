/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.test.client;

import static org.projectnessie.test.compatibility.TestClassesGenerator.resolveTestClasses;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JUnitEngineDelegate implements TestEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(JUnitEngineDelegate.class);

  private final JupiterTestEngine delegate;
  private final Pattern exclusionPattern;

  public JUnitEngineDelegate() {
    delegate = new JupiterTestEngine();

    String testExclusions = System.getProperty("testExclusions");
    exclusionPattern = testExclusions != null ? Pattern.compile(testExclusions) : null;
  }

  @Override
  public String getId() {
    return "nessie-old-clients";
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest engineDiscoveryRequest, UniqueId uniqueId) {
    try {
      String nessieVersions = System.getProperty("nessieClientVersions");
      if (nessieVersions == null || nessieVersions.trim().isEmpty()) {
        return new SkipEngineDescriptor(uniqueId);
      }

      List<DiscoverySelector> selectors =
          Arrays.stream(nessieVersions.split(","))
              .map(String::trim)
              .flatMap(
                  version ->
                      resolveTestClasses(
                          version, version, version.replace('.', '_').replace('-', '_'))
                          .stream())
              .map(DiscoverySelectors::selectClass)
              .collect(Collectors.toList());

      LauncherDiscoveryRequest request =
          LauncherDiscoveryRequestBuilder.request().selectors(selectors).build();

      TestDescriptor discoverResult = delegate.discover(request, uniqueId);
      filterTestDescriptor(discoverResult);
      return discoverResult;
    } catch (Exception e) {
      LOGGER.error("Failed to discover tests", e);
      throw new RuntimeException(e);
    }
  }

  private void filterTestDescriptor(TestDescriptor testDescriptor) {
    if (exclusionPattern != null
        && exclusionPattern.matcher(testDescriptor.getDisplayName()).matches()) {
      testDescriptor.removeFromHierarchy();
    }
    new ArrayList<>(testDescriptor.getChildren()).forEach(this::filterTestDescriptor);
  }

  @Override
  public void execute(ExecutionRequest executionRequest) {
    if (executionRequest.getRootTestDescriptor() instanceof SkipEngineDescriptor) {
      return;
    }
    delegate.execute(executionRequest);
  }

  private static final class SkipEngineDescriptor extends EngineDescriptor {
    SkipEngineDescriptor(UniqueId id) {
      super(id, "No Nessie versions");
    }
  }
}
