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
package org.projectnessie.tools.compatibility.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.jupiter.engine.config.CachingJupiterConfiguration;
import org.junit.jupiter.engine.config.DefaultJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;
import org.projectnessie.tools.compatibility.api.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit test engine implementation that leverages {@link JupiterTestEngine} to discover and execute
 * tests and run those tests once for each Nessie version to test.
 */
public class MultiNessieVersionsTestEngine implements TestEngine {

  public static final String ENGINE_ID = "nessie-versions";
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiNessieVersionsTestEngine.class);

  private final JupiterTestEngine delegate;

  public MultiNessieVersionsTestEngine() {
    delegate = new JupiterTestEngine();
  }

  @Override
  public String getId() {
    return ENGINE_ID;
  }

  public Optional<String> getGroupId() {
    return Optional.of("org.projectnessie");
  }

  public Optional<String> getArtifactId() {
    return Optional.of("nessie-compatibility-common");
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId uniqueId) {
    try {

      // JupiterEngineDescriptor must be the root, that's what the JUnit Jupiter engine
      // implementation expects.
      JupiterConfiguration configuration =
          new CachingJupiterConfiguration(
              new DefaultJupiterConfiguration(discoveryRequest.getConfigurationParameters()));
      JupiterEngineDescriptor engineDescriptor =
          new JupiterEngineDescriptor(uniqueId, configuration);

      SortedSet<Version> versionsToTest;
      try {
        versionsToTest = VersionsToExercise.versionsForEngine(discoveryRequest);
      } catch (IllegalStateException e) {
        // No versions to test found - return early
        return engineDescriptor;
      }

      for (Version version : versionsToTest) {
        UniqueId perVersionId = uniqueId.append("nessie-version", version.toString());
        NessieVersionTestDescriptor perVersionTestDescriptor =
            new NessieVersionTestDescriptor(perVersionId, version);
        engineDescriptor.addChild(perVersionTestDescriptor);

        TestDescriptor discoverResult = delegate.discover(discoveryRequest, perVersionId);
        List<? extends TestDescriptor> children = new ArrayList<>(discoverResult.getChildren());
        for (TestDescriptor child : children) {
          discoverResult.removeChild(child);
          perVersionTestDescriptor.addChild(child);
        }
      }

      return engineDescriptor;
    } catch (Exception e) {
      LOGGER.error("Failed to discover tests", e);
      throw new RuntimeException(e);
    }
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

  private static final class NessieVersionTestDescriptor extends AbstractTestDescriptor {

    NessieVersionTestDescriptor(UniqueId uniqueId, Version version) {
      super(uniqueId, "Nessie " + version);
    }

    @Override
    public Type getType() {
      return Type.CONTAINER;
    }
  }
}
