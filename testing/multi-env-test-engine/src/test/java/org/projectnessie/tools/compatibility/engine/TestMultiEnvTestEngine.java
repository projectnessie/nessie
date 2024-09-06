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
package org.projectnessie.tools.compatibility.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.projectnessie.junit.engine.MultiEnvTestEngine;
import org.projectnessie.junit.engine.MultiEnvTestExtension;
import org.projectnessie.junit.engine.MultiEnvTestFilter;

class TestMultiEnvTestEngine {
  @Test
  void junitJupiter() {
    // Validates that the filter permits plain test in the Jupiter test engine
    assertThat(
            EngineTestKit.engine(JupiterEngineDescriptor.ENGINE_ID)
                .selectors(selectClass(TestMultiEnvTestEngine.PlainTest.class))
                .filters(new MultiEnvTestFilter())
                .execute()
                .testEvents()
                .list()
                .stream()
                .map(e -> e.getTestDescriptor().getUniqueId()))
        .isEmpty();
  }

  @Test
  void plainTest() {
    assertThat(
            EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
                .selectors(selectClass(PlainTest.class))
                .filters(new MultiEnvTestFilter())
                .execute()
                .testEvents()
                .list()
                .stream()
                .map(e -> e.getTestDescriptor().getUniqueId()))
        .isNotEmpty()
        .noneSatisfy(
            id -> assertThat(id.getEngineId()).hasValue(JupiterEngineDescriptor.ENGINE_ID));
  }

  @Test
  void multiEnv() {
    Set<UniqueId> ids =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .selectors(selectClass(MultiEnvAcceptedTest.class))
            .selectors(selectClass(MultiEnvAcceptedTest.Inner.class))
            .filters(new MultiEnvTestFilter())
            .execute()
            .testEvents()
            .list()
            .stream()
            .map(e -> e.getTestDescriptor().getUniqueId())
            .collect(Collectors.toSet());
    assertThat(ids).hasSize(4); // 2 from the outer class, 2 from the inner
    assertThat(ids)
        .allSatisfy(id -> assertThat(id.getEngineId()).hasValue(MultiEnvTestEngine.ENGINE_ID));
    assertThat(
            ids.stream()
                .flatMap(
                    id ->
                        id.getSegments().stream()
                            .filter(s -> "test-segment".equals(s.getType()))
                            .map(UniqueId.Segment::getValue)))
        .containsExactlyInAnyOrder("TE1", "TE2", "TE1", "TE2");
  }

  public static class PlainTest {
    @Test
    void test() {
      // nop
    }

    @Nested
    class Inner {
      @Test
      void test() {
        // nop
      }
    }
  }

  @ExtendWith(TestExtension.class)
  public static class MultiEnvAcceptedTest {
    @Test
    void test() {
      // nop
    }

    @Nested
    class Inner {
      @Test
      void test() {
        // nop
      }
    }
  }

  public static class TestExtension implements MultiEnvTestExtension {
    @Override
    public String segmentType() {
      return "test-segment";
    }

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return Arrays.asList("TE1", "TE2");
    }
  }
}
