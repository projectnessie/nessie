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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.descriptor.NestedClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.TestMethodTestDescriptor;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.testkit.engine.EngineTestKit;

class TestMultiEnvTestEngine {
  private static final List<UniqueId> PLAIN_TEST_ON_JUPITER_ENGINE_IDS = List.of(
    UniqueId.forEngine(JupiterEngineDescriptor.ENGINE_ID)
      .append(ClassTestDescriptor.SEGMENT_TYPE, PlainTest.class.getName())
      .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
    UniqueId.forEngine(JupiterEngineDescriptor.ENGINE_ID)
      .append(ClassTestDescriptor.SEGMENT_TYPE, PlainTest.class.getName())
      .append(NestedClassTestDescriptor.SEGMENT_TYPE, "Inner")
      .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()")
  );

  private static final List<UniqueId> MULTI_ENV_EXTENSION_2_ON_MULTI_ENV_ENGINE_IDS = List.of(
    UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
      .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
      .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
      .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
    UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
      .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
      .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
      .append(NestedClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.Inner.class.getSimpleName())
      .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
    UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
      .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
      .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
      .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
    UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
      .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
      .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
      .append(NestedClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.Inner.class.getSimpleName())
      .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()")
  );

  @BeforeEach
  public void beforeEach() {
    MultiEnvTestEngine.clearRegistry();
    MultiEnvTestFilter.clear();
  }

  @Test
  void plainTestOnJunitJupiter() {
    // Validates that the filter permits plain test in the Jupiter test engine
    Set<UniqueId> uniqueTestIds = EngineTestKit.engine(JupiterEngineDescriptor.ENGINE_ID)
      .selectors(selectClass(PlainTest.class))
      .filters(new MultiEnvTestFilter())
      .execute()
      .testEvents()
      .list()
      .stream()
      .map(e -> e.getTestDescriptor().getUniqueId())
      .collect(Collectors.toSet());

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(PLAIN_TEST_ON_JUPITER_ENGINE_IDS);
  }

  @Test
  void plainTestOnMultiEnv() {
    Set<UniqueId> uniqueTestIds = EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
      .selectors(selectClass(PlainTest.class))
      .filters(new MultiEnvTestFilter())
      .execute()
      .testEvents()
      .list()
      .stream()
      .map(e -> e.getTestDescriptor().getUniqueId())
      .collect(Collectors.toSet());

    assertThat(uniqueTestIds).isEmpty();
  }

  @Test
  void multiEnvOnJunitJupiter() {
    Set<UniqueId> uniqueTestIds = EngineTestKit.engine(JupiterEngineDescriptor.ENGINE_ID)
      .selectors(selectClass(MultiEnvAcceptedTest.class))
      .selectors(selectClass(MultiEnvAcceptedTest.Inner.class))
      .filters(new MultiEnvTestFilter())
      .execute()
      .testEvents()
      .list()
      .stream()
      .map(e -> e.getTestDescriptor().getUniqueId())
      .collect(Collectors.toSet());

    assertThat(uniqueTestIds).isEmpty();
  }

  @Test
  void multiEnvOnMultiEnv() {
    Set<UniqueId> uniqueTestIds = EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
      .selectors(selectClass(MultiEnvAcceptedTest.class))
      .selectors(selectClass(MultiEnvAcceptedTest.Inner.class))
      .filters(new MultiEnvTestFilter())
      .execute()
      .testEvents()
      .list()
      .stream()
      .map(e -> e.getTestDescriptor().getUniqueId())
      .collect(Collectors.toSet());

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(MULTI_ENV_EXTENSION_2_ON_MULTI_ENV_ENGINE_IDS);
  }

  @Test
  void bothOnJunitJupiter() {
    Set<UniqueId> uniqueTestIds = EngineTestKit.engine(JupiterEngineDescriptor.ENGINE_ID)
      .selectors(selectClass(PlainTest.class))
      .selectors(selectClass(MultiEnvAcceptedTest.class))
      .selectors(selectClass(MultiEnvAcceptedTest.Inner.class))
      .filters(new MultiEnvTestFilter())
      .execute()
      .testEvents()
      .list()
      .stream()
      .map(e -> e.getTestDescriptor().getUniqueId())
      .collect(Collectors.toSet());

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(PLAIN_TEST_ON_JUPITER_ENGINE_IDS);
  }

  @Test
  void bothOnMultiEnv() {
    Set<UniqueId> uniqueTestIds = EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
      .selectors(selectClass(PlainTest.class))
      .selectors(selectClass(MultiEnvAcceptedTest.class))
      .selectors(selectClass(MultiEnvAcceptedTest.Inner.class))
      .filters(new MultiEnvTestFilter())
      .execute()
      .testEvents()
      .list()
      .stream()
      .map(e -> e.getTestDescriptor().getUniqueId())
      .collect(Collectors.toSet());

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(MULTI_ENV_EXTENSION_2_ON_MULTI_ENV_ENGINE_IDS);
  }

  @Test
  void cartesianProduct() {
    Set<UniqueId> uniqueTestIds =
      EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
        .selectors(selectClass(CartesianProductTest.class))
        .selectors(selectClass(CartesianProductTest.Inner.class))
        .filters(new MultiEnvTestFilter())
        .execute()
        .testEvents()
        .list()
        .stream()
        .map(e -> e.getTestDescriptor().getUniqueId())
        .collect(Collectors.toSet());

    List<UniqueId> expectedIds = List.of(
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_1)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_1)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(NestedClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.Inner.class.getSimpleName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_2)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_2)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(NestedClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.Inner.class.getSimpleName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_3)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_1)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_3)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(NestedClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.Inner.class.getSimpleName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_1)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_1)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(NestedClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.Inner.class.getSimpleName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_2)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_2)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(NestedClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.Inner.class.getSimpleName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_3)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(TestExtension1.SEGMENT_TYPE, TestExtension1.SEGMENT_1)
        .append(TestExtension2.SEGMENT_TYPE, TestExtension2.SEGMENT_2)
        .append(TestExtension3.SEGMENT_TYPE, TestExtension3.SEGMENT_3)
        .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.class.getName())
        .append(NestedClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest.Inner.class.getSimpleName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()")
    );

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  /**
   * M before A because M sets higher order value
   * A before Z because alphabetical
   */
  @Test
  void orderedTest() {
    Set<UniqueId> uniqueTestIds = EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
      .selectors(selectClass(OrderedTest.class))
      .selectors(selectClass(OrderedTest.Inner.class))
      .filters(new MultiEnvTestFilter())
      .execute()
      .testEvents()
      .list()
      .stream()
      .map(e -> e.getTestDescriptor().getUniqueId())
      .collect(Collectors.toSet());

    List<UniqueId> expectedIds = List.of(
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(MmmOrderedTestExtension.SEGMENT_TYPE, MmmOrderedTestExtension.SEGMENT_1)
        .append(AaaOrderedTestExtension.SEGMENT_TYPE, AaaOrderedTestExtension.SEGMENT_1)
        .append(ZzzOrderedTestExtension.SEGMENT_TYPE, ZzzOrderedTestExtension.SEGMENT_1)
        .append(ClassTestDescriptor.SEGMENT_TYPE, OrderedTest.class.getName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
      UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
        .append(MmmOrderedTestExtension.SEGMENT_TYPE, MmmOrderedTestExtension.SEGMENT_1)
        .append(AaaOrderedTestExtension.SEGMENT_TYPE, AaaOrderedTestExtension.SEGMENT_1)
        .append(ZzzOrderedTestExtension.SEGMENT_TYPE, ZzzOrderedTestExtension.SEGMENT_1)
        .append(ClassTestDescriptor.SEGMENT_TYPE, OrderedTest.class.getName())
        .append(NestedClassTestDescriptor.SEGMENT_TYPE, OrderedTest.Inner.class.getSimpleName())
        .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()")
    );

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @SuppressWarnings("JUnitMalformedDeclaration") // Intentionally not nested, used above
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

  @ExtendWith(TestExtension2.class)
  @SuppressWarnings("JUnitMalformedDeclaration") // Intentionally not nested, used above
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

  @ExtendWith(TestExtension1.class)
  @ExtendWith(TestExtension2.class)
  @ExtendWith(TestExtension3.class)
  @SuppressWarnings("JUnitMalformedDeclaration") // Intentionally not nested, used above
  public static class CartesianProductTest {
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

  @ExtendWith(AaaOrderedTestExtension.class)
  @ExtendWith(MmmOrderedTestExtension.class)
  @ExtendWith(ZzzOrderedTestExtension.class)
  @SuppressWarnings("JUnitMalformedDeclaration") // Intentionally not nested, used above
  public static class OrderedTest {
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

  public static class TestExtension1 implements MultiEnvTestExtension {
    public static final String SEGMENT_TYPE = "test-segment-1";
    public static final String SEGMENT_1 = "TE1-1";

    @Override
    public String segmentType() {
      return SEGMENT_TYPE;
    }

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }
  }

  public static class TestExtension2 implements MultiEnvTestExtension {
    public static final String SEGMENT_TYPE = "test-segment-2";
    public static final String SEGMENT_1 = "TE2-1";
    public static final String SEGMENT_2 = "TE2-2";

    @Override
    public String segmentType() {
      return SEGMENT_TYPE;
    }

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return Arrays.asList(SEGMENT_1, SEGMENT_2);
    }
  }

  public static class TestExtension3 implements MultiEnvTestExtension {
    public static final String SEGMENT_TYPE = "test-segment-3";
    public static final String SEGMENT_1 = "TE3-1";
    public static final String SEGMENT_2 = "TE3-2";
    public static final String SEGMENT_3 = "TE3-3";


    @Override
    public String segmentType() {
      return SEGMENT_TYPE;
    }

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return Arrays.asList(SEGMENT_1, SEGMENT_2, SEGMENT_3);
    }
  }

  public static class AaaOrderedTestExtension implements MultiEnvTestExtension {
    public static final String SEGMENT_TYPE = "test-segment-a";
    public static final String SEGMENT_1 = "aaa";

    @Override
    public String segmentType() {
      return SEGMENT_TYPE;
    }

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }
  }

  public static class MmmOrderedTestExtension implements MultiEnvTestExtension {
    public static final String SEGMENT_TYPE = "test-segment-m";
    public static final String SEGMENT_1 = "mmm";

    @Override
    public String segmentType() {
      return SEGMENT_TYPE;
    }

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }

    @Override
    public int getOrder() {
      return 1;
    }
  }

  public static class ZzzOrderedTestExtension implements MultiEnvTestExtension {
    public static final String SEGMENT_TYPE = "test-segment-z";
    public static final String SEGMENT_1 = "zzz";

    @Override
    public String segmentType() {
      return SEGMENT_TYPE;
    }

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }
  }
}
