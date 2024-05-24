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
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectUniqueId;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.descriptor.NestedClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.TestMethodTestDescriptor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.testkit.engine.EngineTestKit;

class TestMultiEnvTestEngine {

  public static final String SEGMENT_TYPE_1 = "test-segment-1";
  public static final String SEGMENT_TYPE_2 = "test-segment-2";
  public static final String SEGMENT_TYPE_3 = "test-segment-3";
  public static final String SEGMENT_TYPE_A = "test-segment-a";
  public static final String SEGMENT_TYPE_M = "test-segment-m";
  public static final String SEGMENT_TYPE_Z = "test-segment-z";

  private static final List<UniqueId> PLAIN_TEST_ON_JUPITER_ENGINE_IDS =
      List.of(
          UniqueId.forEngine(JupiterEngineDescriptor.ENGINE_ID)
              .append(ClassTestDescriptor.SEGMENT_TYPE, PlainTest.class.getName())
              .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
          UniqueId.forEngine(JupiterEngineDescriptor.ENGINE_ID)
              .append(ClassTestDescriptor.SEGMENT_TYPE, PlainTest.class.getName())
              .append(NestedClassTestDescriptor.SEGMENT_TYPE, "Inner")
              .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"));

  private static final List<UniqueId> MULTI_ENV_EXTENSION_2_ON_MULTI_ENV_ENGINE_IDS =
      List.of(
          UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
              .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
              .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
              .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
          UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
              .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
              .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
              .append(
                  NestedClassTestDescriptor.SEGMENT_TYPE,
                  MultiEnvAcceptedTest.Inner.class.getSimpleName())
              .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
          UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
              .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
              .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
              .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
          UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
              .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
              .append(ClassTestDescriptor.SEGMENT_TYPE, MultiEnvAcceptedTest.class.getName())
              .append(
                  NestedClassTestDescriptor.SEGMENT_TYPE,
                  MultiEnvAcceptedTest.Inner.class.getSimpleName())
              .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"));

  @Test
  void plainTestOnJunitJupiter() {
    // Validates that the filter permits plain test in the Jupiter test engine
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(JupiterEngineDescriptor.ENGINE_ID)
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
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
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
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(JupiterEngineDescriptor.ENGINE_ID)
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
    Set<UniqueId> uniqueTestIds =
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

    assertThat(uniqueTestIds)
        .containsExactlyInAnyOrderElementsOf(MULTI_ENV_EXTENSION_2_ON_MULTI_ENV_ENGINE_IDS);
  }

  @Test
  void bothOnJunitJupiter() {
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(JupiterEngineDescriptor.ENGINE_ID)
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
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
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

    assertThat(uniqueTestIds)
        .containsExactlyInAnyOrderElementsOf(MULTI_ENV_EXTENSION_2_ON_MULTI_ENV_ENGINE_IDS);
  }

  @Test
  void cartesianProduct() {
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .selectors(selectClass(CartesianProductTest1.class))
            .selectors(selectClass(CartesianProductTest1.Inner.class))
            .selectors(selectClass(CartesianProductTest2.class))
            .selectors(selectClass(CartesianProductTest2.Inner.class))
            .filters(new MultiEnvTestFilter())
            .execute()
            .testEvents()
            .list()
            .stream()
            .map(e -> e.getTestDescriptor().getUniqueId())
            .collect(Collectors.toSet());

    List<UniqueId> expectedIds =
        List.of(
            // CartesianProductTest1
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest1.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest1.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest1.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest1.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest1.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest1.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),

            // CartesianProductTest2
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest2.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_2)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_2)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest2.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_3)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_3)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest2.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest2.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_2)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_2)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest2.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_3)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
                .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
                .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_3)
                .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE,
                    CartesianProductTest2.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"));

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  private static Stream<Arguments> provideUniqueIds() {
    Stream.Builder<Arguments> streamBuilder = Stream.builder();

    for (String segment1Id : List.of(TestExtension1.SEGMENT_1)) {
      for (String segment2Id : List.of(TestExtension2.SEGMENT_1, TestExtension2.SEGMENT_2)) {
        for (String segment3Id :
            List.of(TestExtension3.SEGMENT_1, TestExtension3.SEGMENT_2, TestExtension3.SEGMENT_3)) {
          UniqueId outerClassId =
              UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                  .append(SEGMENT_TYPE_1, segment1Id)
                  .append(SEGMENT_TYPE_2, segment2Id)
                  .append(SEGMENT_TYPE_3, segment3Id)
                  .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName());

          streamBuilder.add(
              Arguments.of(outerClassId.append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()")));

          UniqueId innerClassId =
              outerClassId.append(
                  NestedClassTestDescriptor.SEGMENT_TYPE,
                  CartesianProductTest2.Inner.class.getSimpleName());

          streamBuilder.add(
              Arguments.of(innerClassId.append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()")));
        }
      }
    }

    return streamBuilder.build();
  }

  @ParameterizedTest
  @MethodSource("provideUniqueIds")
  void cartesianProductSelectOneTestOuter(UniqueId uniqueId) {
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .selectors(selectUniqueId(uniqueId))
            .filters(new MultiEnvTestFilter())
            .execute()
            .testEvents()
            .list()
            .stream()
            .map(e -> e.getTestDescriptor().getUniqueId())
            .collect(Collectors.toSet());

    assertThat(uniqueTestIds).containsExactlyInAnyOrder(uniqueId);
  }

  @Test
  void cartesianProductSelectMultipleUnrelatedTests() {
    UniqueId uniqueId_Test1_12 =
        UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
            .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
            .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
            .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest1.class.getName())
            .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()");

    UniqueId uniqueId_Test2_111 =
        UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
            .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
            .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
            .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_1)
            .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
            .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()");

    UniqueId uniqueId_Test2Inner_111 =
        UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
            .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
            .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_1)
            .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_1)
            .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
            .append(
                NestedClassTestDescriptor.SEGMENT_TYPE,
                CartesianProductTest2.Inner.class.getSimpleName())
            .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()");

    UniqueId uniqueId_Test2_123 =
        UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
            .append(SEGMENT_TYPE_1, TestExtension1.SEGMENT_1)
            .append(SEGMENT_TYPE_2, TestExtension2.SEGMENT_2)
            .append(SEGMENT_TYPE_3, TestExtension3.SEGMENT_3)
            .append(ClassTestDescriptor.SEGMENT_TYPE, CartesianProductTest2.class.getName())
            .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()");

    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .selectors(selectUniqueId(uniqueId_Test1_12))
            .selectors(selectUniqueId(uniqueId_Test2_111))
            .selectors(selectUniqueId(uniqueId_Test2Inner_111))
            .selectors(selectUniqueId(uniqueId_Test2_123))
            .filters(new MultiEnvTestFilter())
            .execute()
            .testEvents()
            .list()
            .stream()
            .map(e -> e.getTestDescriptor().getUniqueId())
            .collect(Collectors.toSet());

    assertThat(uniqueTestIds)
        .containsExactlyInAnyOrder(
            uniqueId_Test1_12, uniqueId_Test2_111, uniqueId_Test2Inner_111, uniqueId_Test2_123);
  }

  /**
   * M before A because M sets higher order value.<br>
   * A before Z because orders are equal, falling back to alphabetical.
   */
  @Test
  void orderedTest() {
    Set<UniqueId> uniqueTestIds =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .selectors(selectClass(OrderedTest.class))
            .selectors(selectClass(OrderedTest.Inner.class))
            .filters(new MultiEnvTestFilter())
            .execute()
            .testEvents()
            .list()
            .stream()
            .map(e -> e.getTestDescriptor().getUniqueId())
            .collect(Collectors.toSet());

    List<UniqueId> expectedIds =
        List.of(
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_M, MmmOrderedTestExtension.SEGMENT_1)
                .append(SEGMENT_TYPE_A, AaaOrderedTestExtension.SEGMENT_1)
                .append(SEGMENT_TYPE_Z, ZzzOrderedTestExtension.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, OrderedTest.class.getName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"),
            UniqueId.forEngine(MultiEnvTestEngine.ENGINE_ID)
                .append(SEGMENT_TYPE_M, MmmOrderedTestExtension.SEGMENT_1)
                .append(SEGMENT_TYPE_A, AaaOrderedTestExtension.SEGMENT_1)
                .append(SEGMENT_TYPE_Z, ZzzOrderedTestExtension.SEGMENT_1)
                .append(ClassTestDescriptor.SEGMENT_TYPE, OrderedTest.class.getName())
                .append(
                    NestedClassTestDescriptor.SEGMENT_TYPE, OrderedTest.Inner.class.getSimpleName())
                .append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()"));

    assertThat(uniqueTestIds).containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  /**
   * M before A because M sets higher order value.<br>
   * A before Z because orders are equal, falling back to alphabetical.
   */
  @Test
  void displayNames() {
    Set<String> uniqueTestNames =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .selectors(selectClass(TestDisplayNames.class))
            .selectors(selectClass(TestDisplayNames.Inner.class))
            .filters(new MultiEnvTestFilter())
            .execute()
            .testEvents()
            .list()
            .stream()
            .map(e -> e.getTestDescriptor().getDisplayName())
            .collect(Collectors.toSet());

    // Note that inner/outer tests have the same display names, so there are half as many actually
    // expected display names
    List<String> expectedDisplayNames =
        List.of(
            String.format(
                "test1() [%s,%s,%s]",
                MmmOrderedTestExtension.SEGMENT_1,
                AaaOrderedTestExtension.SEGMENT_1,
                ZzzOrderedTestExtension.SEGMENT_1),
            String.format(
                "test2() [%s,%s,%s]",
                MmmOrderedTestExtension.SEGMENT_1,
                AaaOrderedTestExtension.SEGMENT_1,
                ZzzOrderedTestExtension.SEGMENT_1));

    assertThat(uniqueTestNames).containsExactlyInAnyOrderElementsOf(expectedDisplayNames);
  }

  @SuppressWarnings({"JUnitMalformedDeclaration"}) // Intentionally not nested, used above
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
  @SuppressWarnings({"JUnitMalformedDeclaration"}) // Intentionally not nested, used above
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
  @SuppressWarnings({
    "JUnitMalformedDeclaration",
    "NewClassNamingConvention"
  }) // Intentionally not nested, used above
  public static class CartesianProductTest1 {
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
  @SuppressWarnings({
    "JUnitMalformedDeclaration",
    "NewClassNamingConvention"
  }) // Intentionally not nested, used above
  public static class CartesianProductTest2 {
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

  @ExtendWith(ZzzOrderedTestExtension.class) // Z first to throw off default ordering
  @ExtendWith(AaaOrderedTestExtension.class)
  @ExtendWith(MmmOrderedTestExtension.class)
  @SuppressWarnings({"JUnitMalformedDeclaration"}) // Intentionally not nested, used above
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

  @ExtendWith(ZzzOrderedTestExtension.class) // Z first to throw off default ordering
  @ExtendWith(AaaOrderedTestExtension.class)
  @ExtendWith(MmmOrderedTestExtension.class)
  @SuppressWarnings({"JUnitMalformedDeclaration"}) // Intentionally not nested, used above
  public static class TestDisplayNames {
    @Test
    void test1() {
      // nop
    }

    @Test
    void test2() {
      // nop
    }

    @Nested
    class Inner {
      @Test
      void test1() {
        // nop
      }

      @Test
      void test2() {
        // nop
      }
    }
  }

  @MultiEnvSegmentType(SEGMENT_TYPE_1)
  public static class TestExtension1 implements MultiEnvTestExtension {
    public static final String SEGMENT_1 = "TE1-1";

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }
  }

  @MultiEnvSegmentType(SEGMENT_TYPE_2)
  public static class TestExtension2 implements MultiEnvTestExtension {
    public static final String SEGMENT_1 = "TE2-1";
    public static final String SEGMENT_2 = "TE2-2";

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return Arrays.asList(SEGMENT_1, SEGMENT_2);
    }
  }

  @MultiEnvSegmentType(SEGMENT_TYPE_3)
  public static class TestExtension3 implements MultiEnvTestExtension {
    public static final String SEGMENT_1 = "TE3-1";
    public static final String SEGMENT_2 = "TE3-2";
    public static final String SEGMENT_3 = "TE3-3";

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return Arrays.asList(SEGMENT_1, SEGMENT_2, SEGMENT_3);
    }
  }

  @MultiEnvSegmentType(SEGMENT_TYPE_A)
  public static class AaaOrderedTestExtension implements MultiEnvTestExtension {
    public static final String SEGMENT_1 = "aaa";

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }
  }

  @MultiEnvSegmentType(SEGMENT_TYPE_M)
  public static class MmmOrderedTestExtension implements MultiEnvTestExtension {
    public static final String SEGMENT_1 = "mmm";

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }

    @Override
    public int segmentPriority() {
      return 1;
    }
  }

  @MultiEnvSegmentType(SEGMENT_TYPE_Z)
  public static class ZzzOrderedTestExtension implements MultiEnvTestExtension {
    public static final String SEGMENT_1 = "zzz";

    @Override
    public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
      return List.of(SEGMENT_1);
    }
  }
}
