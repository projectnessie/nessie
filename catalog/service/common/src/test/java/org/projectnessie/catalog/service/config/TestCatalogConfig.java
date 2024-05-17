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
package org.projectnessie.catalog.service.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCatalogConfig {
  @ParameterizedTest
  @MethodSource
  public void lookupWarehouseFail(CatalogConfig catalogConfig, String lookup) {
    if (lookup != null && !lookup.isEmpty()) {
      assertThatIllegalStateException()
          .isThrownBy(() -> catalogConfig.getWarehouse(lookup))
          .withMessage(
              "Warehouse '" + lookup + "' is not defined and no default-warehouse is configured");
    } else {
      assertThatIllegalStateException()
          .isThrownBy(() -> catalogConfig.getWarehouse(lookup))
          .withMessage("No default-warehouse configured");
    }
  }

  @ParameterizedTest
  @MethodSource
  public void lookupWarehouse(
      CatalogConfig catalogConfig, String lookup, WarehouseConfig expected) {
    assertThat(catalogConfig.getWarehouse(lookup)).isEqualTo(expected);
  }

  static Stream<Arguments> lookupWarehouseFail() {
    WarehouseConfigForTest w1 =
        ImmutableWarehouseConfigForTest.builder().name("w1").location("s3://blah/blah").build();
    WarehouseConfigForTest w2 =
        ImmutableWarehouseConfigForTest.builder().name("w2").location("s3://blah/blah").build();
    WarehouseConfigForTest w3 =
        ImmutableWarehouseConfigForTest.builder().name("w3").location("gcs://blah/blah").build();

    CatalogConfig cfgWithoutDefault =
        ImmutableCatalogConfigForTest.builder()
            .putWarehouses(w1.name(), w1)
            .putWarehouses(w2.name(), w2)
            .putWarehouses(w3.name(), w3)
            .build();

    return Stream.of(
        arguments(cfgWithoutDefault, null),
        arguments(cfgWithoutDefault, ""),
        arguments(cfgWithoutDefault, "foo"),
        arguments(cfgWithoutDefault, w1.name() + "foo"),
        arguments(cfgWithoutDefault, w1.location() + "blah"),
        arguments(cfgWithoutDefault, w1.location() + "/blah"));
  }

  static Stream<Arguments> lookupWarehouse() {
    WarehouseConfigForTest w1 =
        ImmutableWarehouseConfigForTest.builder().name("w1").location("s3://blah/blah").build();
    WarehouseConfigForTest w2 =
        ImmutableWarehouseConfigForTest.builder().name("w2").location("s3://blah/blah").build();
    WarehouseConfigForTest w3 =
        ImmutableWarehouseConfigForTest.builder().name("w3").location("gcs://blah/blah").build();

    CatalogConfig cfgWithoutDefault =
        ImmutableCatalogConfigForTest.builder()
            .putWarehouses(w1.name(), w1)
            .putWarehouses(w2.name(), w2)
            .putWarehouses(w3.name(), w3)
            .build();

    CatalogConfig cfgWithDefault1 =
        ImmutableCatalogConfigForTest.builder()
            .putWarehouses(w1.name(), w1)
            .putWarehouses(w3.name(), w3)
            .defaultWarehouse(w2)
            .build();

    CatalogConfig cfgWithDefault2 =
        ImmutableCatalogConfigForTest.builder()
            .putWarehouses(w3.name(), w3)
            .defaultWarehouse(w2)
            .build();

    return Stream.of(
        arguments(cfgWithoutDefault, w1.name(), w1),
        arguments(cfgWithoutDefault, w1.location(), w1),
        arguments(cfgWithoutDefault, w1.location() + "/", w1),
        // Actually, the behavior when looking up a warehouse by a location used by multiple
        // warehouses is undefined, but for this test we can (probably) rely on Immutables.
        arguments(cfgWithoutDefault, w2.location(), w1),
        arguments(cfgWithoutDefault, w2.location() + "/", w1),
        arguments(cfgWithoutDefault, w3.name(), w3),
        arguments(cfgWithoutDefault, w3.location(), w3),
        arguments(cfgWithoutDefault, w3.location() + "/", w3),
        //
        arguments(cfgWithDefault1, w1.name(), w1),
        arguments(cfgWithDefault1, w1.location(), w1),
        arguments(cfgWithDefault1, w1.location() + "/", w1),
        arguments(cfgWithDefault1, w2.location(), w1),
        arguments(cfgWithDefault1, w2.location() + "/", w1),
        arguments(cfgWithDefault1, w3.name(), w3),
        arguments(cfgWithDefault1, w3.location(), w3),
        arguments(cfgWithDefault1, w3.location() + "/", w3),
        arguments(cfgWithDefault1, "fooooo", w2),
        //
        arguments(cfgWithDefault2, w1.name(), w2),
        arguments(cfgWithDefault2, w1.location(), w2),
        arguments(cfgWithDefault2, w1.location() + "/", w2),
        arguments(cfgWithDefault2, w2.location(), w2),
        arguments(cfgWithDefault2, w2.location() + "/", w2),
        arguments(cfgWithDefault2, w3.name(), w3),
        arguments(cfgWithDefault2, w3.location(), w3),
        arguments(cfgWithDefault2, w3.location() + "/", w3),
        arguments(cfgWithDefault2, "fooooo", w2)
        //
        );
  }

  @Value.Immutable
  @SuppressWarnings("immutables:from")
  interface CatalogConfigForTest extends CatalogConfig {
    @Override
    Optional<WarehouseConfigForTest> defaultWarehouse();

    @Override
    Map<String, WarehouseConfigForTest> warehouses();
  }

  @Value.Immutable
  interface WarehouseConfigForTest extends WarehouseConfig {}
}
