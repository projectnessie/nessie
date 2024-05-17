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
package org.projectnessie.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableMap;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestQuarkusCatalogConfig {
  @ParameterizedTest
  @MethodSource
  public void lookupWarehouse(
      Map<String, String> configs, String lookup, String expectedWarehouseName) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(QuarkusCatalogConfig.class)
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    QuarkusCatalogConfig catalogConfig = config.getConfigMapping(QuarkusCatalogConfig.class);

    assertThat(catalogConfig.getWarehouse(lookup))
        .matches(c -> c.name().equals(expectedWarehouseName));
  }

  static Stream<Arguments> lookupWarehouse() {
    String loc12 = "s3://blah/blah";
    String loc3 = "gcs://blah/blah";

    Map<String, String> cfgWithoutDefault =
        ImmutableMap.<String, String>builder()
            .put("nessie.catalog.warehouses.w1.name", "w1")
            .put("nessie.catalog.warehouses.w1.location", loc12)
            .put("nessie.catalog.warehouses.w2.name", "w2")
            .put("nessie.catalog.warehouses.w2.location", loc12)
            .put("nessie.catalog.warehouses.w3.name", "w3")
            .put("nessie.catalog.warehouses.w3.location", loc3)
            .build();

    Map<String, String> cfgWithTrailingSlash =
        ImmutableMap.<String, String>builder()
            .put("nessie.catalog.warehouses.w1.name", "w1")
            .put("nessie.catalog.warehouses.w1.location", loc12 + "/")
            .put("nessie.catalog.warehouses.w2.name", "w2")
            .put("nessie.catalog.warehouses.w2.location", loc12 + "/")
            .put("nessie.catalog.warehouses.w3.name", "w3")
            .put("nessie.catalog.warehouses.w3.location", loc3 + "/")
            .build();

    Map<String, String> cfgWithDefault1 =
        ImmutableMap.<String, String>builder()
            .put("nessie.catalog.warehouses.w1.name", "w1")
            .put("nessie.catalog.warehouses.w1.location", loc12)
            .put("nessie.catalog.warehouses.w3.name", "w3")
            .put("nessie.catalog.warehouses.w3.location", loc3)
            .put("nessie.catalog.default-warehouse.name", "w2")
            .put("nessie.catalog.default-warehouse.location", loc12)
            .build();

    Map<String, String> cfgWithDefault2 =
        ImmutableMap.<String, String>builder()
            .put("nessie.catalog.warehouses.w3.name", "w3")
            .put("nessie.catalog.warehouses.w3.location", loc3)
            .put("nessie.catalog.default-warehouse.name", "w2")
            .put("nessie.catalog.default-warehouse.location", loc12)
            .build();

    return Stream.of(
        arguments(cfgWithoutDefault, "w1", "w1"),
        arguments(cfgWithoutDefault, loc12, "w1"),
        arguments(cfgWithoutDefault, loc12 + "/", "w1"),
        // Actually, the behavior when looking up a warehouse by a location used by multiple
        // warehouses is undefined, but for this test we can (probably) rely on Immutables.
        arguments(cfgWithoutDefault, loc12, "w1"),
        arguments(cfgWithoutDefault, loc12 + "/", "w1"),
        arguments(cfgWithoutDefault, "w3", "w3"),
        arguments(cfgWithoutDefault, loc3, "w3"),
        arguments(cfgWithoutDefault, loc3 + "/", "w3"),
        //
        arguments(cfgWithTrailingSlash, "w1", "w1"),
        arguments(cfgWithTrailingSlash, loc12, "w1"),
        arguments(cfgWithTrailingSlash, loc12 + "/", "w1"),
        // Actually, the behavior when looking up a warehouse by a location used by multiple
        // warehouses is undefined, but for this test we can (probably) rely on Immutables.
        arguments(cfgWithTrailingSlash, loc12, "w1"),
        arguments(cfgWithTrailingSlash, loc12 + "/", "w1"),
        arguments(cfgWithTrailingSlash, "w3", "w3"),
        arguments(cfgWithTrailingSlash, loc3, "w3"),
        arguments(cfgWithTrailingSlash, loc3 + "/", "w3"),
        //
        arguments(cfgWithDefault1, "w1", "w1"),
        arguments(cfgWithDefault1, loc12, "w1"),
        arguments(cfgWithDefault1, loc12 + "/", "w1"),
        arguments(cfgWithDefault1, loc12, "w1"),
        arguments(cfgWithDefault1, loc12 + "/", "w1"),
        arguments(cfgWithDefault1, "w3", "w3"),
        arguments(cfgWithDefault1, loc3, "w3"),
        arguments(cfgWithDefault1, loc3 + "/", "w3"),
        arguments(cfgWithDefault1, "fooooo", "w2"),
        //
        arguments(cfgWithDefault2, "w1", "w2"),
        arguments(cfgWithDefault2, loc12, "w2"),
        arguments(cfgWithDefault2, loc12 + "/", "w2"),
        arguments(cfgWithDefault2, loc12, "w2"),
        arguments(cfgWithDefault2, loc12 + "/", "w2"),
        arguments(cfgWithDefault2, "w3", "w3"),
        arguments(cfgWithDefault2, loc3, "w3"),
        arguments(cfgWithDefault2, loc3 + "/", "w3"),
        arguments(cfgWithDefault2, "fooooo", "w2")
        //
        );
  }
}
