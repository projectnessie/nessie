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
package org.projectnessie.catalog.formats.iceberg.rest;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.rest.RESTSerializers;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;

@ExtendWith(SoftAssertionsExtension.class)
public class TestRestTypes {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void stringDeser(String simpleName, String json, Class<?> nessieType, Class<?> icebergType)
      throws Exception {
    soft.assertThat(nessieType).describedAs("No Nessie type for %s", simpleName).isNotNull();

    Object nessie = IcebergJson.objectMapper().readValue(json, nessieType);
    String nessieJson = IcebergJson.objectMapper().writeValueAsString(nessie);

    Object nessieFromNessie = IcebergJson.objectMapper().readValue(nessieJson, nessieType);
    soft.assertThat(nessieFromNessie).isEqualTo(nessie);

    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);

    Object iceberg;
    try {
      iceberg = mapper.readValue(json, icebergType);
    } catch (UnrecognizedPropertyException x) {
      if (simpleName.endsWith("Request")) {
        // Iceberg does not properly deserialize all *Request types
        return;
      }
      throw x;
    }
    String icebergJson = mapper.writeValueAsString(iceberg);

    Object nessieFromIceberg = IcebergJson.objectMapper().readValue(icebergJson, nessieType);
    soft.assertThat(nessieFromIceberg).isEqualTo(nessie);

    Object icebergFromNessie = mapper.readValue(nessieJson, icebergType);
    if (!icebergFromNessie.equals(iceberg)) {
      // Most Iceberg types do not implement hashCode() & equals() :sigh:
      // So we fall back and compare using JsonNode (and pray property orders are consistent)
      JsonNode icebergFromNessieJsonNode = mapper.readValue(nessieJson, JsonNode.class);
      soft.assertThat(icebergFromNessieJsonNode)
          .isEqualTo(mapper.readValue(icebergJson, JsonNode.class));
    }
  }

  // expected:
  // {"metadata-location":"file:/tmp/ns/table/metadata/00000-8da51d9a-49d8-44f0-b452-691f50616652.metadata.json","metadata":{"format-version":2,"table-uuid":"6fa50a92-20d0-44ee-b1bb-b69b574f89ee","location":"file:/tmp/ns/table","last-sequence-number":0,"last-updated-ms":1709720862886,"last-column-id":2,"current-schema-id":0,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"int","doc":"unique ID 🤪"},{"id":2,"name":"data","required":true,"type":"string"}]}],"default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[{"name":"id_bucket","transform":"bucket[16]","source-id":1,"field-id":1000}]}],"last-partition-id":1000,"default-sort-order-id":1,"sort-orders":[{"order-id":1,"fields":[{"transform":"bucket[16]","source-id":1,"direction":"asc","null-order":"nulls-first"},{"transform":"identity","source-id":1,"direction":"asc","null-order":"nulls-first"}]}],"properties":{"created-at":"2022-02-25T00:38:19","user":"someone","write.parquet.compression-codec":"zstd"},"current-snapshot-id":-1,"refs":{},"snapshots":[],"statistics":[],"snapshot-log":[],"metadata-log":[]},"config":{}}
  // but was:
  // {"metadata-location":"file:/tmp/ns/table/metadata/00000-8da51d9a-49d8-44f0-b452-691f50616652.metadata.json","metadata":{"format-version":2,"table-uuid":"6fa50a92-20d0-44ee-b1bb-b69b574f89ee","location":"file:/tmp/ns/table","last-sequence-number":0,"last-updated-ms":1709720862886,"last-column-id":2,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"int","doc":"unique ID 🤪"},{"id":2,"name":"data","required":true,"type":"string"}]}],"current-schema-id":0,"partition-specs":[{"spec-id":0,"fields":[{"name":"id_bucket","transform":"bucket[16]","source-id":1,"field-id":1000}]}],"default-spec-id":0,"last-partition-id":1000,"default-sort-order-id":1,"sort-orders":[{"order-id":1,"fields":[{"transform":"bucket[16]","source-id":1,"direction":"asc","null-order":"nulls-first"},{"transform":"identity","source-id":1,"direction":"asc","null-order":"nulls-first"}]}],"properties":{"created-at":"2022-02-25T00:38:19","user":"someone","write.parquet.compression-codec":"zstd"},"current-snapshot-id":-1,"snapshots":[],"statistics":[],"snapshot-log":[],"metadata-log":[]},"config":{}}

  static Stream<Arguments> stringDeser() throws Exception {
    URL dataUrl = TestRestTypes.class.getResource("rest-objects.txt");
    Stream<String> input = Files.lines(Paths.get(dataUrl.toURI()));
    List<String> lines;
    try {
      lines = input.collect(Collectors.toList());
    } finally {
      input.close();
    }
    return lines.stream()
        .map(
            ln -> {
              int i = ln.indexOf(": ");
              String clazzSimpleName = ln.substring(0, i);
              String json = ln.substring(i + 2);
              String nessieSimpleName;
              switch (clazzSimpleName) {
                case "ReportMetricsRequest":
                  nessieSimpleName = "MetricsReport";
                  break;
                default:
                  nessieSimpleName = clazzSimpleName;
                  break;
              }
              Class<?> nessieType =
                  findClass(
                      nessieSimpleName,
                      "Iceberg",
                      "org.projectnessie.catalog.formats.iceberg.rest",
                      "org.projectnessie.catalog.formats.iceberg.meta",
                      "org.projectnessie.catalog.formats.iceberg.metrics");
              Class<?> icebergType =
                  findClass(
                      clazzSimpleName,
                      "",
                      "org.apache.iceberg.rest.requests",
                      "org.apache.iceberg.rest.responses");
              return arguments(clazzSimpleName, json, nessieType, icebergType);
            });
  }

  static Class<?> findClass(String simpleName, String prefix, String... packageNames) {
    return Arrays.stream(packageNames)
        .map(pkg -> pkg + "." + prefix + simpleName)
        .map(TestRestTypes::tryLoadClass)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  static Class<?> tryLoadClass(String name) {
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
