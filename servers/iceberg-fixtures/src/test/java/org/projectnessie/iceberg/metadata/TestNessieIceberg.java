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
package org.projectnessie.iceberg.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.iceberg.metadata.NessieIceberg.asJsonNode;
import static org.projectnessie.iceberg.metadata.NessieIceberg.toIceberg;
import static org.projectnessie.iceberg.metadata.NessieIceberg.toNessie;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.view.IcebergBridge;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestNessieIceberg {

  private static Random random;

  @BeforeAll
  public static void initRandom() {
    random = new Random(42L);
  }

  static Stream<String> randomTableMetadata() {
    return IntStream.range(0, 100)
        .mapToObj(i -> NessieIceberg.randomTableMetadata(random, 5))
        .map(TableMetadataParser::toJson);
  }

  @ParameterizedTest
  @MethodSource("randomTableMetadata")
  public void tableMetadata(String tableMetadataJson) {
    TableMetadata tm =
        TableMetadataParser.fromJson(null, "foo://metadata-location", tableMetadataJson);
    assertThat(asJsonNode(TableMetadataParser.toJson(tm))).isEqualTo(asJsonNode(tableMetadataJson));

    JsonNode nessie = toNessie(tm);
    TableMetadata fromNessie = toIceberg(null, nessie);
    assertThat(asJsonNode(TableMetadataParser.toJson(fromNessie)))
        .isEqualTo(asJsonNode(tableMetadataJson));
    JsonNode nessie2 = toNessie(fromNessie);

    assertThat(nessie2).isEqualTo(nessie);
  }

  static Stream<String> randomViewMetadata() {
    return IntStream.range(0, 100)
        .mapToObj(i -> NessieIceberg.randomViewMetadata(random, 5))
        .map(IcebergBridge::viewVersionMetadataToJson);
  }

  @ParameterizedTest
  @MethodSource("randomViewMetadata")
  public void viewMetadata(String viewMetadataJson) {
    ViewVersionMetadata tm =
        IcebergBridge.parseJsonAsViewVersionMetadata(asJsonNode(viewMetadataJson));
    assertThat(asJsonNode(IcebergBridge.viewVersionMetadataToJson(tm)))
        .isEqualTo(asJsonNode(viewMetadataJson));

    JsonNode nessie = toNessie(tm);
    ViewVersionMetadata fromNessie = toIceberg(nessie);
    assertThat(asJsonNode(IcebergBridge.viewVersionMetadataToJson(fromNessie)))
        .isEqualTo(asJsonNode(viewMetadataJson));
    JsonNode nessie2 = toNessie(fromNessie);

    assertThat(nessie2).isEqualTo(nessie);
  }
}
