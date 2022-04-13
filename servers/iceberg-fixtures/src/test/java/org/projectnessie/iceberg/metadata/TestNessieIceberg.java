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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.view.IcebergBride;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Execution(ExecutionMode.CONCURRENT)
public class TestNessieIceberg {

  static Stream<String> randomTableMetadata() {
    return IntStream.range(0, 100)
        .mapToObj(i -> NessieIceberg.randomTableMetadata(5))
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
        .mapToObj(i -> NessieIceberg.randomViewMetadata(5))
        .map(IcebergBride::viewVersionMetadataToJson);
  }

  @ParameterizedTest
  @MethodSource("randomViewMetadata")
  public void viewMetadata(String viewMetadataJson) {
    ViewVersionMetadata tm =
        IcebergBride.parseJsonAsViewVersionMetadata(asJsonNode(viewMetadataJson));
    assertThat(asJsonNode(IcebergBride.viewVersionMetadataToJson(tm)))
        .isEqualTo(asJsonNode(viewMetadataJson));

    JsonNode nessie = toNessie(tm);
    ViewVersionMetadata fromNessie = toIceberg(nessie);
    assertThat(asJsonNode(IcebergBride.viewVersionMetadataToJson(fromNessie)))
        .isEqualTo(asJsonNode(viewMetadataJson));
    JsonNode nessie2 = toNessie(fromNessie);

    assertThat(nessie2).isEqualTo(nessie);
  }
}
