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
package org.projectnessie.clients.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.apache.iceberg.view.ViewVersionMetadataParser;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;

@Execution(ExecutionMode.CONCURRENT)
public class TestNessieIceberg {

  static Stream<String> randomTableMetadata() {
    return IntStream.range(0, 100)
        .mapToObj(i -> IcebergTypes.randomTableMetadata(13))
        .map(TableMetadataParser::toJson);
  }

  @ParameterizedTest
  @MethodSource("randomTableMetadata")
  public void tableMetadata(String tableMetadataJson) {
    TableMetadata tm =
        TableMetadataParser.fromJson(null, "foo://metadata-location", tableMetadataJson);
    assertThat(TableMetadataParser.toJson(tm)).isEqualTo(tableMetadataJson);

    IcebergTable nessie = NessieIceberg.toNessie(tm);
    TableMetadata fromNessie = NessieIceberg.toIceberg(null, nessie);
    assertThat(TableMetadataParser.toJson(fromNessie)).isEqualTo(tableMetadataJson);
    IcebergTable nessie2 = NessieIceberg.toNessie(fromNessie);
    nessie2 = IcebergTable.builder().from(nessie2).id(nessie.getId()).build();

    assertThat(nessie2).isEqualTo(nessie);
  }

  static Stream<String> randomViewMetadata() {
    return IntStream.range(0, 100)
        .mapToObj(i -> IcebergTypes.randomViewMetadata(13))
        .map(ViewVersionMetadataParser::toJson);
  }

  @ParameterizedTest
  @MethodSource("randomViewMetadata")
  public void viewMetadata(String viewMetadataJson) {
    ViewVersionMetadata tm = ViewVersionMetadataParser.fromJson(viewMetadataJson);
    assertThat(ViewVersionMetadataParser.toJson(tm)).isEqualTo(viewMetadataJson);

    IcebergView nessie = NessieIceberg.toNessie(tm);
    ViewVersionMetadata fromNessie = NessieIceberg.toIceberg(nessie);
    assertThat(ViewVersionMetadataParser.toJson(fromNessie)).isEqualTo(viewMetadataJson);
    IcebergView nessie2 = NessieIceberg.toNessie(fromNessie);
    nessie2 = IcebergView.builder().from(nessie2).id(nessie.getId()).build();

    assertThat(nessie2).isEqualTo(nessie);
  }
}
