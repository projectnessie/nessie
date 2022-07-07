/*
 * Copyright (C) 2020 Dremio
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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Random;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Execution(ExecutionMode.CONCURRENT)
public class TestBigSchema {

  private Random random;

  @BeforeEach
  public void initRandom() {
    random = new Random(42L);
  }

  @ParameterizedTest
  @ValueSource(ints = {10, 100, 1000, 10000})
  public void testManyColumns(int numColumns) {
    TableMetadata tableMetadata = NessieIceberg.randomTableMetadata(random, numColumns);
    String jsonIceberg = TableMetadataParser.toJson(tableMetadata);

    // This adds the mandatory (non-null) metadata-location field
    tableMetadata = TableMetadataParser.fromJson(null, "foo://metadata-location", jsonIceberg);
    jsonIceberg = TableMetadataParser.toJson(tableMetadata);

    JsonNode icebergTableMetadata = NessieIceberg.toNessie(tableMetadata);

    TableMetadata recreatedTableMetadata = NessieIceberg.toIceberg(null, icebergTableMetadata);
    String jsonIcebergRecreated = TableMetadataParser.toJson(recreatedTableMetadata);
    assertThat(jsonIcebergRecreated).isEqualTo(jsonIceberg);
  }
}
