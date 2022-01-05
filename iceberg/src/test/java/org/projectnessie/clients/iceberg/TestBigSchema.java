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
package org.projectnessie.clients.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.IcebergTable;

@Execution(ExecutionMode.CONCURRENT)
public class TestBigSchema {

  @ParameterizedTest
  @ValueSource(ints = {10, 100, 1000, 10000})
  public void testManyColumns(int numColumns) throws Exception {
    TableMetadata tableMetadata = IcebergTypes.randomTableMetadata(numColumns);
    String jsonIceberg = TableMetadataParser.toJson(tableMetadata);

    // This adds the mandatory (non-null) metadata-location field
    tableMetadata = TableMetadataParser.fromJson(null, "foo://metadata-location", jsonIceberg);
    jsonIceberg = TableMetadataParser.toJson(tableMetadata);

    IcebergTable icebergTableMetadata = NessieIceberg.toNessie(tableMetadata);
    String jsonNessie = JsonUtil.mapper().writeValueAsString(icebergTableMetadata);

    TableMetadata recreatedTableMetadata = NessieIceberg.toIceberg(null, icebergTableMetadata);
    String jsonIcebergRecreated = TableMetadataParser.toJson(recreatedTableMetadata);
    assertThat(jsonIcebergRecreated).isEqualTo(jsonIceberg);

    byte[] compressedIceberg = compress(jsonIceberg);
    byte[] compressedNessie = compress(jsonNessie);

    System.err.printf(
        "  num-cols: %-8d   iceberg-json-size: %10d / %-10d   nessie-json-size: %10d / %-10d%n",
        numColumns,
        jsonIceberg.length(),
        compressedIceberg.length,
        jsonNessie.length(),
        compressedNessie.length);
  }

  private byte[] compress(String jsonIceberg) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(jsonIceberg.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }
}
