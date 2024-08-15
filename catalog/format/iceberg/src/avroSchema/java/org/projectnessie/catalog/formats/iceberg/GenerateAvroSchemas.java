/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.formats.iceberg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.avro.SchemaFormatter;
import org.apache.iceberg.IcebergSchemas;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;

/** Generates Avro schema JSON representation for required Iceberg structures. */
public class GenerateAvroSchemas {
  public static void main(String[] args) throws IOException {
    Path targetDir = Paths.get(args[0]);

    Path dirV1 = targetDir.resolve("v1");
    Path dirV2 = targetDir.resolve("v2");

    Files.createDirectories(dirV1);
    Files.createDirectories(dirV2);

    // V1

    GenerateAvroSchemas generator = new GenerateAvroSchemas(dirV1);

    org.apache.avro.Schema manifestFileV1 =
        generator.generateAvroSchema("manifest_file", IcebergSchemas.manifestFileSchemaV1());

    generator.generateAvroSchema(
        "partition_summary",
        manifestFileV1.getField("partitions").schema().getTypes().get(1).getElementType());

    org.apache.avro.Schema manifestEntryV1 =
        generator.generateAvroSchema("manifest_entry", IcebergSchemas.manifestEntrySchemaV1());

    generator.generateAvroSchema(
        "manifest_entry_data_file", manifestEntryV1.getField("data_file").schema());

    // V2

    generator = new GenerateAvroSchemas(dirV2);

    org.apache.avro.Schema manifestFileV2 =
        generator.generateAvroSchema("manifest_file", IcebergSchemas.manifestFileSchemaV2());

    generator.generateAvroSchema(
        "partition_summary",
        manifestFileV2.getField("partitions").schema().getTypes().get(1).getElementType());

    org.apache.avro.Schema manifestEntryV2 =
        generator.generateAvroSchema("manifest_entry", IcebergSchemas.manifestEntrySchemaV2());

    generator.generateAvroSchema(
        "manifest_entry_data_file", manifestEntryV2.getField("data_file").schema());
  }

  private final Path targetDir;

  GenerateAvroSchemas(Path targetDir) {
    this.targetDir = targetDir;
  }

  org.apache.avro.Schema generateAvroSchema(String name, Schema icebergSchema) throws IOException {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, name);
    return generateAvroSchema(name, avroSchema);
  }

  private org.apache.avro.Schema generateAvroSchema(String name, org.apache.avro.Schema avroSchema)
      throws IOException {
    String schemaJson = SchemaFormatter.getInstance("json/pretty").format(avroSchema);

    Files.write(
        targetDir.resolve(name + ".avro.json"), schemaJson.getBytes(StandardCharsets.UTF_8));

    org.apache.avro.Schema.Parser schemaParser =
        new org.apache.avro.Schema.Parser().setValidateDefaults(true);
    return schemaParser.parse(schemaJson);
  }
}
