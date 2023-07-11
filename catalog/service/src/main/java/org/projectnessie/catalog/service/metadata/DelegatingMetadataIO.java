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
package org.projectnessie.catalog.service.metadata;

import static org.projectnessie.catalog.service.util.Json.jsonToTableMetadata;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.LocationUtil;
import org.projectnessie.catalog.service.util.Json;

public class DelegatingMetadataIO implements MetadataIO {

  private static final String METADATA_FOLDER_NAME = "metadata";

  private final FileIO fileIO;

  public DelegatingMetadataIO(FileIO fileIO) {
    this.fileIO = fileIO;
  }

  @Override
  public TableMetadata load(String metadataLocation) {
    return TableMetadataParser.read(fileIO, metadataLocation);
  }

  @Override
  public TableMetadata store(TableMetadata metadata, int newVersion) {
    String metadataLocation = writeNewMetadata(metadata, newVersion);

    JsonNode root = Json.tableMetadataToJsonNode(metadata);

    return jsonToTableMetadata(metadataLocation, root);
  }

  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    String newTableMetadataLocation = newTableMetadataFilePath(metadata, newVersion);
    OutputFile metadataOutput = fileIO.newOutputFile(newTableMetadataLocation);

    // write the new metadata
    // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
    // always unique because it includes a UUID.
    TableMetadataParser.overwrite(metadata, metadataOutput);

    return newTableMetadataLocation;
  }

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName =
        meta.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(
        meta, String.format("%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  private String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }
  }
}
