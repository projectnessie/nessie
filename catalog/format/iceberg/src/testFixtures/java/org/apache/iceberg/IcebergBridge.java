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
package org.apache.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

public final class IcebergBridge {
  private IcebergBridge() {}

  public static org.apache.iceberg.Schema manifestEntrySchema(
      int version, Types.StructType partitionType) {
    switch (version) {
      case 1:
        return manifestEntrySchemaV1(partitionType);
      case 2:
        return manifestEntrySchemaV2(partitionType);
      default:
        throw new IllegalArgumentException("version " + version);
    }
  }

  public static org.apache.iceberg.Schema manifestEntrySchemaV1(Types.StructType partitionType) {
    return V1Metadata.entrySchema(partitionType);
  }

  public static Schema manifestEntrySchemaV2(Types.StructType partitionType) {
    return V2Metadata.entrySchema(partitionType);
  }

  public static CloseableIterable<ManifestFile> loadManifestList(InputFile file) {
    return Avro.read(file)
        .rename("manifest_file", GenericManifestFile.class.getName())
        .rename("partitions", GenericPartitionFieldSummary.class.getName())
        .rename("r508", GenericPartitionFieldSummary.class.getName())
        .classLoader(GenericManifestFile.class.getClassLoader())
        .project(ManifestFile.schema())
        .reuseContainers(false)
        .build();
  }

  public static ManifestListWriterBridge writeManifestList(
      int formatVersion,
      OutputFile manifestListFile,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber) {
    return new ManifestListWriterBridge(
        ManifestLists.write(
            formatVersion, manifestListFile, snapshotId, parentSnapshotId, sequenceNumber, null));
  }

  public static class ManifestListWriterBridge implements Closeable {
    final ManifestListWriter listWriter;

    ManifestListWriterBridge(ManifestListWriter listWriter) {
      this.listWriter = listWriter;
    }

    public void add(ManifestFile manifest) {
      listWriter.add(manifest);
    }

    @Override
    public void close() throws IOException {
      listWriter.close();
    }

    public long length() {
      return listWriter.length();
    }

    public List<Long> splitOffsets() {
      return listWriter.splitOffsets();
    }
  }
}
