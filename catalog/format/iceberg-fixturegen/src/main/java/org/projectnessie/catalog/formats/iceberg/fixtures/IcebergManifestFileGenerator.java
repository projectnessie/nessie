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
package org.projectnessie.catalog.formats.iceberg.fixtures;

import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.AVRO_COMPRESSION;

import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericData;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntryStatus;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriterSpec;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Used to generate an Iceberg manifest-file based {@link IcebergSchemaGenerator}, number of files
 * in the manifest-file and some Avro parameters are configurable.
 */
@NessieImmutable
public abstract class IcebergManifestFileGenerator {

  public abstract IcebergSchemaGenerator generator();

  public abstract int addDataFiles();

  public abstract String basePath();

  public abstract Function<String, OutputStream> output();

  @Value.Default
  public long sequenceNumber() {
    return 1;
  }

  @Value.Default
  public long fileSequenceNumber() {
    return 1;
  }

  @Value.Default
  public long minSequenceNumber() {
    return 1;
  }

  @Value.Default
  public long addedSnapshotId() {
    return 1;
  }

  @Value.Default
  public String avroCompression() {
    return "gzip";
  }

  public static ImmutableIcebergManifestFileGenerator.Builder builder() {
    return ImmutableIcebergManifestFileGenerator.builder();
  }

  public String randomManifestFileName(UUID commitId, int seq) {
    return String.format("%s-m%08d%s", commitId, seq, IcebergFileFormat.AVRO.fileExtension());
  }

  public Supplier<IcebergManifestFile> createSupplier(UUID commitId) {
    AtomicInteger seq = new AtomicInteger();
    return () -> {
      String filePath = basePath() + randomManifestFileName(commitId, seq.incrementAndGet());
      try {
        return generate(filePath);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  public IcebergManifestFile generate(String manifestPath) throws Exception {
    IcebergManifestFileWriterSpec writerSpec =
        IcebergManifestFileWriterSpec.builder()
            .content(IcebergManifestContent.DATA)
            .sequenceNumber(sequenceNumber())
            .minSequenceNumber(minSequenceNumber())
            .addedSnapshotId(addedSnapshotId())
            .partitionSpec(generator().getIcebergPartitionSpec())
            .schema(generator().getIcebergSchema())
            .spec(IcebergSpec.V2)
            .putTableProperty(AVRO_COMPRESSION, avroCompression())
            .closeOutput(true)
            .manifestPath(manifestPath)
            .build();
    ThreadLocalRandom rand = ThreadLocalRandom.current();

    try (IcebergManifestFileWriter entryWriter =
        IcebergManifestFileWriter.openManifestFileWriter(
            writerSpec, output().apply(manifestPath))) {

      for (int i = 0; i < addDataFiles(); i++) {
        GenericData.Record partition = generator().generatePartitionRecord();

        IcebergDataFile.Builder dataFile =
            IcebergDataFile.builder()
                .content(IcebergDataContent.DATA)
                .recordCount(42)
                .partition(partition)
                .fileFormat(IcebergFileFormat.PARQUET)
                .filePath("/some/file/path/" + rand.nextLong(0, Long.MAX_VALUE))
                .fileSizeInBytes(rand.nextLong(0, Long.MAX_VALUE));

        generator().generateColumns(dataFile::putLowerBound, dataFile::putUpperBound);

        IcebergManifestEntryStatus status = IcebergManifestEntryStatus.ADDED;
        entryWriter.append(
            dataFile.build(), status, fileSequenceNumber(), sequenceNumber(), addedSnapshotId());
      }

      return entryWriter.finish();
    }
  }
}
