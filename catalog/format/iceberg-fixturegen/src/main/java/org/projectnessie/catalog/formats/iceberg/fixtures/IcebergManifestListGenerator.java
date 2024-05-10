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
import java.util.function.Function;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriterSpec;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Used to generate an Iceberg manifest-list based on {@link IcebergManifestFileGenerator} and
 * {@link IcebergSchemaGenerator}, number of referenced manifest-files and some Avro parameters are
 * configurable.
 */
@NessieImmutable
public abstract class IcebergManifestListGenerator {
  public abstract IcebergSchemaGenerator generator();

  public abstract int manifestFileCount();

  public abstract String basePath();

  public abstract Function<String, OutputStream> output();

  @Value.Default
  public long sequenceNumber() {
    return 1;
  }

  @Value.Default
  public long snapshotId() {
    return 1;
  }

  @Value.Default
  public UUID commitId() {
    return UUID.randomUUID();
  }

  @Value.Default
  public int attempt() {
    return 1;
  }

  @Value.Default
  public String avroCompression() {
    return "gzip";
  }

  public static ImmutableIcebergManifestListGenerator.Builder builder() {
    return ImmutableIcebergManifestListGenerator.builder();
  }

  public String generate(Supplier<IcebergManifestFile> manifestFileSupplier) throws Exception {

    String listFile =
        String.format(
            "snap-%d-%d-%s%s",
            snapshotId(), attempt(), commitId(), IcebergFileFormat.AVRO.fileExtension());
    String listPath = basePath() + listFile;

    IcebergManifestListWriterSpec writerSpec =
        IcebergManifestListWriterSpec.builder()
            .sequenceNumber(sequenceNumber())
            .partitionSpec(generator().getIcebergPartitionSpec())
            .schema(generator().getIcebergSchema())
            .snapshotId(snapshotId())
            .spec(IcebergSpec.V2)
            .putTableProperty(AVRO_COMPRESSION, avroCompression())
            .build();

    try (OutputStream listOutput = output().apply(listPath);
        IcebergManifestListWriter listEntryWriter =
            IcebergManifestListWriter.openManifestListWriter(writerSpec, listOutput)) {
      for (int i = 0; i < manifestFileCount(); i++) {
        listEntryWriter.append(manifestFileSupplier.get());
      }
    }

    return listPath;
  }
}
