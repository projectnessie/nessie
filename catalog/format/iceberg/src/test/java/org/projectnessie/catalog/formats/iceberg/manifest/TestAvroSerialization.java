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
package org.projectnessie.catalog.formats.iceberg.manifest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat.AVRO;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat.PARQUET;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent.DATA;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent.DELETES;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntryStatus.ADDED;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary.icebergPartitionFieldSummary;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.partitionSpec;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.IcebergBridge;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.GenManifestFile;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.LocalFileIO;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;

@ExtendWith(SoftAssertionsExtension.class)
public class TestAvroSerialization {

  @InjectSoftAssertions protected SoftAssertions soft;

  @TempDir private Path tempDir;

  @ParameterizedTest
  @MethodSource
  public void manifestLists(
      @SuppressWarnings("unused") String description,
      IcebergSpec spec,
      IcebergSchema schema,
      IcebergPartitionSpec partitionSpec,
      long snapshotId,
      long parentSnapshotId,
      long sequenceNumber,
      List<IcebergManifestFile> icebergManifestFiles)
      throws Exception {

    byte[] manifestListData;
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      IcebergManifestListWriterSpec writerSpec =
          IcebergManifestListWriterSpec.builder()
              .spec(spec)
              .snapshotId(snapshotId)
              .parentSnapshotId(parentSnapshotId)
              .sequenceNumber(sequenceNumber)
              .schema(schema)
              .partitionSpec(partitionSpec)
              .build();

      Schema writerSchema = writerSpec.writerSchema();

      // "Derived" `IcebergManifestFileWriterSpec` instances must yield the same Avro `Schema`
      // instance
      IcebergManifestListWriterSpec writerSpec2 =
          IcebergManifestListWriterSpec.builder()
              .from(writerSpec)
              .snapshotId(123L)
              .sequenceNumber(42L)
              .build();
      soft.assertThat(writerSpec2.writerSchema()).isSameAs(writerSchema);

      try (IcebergManifestListWriter listEntryWriter =
          IcebergManifestListWriter.openManifestListWriter(writerSpec, output)) {
        icebergManifestFiles.forEach(listEntryWriter::append);
      }
      manifestListData = output.toByteArray();
    }

    Path listFile = tempDir.resolve("snap-foo-bar" + AVRO.fileExtension());
    Files.write(listFile, manifestListData);

    try (IcebergManifestListReader listReader =
        IcebergManifestListReader.openManifestListReader(
            new SeekableFileInput(listFile.toFile()))) {
      soft.assertThat(listReader)
          .asInstanceOf(type(IcebergManifestListReader.class))
          .extracting(
              IcebergManifestListReader::spec,
              IcebergManifestListReader::snapshotId,
              IcebergManifestListReader::parentSnapshotId,
              IcebergManifestListReader::sequenceNumber)
          .containsExactly(spec, snapshotId, parentSnapshotId, sequenceNumber);

      List<IcebergManifestFile> files = new ArrayList<>();
      listReader.forEachRemaining(files::add);
      soft.assertThat(files)
          .containsExactlyElementsOf(expectedManifestFiles(spec, icebergManifestFiles));
    }

    Path referenceFile = tempDir.resolve("snap-bar-foo" + AVRO.fileExtension());
    @SuppressWarnings("resource")
    LocalFileIO io = new LocalFileIO();

    List<ManifestFile> referenceFiles = new ArrayList<>();
    try (CloseableIterable<ManifestFile> reader =
        IcebergBridge.loadManifestList(new InMemoryInputFile(manifestListData))) {
      reader.forEach(referenceFiles::add);
    }
    soft.assertThat(referenceFiles.stream().map(TestAvroSerialization::toIcebergManifestFile))
        .containsExactlyElementsOf(icebergManifestFiles);

    OutputFile referenceOutput = io.newOutputFile(referenceFile.toUri().toString());
    try (IcebergBridge.ManifestListWriterBridge writer =
        IcebergBridge.writeManifestList(
            spec.version(), referenceOutput, snapshotId, parentSnapshotId, sequenceNumber)) {
      icebergManifestFiles.stream().map(TestAvroSerialization::toManifestFile).forEach(writer::add);
    }

    try (IcebergManifestListReader listEntryReader =
        IcebergManifestListReader.openManifestListReader(
            new SeekableFileInput(referenceFile.toFile()))) {
      soft.assertThat(listEntryReader)
          .asInstanceOf(type(IcebergManifestListReader.class))
          .extracting(
              IcebergManifestListReader::spec,
              IcebergManifestListReader::snapshotId,
              IcebergManifestListReader::parentSnapshotId,
              IcebergManifestListReader::sequenceNumber)
          .containsExactly(spec, snapshotId, parentSnapshotId, sequenceNumber);

      List<IcebergManifestFile> files = new ArrayList<>();
      listEntryReader.forEachRemaining(files::add);
      soft.assertThat(files)
          .containsExactlyElementsOf(expectedManifestFiles(spec, icebergManifestFiles));
    }
  }

  private List<IcebergManifestFile> expectedManifestFiles(
      IcebergSpec spec, List<IcebergManifestFile> icebergManifestFiles) {
    if (spec.version() >= 2) {
      return icebergManifestFiles;
    }
    return icebergManifestFiles.stream()
        .map(
            mf ->
                IcebergManifestFile.builder()
                    .from(mf)
                    .content(null)
                    .sequenceNumber(null)
                    .minSequenceNumber(null)
                    .build())
        .collect(Collectors.toList());
  }

  static IcebergManifestFile toIcebergManifestFile(ManifestFile file) {
    IcebergManifestFile.Builder builder =
        IcebergManifestFile.builder()
            .content(IcebergManifestContent.valueOf(file.content().name()))
            .partitionSpecId(file.partitionSpecId())
            .addedSnapshotId(file.snapshotId())
            .keyMetadata(toByteArray(file.keyMetadata()))
            .manifestLength(file.length())
            .manifestPath(file.path())
            .addedFilesCount(file.addedFilesCount())
            .addedRowsCount(file.addedRowsCount())
            .existingFilesCount(file.existingFilesCount())
            .existingRowsCount(file.existingRowsCount())
            .deletedFilesCount(file.deletedFilesCount())
            .deletedRowsCount(file.deletedRowsCount())
            .partitions(
                file.partitions().stream()
                    .map(TestAvroSerialization::toIcebergPartitionFieldSummary)
                    .collect(Collectors.toList()));
    if (file.sequenceNumber() != 0L) {
      builder.sequenceNumber(file.sequenceNumber());
    }
    if (file.minSequenceNumber() != 0L) {
      builder.minSequenceNumber(file.minSequenceNumber());
    }
    return builder.build();
  }

  static IcebergPartitionFieldSummary toIcebergPartitionFieldSummary(
      ManifestFile.PartitionFieldSummary field) {
    return IcebergPartitionFieldSummary.icebergPartitionFieldSummary(
        field.containsNull(),
        toByteArray(field.lowerBound()),
        toByteArray(field.upperBound()),
        field.containsNaN());
  }

  @SuppressWarnings({"DataFlowIssue", "deprecation"})
  // GenericManifestFile c'tor removed in Iceberg 1.10, tackled then
  static ManifestFile toManifestFile(IcebergManifestFile file) {
    ToLongFunction<Long> default0L = boxed -> boxed != null ? boxed : 0L;
    return new GenManifestFile(
        file.manifestPath(),
        file.manifestLength(),
        file.partitionSpecId(),
        ManifestContent.valueOf(file.content().name()),
        default0L.applyAsLong(file.sequenceNumber()),
        default0L.applyAsLong(file.minSequenceNumber()),
        file.addedSnapshotId(),
        file.addedFilesCount(),
        file.addedRowsCount(),
        file.existingFilesCount(),
        file.existingRowsCount(),
        file.deletedFilesCount(),
        file.deletedRowsCount(),
        file.partitions().stream()
            .map(TestAvroSerialization::toPartitionFieldSummary)
            .collect(Collectors.toList()));
  }

  @SuppressWarnings("DataFlowIssue")
  static ManifestFile.PartitionFieldSummary toPartitionFieldSummary(
      IcebergPartitionFieldSummary summary) {
    return new GenericPartitionFieldSummary(
        summary.containsNull(),
        summary.containsNan(),
        toByteBuffer(summary.lowerBound()),
        toByteBuffer(summary.upperBound()));
  }

  static Stream<Arguments> manifestLists() {
    IcebergSchema icebergSchema =
        IcebergSchema.schema(
            123,
            asList(1, 2),
            asList(
                nestedField(1, "one", true, stringType(), "doc for one"),
                nestedField(2, "two", true, stringType(), "doc for two")));
    IcebergPartitionSpec icebergPartitionSpec =
        partitionSpec(
            42,
            asList(
                partitionField("one", "identity", 1, 1111),
                partitionField("two", "identity", 2, 1112)));

    return Stream.of(
        arguments(
            "two-lists-v1",
            IcebergSpec.V1,
            icebergSchema,
            icebergPartitionSpec,
            111L,
            110L,
            0L,
            asList(
                IcebergManifestFile.builder()
                    .manifestPath("file:///foo/bar/baz-1")
                    .partitionSpecId(11)
                    .manifestLength(123L)
                    .addedSnapshotId(4242L)
                    .content(DATA)
                    .addedFilesCount(10)
                    .addedRowsCount(1000L)
                    .existingFilesCount(20)
                    .existingRowsCount(2000L)
                    .deletedFilesCount(30)
                    .deletedRowsCount(3000L)
                    .build(),
                IcebergManifestFile.builder()
                    .manifestPath("file:///foo/bar/baz-2")
                    .partitionSpecId(22)
                    .manifestLength(456L)
                    .addedSnapshotId(666L)
                    .content(DATA)
                    .addedFilesCount(11)
                    .addedRowsCount(1001L)
                    .existingFilesCount(21)
                    .existingRowsCount(2001L)
                    .deletedFilesCount(31)
                    .deletedRowsCount(3001L)
                    .build())),
        arguments(
            "two-lists-v2",
            IcebergSpec.V2,
            icebergSchema,
            icebergPartitionSpec,
            211L,
            210L,
            212L,
            asList(
                IcebergManifestFile.builder()
                    .manifestPath("file:///foo/bar/baz-1")
                    .minSequenceNumber(42L)
                    .sequenceNumber(100L)
                    .partitionSpecId(11)
                    .manifestLength(123L)
                    .addedSnapshotId(4242L)
                    .content(DATA)
                    .addedFilesCount(10)
                    .addedRowsCount(1000L)
                    .existingFilesCount(20)
                    .existingRowsCount(2000L)
                    .deletedFilesCount(30)
                    .deletedRowsCount(3000L)
                    .build(),
                IcebergManifestFile.builder()
                    .manifestPath("file:///foo/bar/baz-2")
                    .minSequenceNumber(142L)
                    .sequenceNumber(200L)
                    .partitionSpecId(22)
                    .manifestLength(456L)
                    .addedSnapshotId(666L)
                    .content(DELETES)
                    .addedFilesCount(11)
                    .addedRowsCount(1001L)
                    .existingFilesCount(21)
                    .existingRowsCount(2001L)
                    .deletedFilesCount(31)
                    .deletedRowsCount(3001L)
                    .build())));
  }

  @ParameterizedTest
  @MethodSource
  public void manifestFiles(
      @SuppressWarnings("unused") String description,
      IcebergSpec spec,
      IcebergSchema schema,
      IcebergPartitionSpec partitionSpec,
      List<IcebergDataFile> dataFiles)
      throws Exception {
    IcebergManifestFile icebergManifestFile;

    Long fileSequenceNumber = null;
    long sequenceNumber = 8888L;
    long snapshotId = 42424242L;

    Schema writerSchema;

    IcebergManifestFileWriterSpec writerSpec =
        IcebergManifestFileWriterSpec.builder()
            .spec(spec)
            .content(DATA)
            .schema(schema)
            .partitionSpec(partitionSpec)
            .addedSnapshotId(42424242L)
            .sequenceNumber(8888L)
            .minSequenceNumber(7777L)
            .manifestPath("file:///foo/foo/bar")
            .build();

    writerSchema = writerSpec.writerSchema();

    // "Derived" `IcebergManifestFileWriterSpec` instances must yield the same Avro `Schema`
    // and JSON/String representation instances
    IcebergManifestFileWriterSpec writerSpec2 =
        IcebergManifestFileWriterSpec.builder()
            .from(writerSpec)
            .addedSnapshotId(123L)
            .sequenceNumber(42L)
            .minSequenceNumber(123L)
            .manifestPath("file:///blah")
            .build();
    soft.assertThat(writerSpec2.writerSchema()).isSameAs(writerSchema);
    soft.assertThat(writerSpec2.schemaAsJsonString()).isSameAs(writerSpec.schemaAsJsonString());
    soft.assertThat(writerSpec2.partitionSpecAsJsonString())
        .isSameAs(writerSpec.partitionSpecAsJsonString());

    // Write manifest file using IcebergManifestFileWriter
    byte[] manifestData;
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        IcebergManifestFileWriter entryWriter =
            IcebergManifestFileWriter.openManifestFileWriter(writerSpec, output)) {

      for (IcebergDataFile dataFile : dataFiles) {
        entryWriter.append(dataFile, ADDED, fileSequenceNumber, sequenceNumber, snapshotId);
      }

      icebergManifestFile = entryWriter.finish();

      manifestData = output.toByteArray();
    }

    Path realFile = tempDir.resolve("my-manifest" + AVRO.fileExtension());
    Files.write(realFile, manifestData);

    LocalFileIO io = new LocalFileIO();

    // Write reference manifest file using ManifestWriter
    ManifestFile manifestFile;
    Path referenceFile = tempDir.resolve("reference-manifest" + AVRO.fileExtension());
    org.apache.iceberg.Schema referenceSchema =
        SchemaParser.fromJson(spec.jsonWriter().writeValueAsString(schema));
    PartitionSpec referencePartitionSpec =
        PartitionSpecParser.fromJson(
            referenceSchema, spec.jsonWriter().writeValueAsString(partitionSpec));
    {
      ManifestWriter<DataFile> writer =
          ManifestFiles.write(
              spec.version(),
              referencePartitionSpec,
              io.newOutputFile(referenceFile.toUri().toString()),
              icebergManifestFile.addedSnapshotId());
      try (writer) {
        for (IcebergDataFile dataFile : dataFiles) {
          PartitionData referencePartition =
              new PartitionData(referencePartitionSpec.partitionType());
          for (Schema.Field partitionField : dataFile.partition().getSchema().getFields()) {
            referencePartition.put(
                partitionField.pos(), dataFile.partition().get(partitionField.name()));
          }

          writer.add(
              toDataFile(dataFile, referencePartitionSpec, referencePartition), sequenceNumber);
        }
      }
      manifestFile = writer.toManifestFile();
    }

    // Read reference manifest file using ManifestReader
    List<ManifestFile.PartitionFieldSummary> partitions = emptyList();
    ManifestFile manifestFileX = toGenericManifestFile(realFile, icebergManifestFile, partitions);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, io)) {
      Schema readerSchema =
          AvroSchemaUtil.convert(
              IcebergBridge.manifestEntrySchema(spec.version(), reader.spec().partitionType()),
              "r2");

      soft.assertThat(writerSchema.getFields()).containsExactlyElementsOf(readerSchema.getFields());

      Schema partSchema = AvroSchemaUtil.convert(reader.spec().partitionType(), "r102");
      soft.assertThat(partSchema).isEqualTo(partitionSpec.avroSchema(schema, "r102"));

      List<IcebergDataFile> refDataFiles =
          StreamSupport.stream(reader.spliterator(), false)
              .map(
                  dataFile -> {
                    StructLike part = dataFile.partition();

                    GenericData.Record partition = new GenericData.Record(partSchema);
                    for (Schema.Field partField : partSchema.getFields()) {
                      partition.put(partField.pos(), part.get(partField.pos(), String.class));
                    }

                    IcebergDataFile.Builder dataFileBuilder =
                        IcebergDataFile.builder()
                            .partition(partition)
                            .recordCount(dataFile.recordCount());
                    if (spec.version() >= 2) {
                      dataFileBuilder.equalityIds(dataFile.equalityFieldIds());
                    }
                    if (dataFile.splitOffsets() != null) {
                      dataFileBuilder.splitOffsets(dataFile.splitOffsets());
                    }
                    if (dataFile.columnSizes() != null) {
                      dataFileBuilder.columnSizes(dataFile.columnSizes());
                    }
                    if (dataFile.lowerBounds() != null) {
                      dataFileBuilder.lowerBounds(
                          dataFile.lowerBounds().entrySet().stream()
                              .collect(
                                  Collectors.toMap(
                                      Map.Entry::getKey, e -> toByteArray(e.getValue()))));
                    }
                    if (dataFile.upperBounds() != null) {
                      dataFileBuilder.upperBounds(
                          dataFile.upperBounds().entrySet().stream()
                              .collect(
                                  Collectors.toMap(
                                      Map.Entry::getKey, e -> toByteArray(e.getValue()))));
                    }
                    if (dataFile.nanValueCounts() != null) {
                      dataFileBuilder.nanValueCounts(dataFile.nanValueCounts());
                    }
                    if (dataFile.nullValueCounts() != null) {
                      dataFileBuilder.nullValueCounts(dataFile.nullValueCounts());
                    }
                    return dataFileBuilder
                        .content(IcebergDataContent.valueOf(dataFile.content().name()))
                        .fileFormat(IcebergFileFormat.valueOf(dataFile.format().name()))
                        .filePath(dataFile.location())
                        .fileSizeInBytes(dataFile.fileSizeInBytes())
                        // .blockSizeInBytes(dataFile.)
                        .keyMetadata(toByteArray(dataFile.keyMetadata()))
                        .sortOrderId(dataFile.sortOrderId())
                        // .specId(dataFile.specId())
                        .build();
                  })
              .collect(Collectors.toList());
      soft.assertThat(refDataFiles).containsExactlyElementsOf(dataFiles);
    }

    // Read manifest file using IcebergManifestFileReader
    try (IcebergManifestFileReader entryReader =
        IcebergManifestFileReader.openManifestReader(new SeekableFileInput(realFile.toFile()))) {
      soft.assertThat(entryReader)
          .asInstanceOf(type(IcebergManifestFileReader.class))
          .extracting(
              IcebergManifestFileReader::spec,
              IcebergManifestFileReader::schema,
              IcebergManifestFileReader::partitionSpec,
              IcebergManifestFileReader::content)
          .containsExactly(spec, schema, partitionSpec, DATA);

      List<IcebergManifestEntry> allEntries = new ArrayList<>();
      entryReader.forEachRemaining(allEntries::add);

      soft.assertThat(allEntries)
          .containsExactlyElementsOf(
              dataFiles.stream()
                  .map(
                      df ->
                          dataFileToIcebergManifestEntry(
                              df, spec, snapshotId, fileSequenceNumber, sequenceNumber))
                  .collect(Collectors.toList()));
    }

    // Read reference manifest file using IcebergManifestFileReader
    try (IcebergManifestFileReader entryReader =
        IcebergManifestFileReader.openManifestReader(
            new SeekableFileInput(referenceFile.toFile()))) {
      soft.assertThat(entryReader)
          .asInstanceOf(type(IcebergManifestFileReader.class))
          .extracting(
              IcebergManifestFileReader::spec,
              IcebergManifestFileReader::schema,
              IcebergManifestFileReader::partitionSpec,
              IcebergManifestFileReader::content)
          .containsExactly(spec, schema, partitionSpec, DATA);

      List<IcebergManifestEntry> allEntries = new ArrayList<>();
      entryReader.forEachRemaining(allEntries::add);

      soft.assertThat(allEntries)
          .containsExactlyElementsOf(
              dataFiles.stream()
                  .map(
                      df ->
                          dataFileToIcebergManifestEntry(
                              df, spec, snapshotId, fileSequenceNumber, sequenceNumber))
                  .collect(Collectors.toList()));
    }
  }

  private static IcebergManifestEntry dataFileToIcebergManifestEntry(
      IcebergDataFile dataFile,
      IcebergSpec spec,
      Long snapshotId,
      Long fileSequenceNumber,
      Long sequenceNumber) {
    IcebergManifestEntry.Builder entryBuilder =
        IcebergManifestEntry.builder().dataFile(dataFile).status(ADDED).snapshotId(snapshotId);
    if (spec.version() == 1) {
      return entryBuilder.build();
    }
    return entryBuilder
        .fileSequenceNumber(fileSequenceNumber)
        .sequenceNumber(sequenceNumber)
        .build();
  }

  static DataFile toDataFile(
      IcebergDataFile dataFile, PartitionSpec partitionSpec, PartitionData partitionData) {
    return new DataFiles.Builder(partitionSpec)
        .withFormat(FileFormat.valueOf(dataFile.fileFormat().name()))
        .withFileSizeInBytes(dataFile.fileSizeInBytes())
        .withRecordCount(dataFile.recordCount())
        // (deprecated since Iceberg 1.5) .withEqualityFieldIds(dataFile.equalityIds())
        .withPartition(partitionData)
        .withPath(dataFile.filePath())
        .withMetrics(
            new Metrics(
                dataFile.recordCount(),
                dataFile.columnSizes(),
                dataFile.valueCounts(),
                dataFile.nullValueCounts(),
                dataFile.nanValueCounts(),
                dataFile.lowerBounds().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toByteBuffer(e.getValue()))),
                dataFile.upperBounds().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toByteBuffer(e.getValue())))))
        .build();
  }

  @SuppressWarnings({"DataFlowIssue", "deprecation"})
  // GenericManifestFile c'tor removed in Iceberg 1.10, tackled then
  static ManifestFile toGenericManifestFile(
      Path realFile,
      IcebergManifestFile icebergManifestFile,
      List<ManifestFile.PartitionFieldSummary> partitions) {
    return new GenManifestFile(
        realFile.toUri().toString(),
        icebergManifestFile.manifestLength(),
        icebergManifestFile.partitionSpecId(),
        ManifestContent.valueOf(icebergManifestFile.content().name()),
        icebergManifestFile.sequenceNumber(),
        icebergManifestFile.minSequenceNumber(),
        icebergManifestFile.addedSnapshotId(),
        icebergManifestFile.addedFilesCount(),
        icebergManifestFile.addedRowsCount(),
        icebergManifestFile.existingFilesCount(),
        icebergManifestFile.existingRowsCount(),
        icebergManifestFile.deletedFilesCount(),
        icebergManifestFile.deletedRowsCount(),
        partitions);
  }

  private static byte[] toByteArray(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(bytes);
    return bytes;
  }

  private static ByteBuffer toByteBuffer(byte[] bytes) {
    return bytes != null ? ByteBuffer.wrap(bytes) : null;
  }

  static Stream<Arguments> manifestFiles() {
    IcebergSchema icebergSchema =
        IcebergSchema.schema(
            123,
            asList(1, 2),
            asList(
                nestedField(1, "one", true, stringType(), "doc for one"),
                nestedField(2, "two", true, stringType(), "doc for two")));
    IcebergPartitionSpec icebergPartitionSpec =
        partitionSpec(
            42,
            asList(
                partitionField("one", "identity", 1, 1111),
                partitionField("two", "identity", 2, 1112)));
    Schema partitionAvroSchema = icebergPartitionSpec.avroSchema(icebergSchema, "r102");
    GenericData.Record partition = new GenericData.Record(partitionAvroSchema);
    partition.put("one", "hello");
    partition.put("two", "world");

    return Stream.of(
        arguments(
            "simple-v1",
            IcebergSpec.V1,
            icebergSchema,
            icebergPartitionSpec,
            singletonList(
                IcebergDataFile.builder()
                    .fileFormat(PARQUET)
                    .filePath("file:///blah/blah/spec_v1")
                    .fileSizeInBytes(666L)
                    .content(IcebergDataContent.DATA)
                    .recordCount(11L)
                    .partition(partition)
                    .build()
                // TODO !!!
                )),
        arguments(
            "simple-v2",
            IcebergSpec.V2,
            icebergSchema,
            icebergPartitionSpec,
            singletonList(
                IcebergDataFile.builder()
                    .fileFormat(PARQUET)
                    .filePath("file:///blah/blah/spec_v2")
                    .fileSizeInBytes(666L)
                    .content(IcebergDataContent.DATA)
                    .recordCount(11L)
                    .partition(partition)
                    .build()
                // TODO !!!
                )));
  }

  @ParameterizedTest
  @MethodSource
  public <T> void avroSerialization(
      @SuppressWarnings("unused") String description,
      AvroTyped<T> avroSchema,
      @SuppressWarnings("unused") Class<T> type,
      T referenceObject,
      AvroReadWriteContext readContext,
      Map<String, String> properties)
      throws Exception {

    byte[] serialized;
    Schema writeSchema;
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      // TODO remove?? avroSchema.prefix(output);
      writeSchema = avroSchema.writeSchema(readContext);

      DatumWriter<T> datumWriter =
          new DatumWriter<>() {
            private Schema writeSchema;

            @Override
            public void setSchema(Schema schema) {
              this.writeSchema = schema;
            }

            @Override
            public void write(T datum, Encoder out) throws IOException {
              avroSchema.write(out, datum, writeSchema);
            }
          };

      try (DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
        properties.forEach(writer::setMeta);
        // TODO writer.setCodec();
        try (DataFileWriter<T> realWriter = writer.create(writeSchema, output)) {
          realWriter.append(referenceObject);
        }
      }

      serialized = output.toByteArray();
    }

    try (SeekableByteArrayInput input = new SeekableByteArrayInput(serialized)) {
      DatumReader<T> datumReader =
          new DatumReader<>() {
            private Schema readSchema;

            @Override
            public void setSchema(Schema schema) {
              this.readSchema = schema;
            }

            @Override
            public T read(T reuse, Decoder in) throws IOException {
              return avroSchema.read(in, readSchema);
            }
          };

      DataFileReader<T> reader = new DataFileReader<>(input, datumReader);

      Schema readerSchema = reader.getSchema();

      soft.assertThat(readerSchema).isEqualTo(writeSchema);

      soft.assertThat(reader.hasNext()).isTrue();
      T value = reader.next();
      soft.assertThat(value).isEqualTo(referenceObject);

      soft.assertThat(reader.hasNext()).isFalse();
    }
  }

  static Stream<Arguments> avroSerialization() throws Exception {
    List<Schema.Field> fields =
        asList(
            new Schema.Field("some_string", Schema.create(Schema.Type.STRING)),
            new Schema.Field("an_int", Schema.create(Schema.Type.INT)));
    Schema avroSchema = Schema.createRecord("r102", null, null, false, fields);
    GenericData.Record partitionKey = new GenericData.Record(avroSchema);
    partitionKey.put(0, "hello");
    partitionKey.put(1, 42);
    AvroReadWriteContext entryReadContext =
        AvroReadWriteContext.builder().putSchemaOverride("data_file.partition", avroSchema).build();

    AvroBundle v1 = Avro.bundleFor(1);
    AvroBundle v2 = Avro.bundleFor(2);

    IcebergSchema icebergSchema =
        IcebergSchema.schema(
            123,
            asList(1, 2),
            asList(
                nestedField(1, "one", true, stringType(), "doc for one"),
                nestedField(2, "two", true, stringType(), "doc for two")));
    IcebergPartitionSpec icebergPartitionSpec =
        partitionSpec(
            42,
            asList(
                partitionField("one", "identity", 1, 1111),
                partitionField("two", "identity", 2, 1111)));

    IcebergSpec spec = IcebergSpec.forVersion(2);

    String schemaJson = spec.jsonWriter().writeValueAsString(icebergSchema);
    String specJson = spec.jsonWriter().writeValueAsString(icebergPartitionSpec);

    return Stream.of(
        arguments(
            "manifest file V1",
            v1.schemaManifestFile(),
            IcebergManifestFile.class,
            IcebergManifestFile.builder()
                .manifestPath("path")
                .manifestLength(42L)
                .partitionSpecId(42)
                .addPartitions(
                    icebergPartitionFieldSummary(
                        false, "aaa".getBytes(UTF_8), "zzz".getBytes(UTF_8), null),
                    icebergPartitionFieldSummary(
                        true, "000".getBytes(UTF_8), "999".getBytes(UTF_8), Boolean.TRUE))
                .addedSnapshotId(666L)
                .build(),
            null,
            ImmutableMap.builder()
                .put("schema", schemaJson)
                .put("partition-spec", specJson)
                .put("partition-spec-id", "" + icebergPartitionSpec.specId())
                .put("format-version", "" + spec.version())
                .put("content", "data")
                .build()),
        arguments(
            "manifest file V2",
            v2.schemaManifestFile(),
            IcebergManifestFile.class,
            IcebergManifestFile.builder()
                .manifestPath("path")
                .manifestLength(42L)
                .content(DATA)
                .partitionSpecId(42)
                .addPartitions(
                    icebergPartitionFieldSummary(
                        false, "aaa".getBytes(UTF_8), "zzz".getBytes(UTF_8), null),
                    icebergPartitionFieldSummary(
                        true, "000".getBytes(UTF_8), "999".getBytes(UTF_8), Boolean.TRUE))
                .addedSnapshotId(666L)
                .sequenceNumber(123L)
                .minSequenceNumber(111L)
                .addedFilesCount(3)
                .addedRowsCount(33L)
                .existingFilesCount(3)
                .existingRowsCount(33L)
                .deletedFilesCount(3)
                .deletedRowsCount(33L)
                .build(),
            null,
            ImmutableMap.builder()
                .put("schema", schemaJson)
                .put("partition-spec", specJson)
                .put("partition-spec-id", "" + icebergPartitionSpec.specId())
                .put("format-version", "" + spec.version())
                .put("content", "data")
                .build()),
        arguments(
            "manifest entry V1",
            v1.schemaManifestEntry(),
            IcebergManifestEntry.class,
            IcebergManifestEntry.builder()
                // Not in v1: .sequenceNumber(666L)
                // Not in v1: .fileSequenceNumber(600L)
                .snapshotId(42L)
                .status(ADDED)
                .dataFile(
                    IcebergDataFile.builder()
                        .filePath("file-path")
                        .fileFormat(PARQUET)
                        .recordCount(43L)
                        .fileSizeInBytes(1234L)
                        .partition(partitionKey)
                        .build())
                .build(),
            entryReadContext,
            emptyMap()),
        arguments(
            "manifest entry V2",
            v2.schemaManifestEntry(),
            IcebergManifestEntry.class,
            IcebergManifestEntry.builder()
                .sequenceNumber(666L)
                .fileSequenceNumber(600L)
                .snapshotId(42L)
                .status(ADDED)
                .dataFile(
                    IcebergDataFile.builder()
                        .content(IcebergDataContent.DATA)
                        .filePath("file-path")
                        .fileFormat(PARQUET)
                        .recordCount(43L)
                        .fileSizeInBytes(1234L)
                        .partition(partitionKey)
                        .build())
                .build(),
            entryReadContext,
            emptyMap()));
  }
}
