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
package org.projectnessie.catalog.formats.iceberg.meta;

import static com.google.common.base.Preconditions.checkArgument;
import static java.io.OutputStream.nullOutputStream;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.catalog.formats.iceberg.LocalFileIO.nullOutputFile;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat.AVRO;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.partitionSpec;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.schema;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.AVRO_COMPRESSION;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.AVRO_COMPRESSION_LEVEL;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
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
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.projectnessie.catalog.formats.iceberg.GenManifestFile;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.LocalFileIO;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntryStatus;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriterSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriterSpec;

@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class AvroSerializeBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    @Param({"100"})
    public int schemaFields;

    @Param({"2"})
    public int specVersion;

    @Param({"3"})
    public int partitionSpecFields;

    @Param({"10", "1000"})
    public int entriesCount;

    @Param({"gzip", "snappy", "zstd", "uncompressed"})
    public String avroCompression;

    @Param("9")
    public int avroCompressionLevel;

    @Param("30")
    public int fieldPrefixLength;

    @Param("150")
    public int pathPrefixLength;

    IcebergSpec spec;
    Map<String, String> tableProperties;

    IcebergSchema icebergSchema;
    IcebergPartitionSpec icebergPartitionSpec;
    List<IcebergDataFile> icebergDataFiles;
    byte[] serializedIcebergManifestFile;
    List<IcebergManifestFile> icebergManifestFiles;
    byte[] serializedIcebergManifestList;

    Schema schema;
    PartitionSpec partitionSpec;
    List<DataFile> dataFiles;
    byte[] serializedManifestFile;
    List<ManifestFile> manifestFiles;
    byte[] serializedManifestList;
    ManifestFile manifestFile;

    @Setup
    public void init() throws Exception {

      spec = IcebergSpec.forVersion(specVersion);

      IntFunction<String> fieldName =
          f -> prefixed("f_", fieldPrefixLength).append('_').append(f).toString();

      tableProperties =
          ImmutableMap.of(
              AVRO_COMPRESSION,
              avroCompression,
              AVRO_COMPRESSION_LEVEL,
              Integer.toString(avroCompressionLevel));

      List<IcebergNestedField> fields = new ArrayList<>(schemaFields);

      for (int f = 1; f <= schemaFields; f++) {
        fields.add(
            nestedField(
                f,
                fieldName.apply(f),
                f <= partitionSpecFields,
                stringType(),
                "doc for field " + f));
      }
      List<Integer> identifierFields =
          IntStream.rangeClosed(1, partitionSpecFields).boxed().collect(Collectors.toList());
      icebergSchema = schema(1, identifierFields, fields);

      List<IcebergPartitionField> partitionFields = new ArrayList<>(partitionSpecFields);
      for (int f = 1; f <= partitionSpecFields; f++) {
        partitionFields.add(partitionField(fieldName.apply(f) + "_part", "identity", f, 10000 + f));
      }
      icebergPartitionSpec = partitionSpec(1, partitionFields);

      ThreadLocalRandom random = ThreadLocalRandom.current();

      Supplier<byte[]> fieldValueSupplier =
          () -> prefixed("", 30).toString().getBytes(StandardCharsets.UTF_8);

      icebergDataFiles = new ArrayList<>(entriesCount);
      for (int i = 0; i < entriesCount; i++) {
        GenericData.Record partition =
            new GenericData.Record(icebergPartitionSpec.avroSchema(icebergSchema, "r102"));
        for (int partField = 0; partField < partitionSpecFields; partField++) {
          partition.put(partField++, prefixed("", 30).toString());
        }

        icebergDataFiles.add(
            (IcebergDataFile.builder()
                .filePath(
                    prefixed("s3:///", pathPrefixLength).append(AVRO.fileExtension()).toString())
                .fileSizeInBytes(random.nextLong(10000, 10 * 1024 * 1024 * 1024L))
                .fileFormat(IcebergFileFormat.PARQUET)
                .recordCount(random.nextLong(1, 10_000_000))
                .partition(partition)
                .sortOrderId(4567)
                .content(IcebergDataContent.DATA)
                .specId(specVersion)
                .upperBounds(
                    IntStream.rangeClosed(1, schemaFields)
                        .boxed()
                        .collect(Collectors.toMap(f -> f, f -> fieldValueSupplier.get())))
                .lowerBounds(
                    IntStream.rangeClosed(1, schemaFields)
                        .boxed()
                        .collect(Collectors.toMap(f -> f, f -> fieldValueSupplier.get())))
                .nullValueCounts(
                    IntStream.rangeClosed(1, schemaFields)
                        .boxed()
                        .collect(Collectors.toMap(f -> f, f -> random.nextLong(10_000))))
                // .nanValueCounts(nans)
                // .equalityIds()
                .splitOffsets(
                    IntStream.range(1, 10)
                        .mapToLong(x -> random.nextLong(10000, 10 * 1024 * 1024 * 1024L))
                        .boxed()
                        .collect(Collectors.toList()))
                .columnSizes(
                    IntStream.rangeClosed(1, schemaFields)
                        .boxed()
                        .collect(Collectors.toMap(f -> f, f -> random.nextLong(10_000))))
                .build()));
      }

      icebergManifestFiles = new ArrayList<>(entriesCount);
      for (int i = 0; i < entriesCount; i++) {
        icebergManifestFiles.add(
            IcebergManifestFile.builder()
                .content(IcebergManifestContent.DATA)
                .manifestPath(
                    prefixed("s3:///", pathPrefixLength).append(AVRO.fileExtension()).toString())
                .manifestLength(random.nextLong(10000, 32 * 1024 * 1024L))
                .partitionSpecId(random.nextInt(1, 1000))
                .sequenceNumber(random.nextLong(1, 10_000_000))
                .minSequenceNumber(random.nextLong(1, 10_000_000))
                .addedSnapshotId(random.nextLong(1, Long.MAX_VALUE))
                .addedFilesCount(random.nextInt(1, 250))
                .addedRowsCount(random.nextLong(500, 10_000_000))
                .existingFilesCount(0)
                .existingRowsCount(0L)
                .deletedFilesCount(0)
                .deletedRowsCount(0L)
                .partitions(
                    IntStream.range(1, partitionSpecFields)
                        .mapToObj(
                            x ->
                                IcebergPartitionFieldSummary.icebergPartitionFieldSummary(
                                    true, fieldValueSupplier.get(), fieldValueSupplier.get(), null))
                        .collect(Collectors.toList()))
                .build());
      }

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      serializeIcebergManifestFile(this, output);
      serializedIcebergManifestFile = output.toByteArray();

      output = new ByteArrayOutputStream();
      serializeIcebergManifestList(this, output);
      serializedIcebergManifestList = output.toByteArray();

      schema = SchemaParser.fromJson(spec.jsonWriter().writeValueAsString(icebergSchema));
      partitionSpec =
          PartitionSpecParser.fromJson(
              schema, spec.jsonWriter().writeValueAsString(icebergPartitionSpec));
      dataFiles = new ArrayList<>();
      for (IcebergDataFile dataFile : icebergDataFiles) {
        PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
        for (org.apache.avro.Schema.Field partitionField :
            dataFile.partition().getSchema().getFields()) {
          partitionData.put(partitionField.pos(), dataFile.partition().get(partitionField.name()));
        }

        dataFiles.add(toDataFile(dataFile, partitionSpec, partitionData));
      }
      LocalFileIO.ByteArrayOutputFile outputFile =
          new LocalFileIO.ByteArrayOutputFile(
              "file:///blah/blah/blah/blah/blah" + AVRO.fileExtension());
      manifestFile = serializeManifestFile(this, outputFile);
      serializedManifestFile = outputFile.toByteArray();

      manifestFiles =
          icebergManifestFiles.stream()
              .map(AvroSerializeBench::toManifestFile)
              .collect(Collectors.toList());

      outputFile =
          new LocalFileIO.ByteArrayOutputFile(
              "file:///blah/blah/blah/blah/blah" + AVRO.fileExtension());
      serializeManifestList(this, outputFile);
      serializedManifestList = outputFile.toByteArray();

      System.out.printf(
          "%nSerialized manifest file length : %d (%d)"
              + "%nSerialized manifest list length : %d (%d)"
              + "%n",
          serializedIcebergManifestFile.length,
          serializedManifestFile.length,
          serializedIcebergManifestList.length,
          serializedManifestList.length);
    }
  }

  // "our" implementation

  @Benchmark
  public IcebergManifestFile serializeIcebergManifestFile(BenchmarkParam param) throws Exception {
    return serializeIcebergManifestFile(param, nullOutputStream());
  }

  private static IcebergManifestFile serializeIcebergManifestFile(
      BenchmarkParam param, OutputStream output) throws Exception {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    IcebergManifestFileWriterSpec writerSpec =
        IcebergManifestFileWriterSpec.builder()
            .spec(param.spec)
            .schema(param.icebergSchema)
            .partitionSpec(param.icebergPartitionSpec)
            .content(IcebergManifestContent.DATA)
            .addedSnapshotId(Long.MAX_VALUE)
            .sequenceNumber(123456789L)
            .minSequenceNumber(123456789L)
            .tableProperties(param.tableProperties)
            .manifestPath(
                prefixed("s3:///", param.pathPrefixLength).append(AVRO.fileExtension()).toString())
            .build();
    try (IcebergManifestFileWriter writer =
        IcebergManifestFileWriter.openManifestFileWriter(writerSpec, output)) {
      for (IcebergDataFile icebergDataFile : param.icebergDataFiles) {
        writer.append(
            icebergDataFile,
            IcebergManifestEntryStatus.ADDED,
            random.nextLong(1, Long.MAX_VALUE),
            random.nextLong(1, Long.MAX_VALUE),
            random.nextLong(1, Long.MAX_VALUE));
      }
      return writer.finish();
    }
  }

  @Benchmark
  public void deserializeIcebergManifestFile(BenchmarkParam param, Blackhole blackhole)
      throws Exception {
    try (IcebergManifestFileReader reader =
        IcebergManifestFileReader.openManifestReader(
            new SeekableByteArrayInput(param.serializedIcebergManifestFile))) {
      reader.forEachRemaining(blackhole::consume);
    }
  }

  @Benchmark
  public void serializeIcebergManifestList(BenchmarkParam param, Blackhole blackhole)
      throws Exception {
    serializeIcebergManifestList(param, nullOutputStream());
  }

  private static void serializeIcebergManifestList(BenchmarkParam param, OutputStream output)
      throws Exception {
    IcebergManifestListWriterSpec writerSpec =
        IcebergManifestListWriterSpec.builder()
            .spec(param.spec)
            .schema(param.icebergSchema)
            .partitionSpec(param.icebergPartitionSpec)
            .snapshotId(Long.MAX_VALUE)
            .parentSnapshotId(Long.MAX_VALUE)
            .sequenceNumber(123456789L)
            .tableProperties(param.tableProperties)
            .build();
    try (IcebergManifestListWriter writer =
        IcebergManifestListWriter.openManifestListWriter(writerSpec, output)) {
      param.icebergManifestFiles.forEach(writer::append);
    }
  }

  @Benchmark
  public void deserializeIcebergManifestList(BenchmarkParam param, Blackhole blackhole)
      throws Exception {
    try (IcebergManifestListReader reader =
        IcebergManifestListReader.openManifestListReader(
            new SeekableByteArrayInput(param.serializedIcebergManifestList))) {
      reader.forEachRemaining(blackhole::consume);
    }
  }

  // "Apache Iceberg" implementation

  @Benchmark
  public ManifestFile serializeManifestFile(BenchmarkParam param) throws Exception {
    icebergAvroCompressionCheck(param);

    return serializeManifestFile(
        param, nullOutputFile("file:///blah/blah/blah/blah/blah" + AVRO.fileExtension()));
  }

  private static void icebergAvroCompressionCheck(BenchmarkParam param) {
    checkArgument(
        "gzip".equals(param.avroCompression) && 9 == param.avroCompressionLevel,
        "Iceberg has NO FUNCTIONING code path to configure a different Avro compression than gzip/9");
  }

  private static ManifestFile serializeManifestFile(BenchmarkParam param, OutputFile outputFile)
      throws Exception {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(param.specVersion, param.partitionSpec, outputFile, Long.MAX_VALUE);
    try (writer) {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      for (DataFile dataFile : param.dataFiles) {
        writer.add(dataFile, random.nextLong(1, Long.MAX_VALUE));
      }
    }
    return writer.toManifestFile();
  }

  @Benchmark
  public void deserializeManifestFile(BenchmarkParam param, Blackhole blackhole) throws Exception {
    icebergAvroCompressionCheck(param);

    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(
            param.manifestFile,
            new LocalFileIO.SingleInputFileIO(
                new InMemoryInputFile(param.manifestFile.path(), param.serializedManifestFile)))) {
      reader.forEach(blackhole::consume);
    }
  }

  @Benchmark
  public void serializeManifestList(BenchmarkParam param, Blackhole blackhole) throws Exception {
    icebergAvroCompressionCheck(param);
    serializeManifestList(param, nullOutputFile("file:///blah/blah/blah/blah/blah"));
  }

  private static void serializeManifestList(BenchmarkParam param, OutputFile outputFile)
      throws Exception {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    try (IcebergBridge.ManifestListWriterBridge writer =
        IcebergBridge.writeManifestList(
            param.specVersion,
            outputFile,
            random.nextLong(1, Long.MAX_VALUE),
            random.nextLong(1, Long.MAX_VALUE),
            random.nextLong(1, Long.MAX_VALUE))) {
      param.manifestFiles.forEach(writer::add);
    }
  }

  @Benchmark
  public void deserializeManifestList(BenchmarkParam param, Blackhole blackhole) throws Exception {
    icebergAvroCompressionCheck(param);
    try (CloseableIterable<ManifestFile> reader =
        IcebergBridge.loadManifestList(new InMemoryInputFile(param.serializedManifestList))) {
      reader.forEach(blackhole::consume);
    }
  }

  // Utilities

  static StringBuilder prefixed(String prefix, int len) {
    StringBuilder sb = new StringBuilder(prefix);
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < len; i++) {
      int c = random.nextInt(27);
      char ch = c < 26 ? (char) ('a' + c) : '_';
      sb.append(ch);
    }
    return sb;
  }

  static DataFile toDataFile(
      IcebergDataFile dataFile, PartitionSpec partitionSpec, PartitionData partitionData) {
    return new DataFiles.Builder(partitionSpec)
        .withFormat(FileFormat.valueOf(dataFile.fileFormat().name()))
        .withFileSizeInBytes(dataFile.fileSizeInBytes())
        .withRecordCount(dataFile.recordCount())
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
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, e -> ByteBuffer.wrap(e.getValue()))),
                dataFile.upperBounds().entrySet().stream()
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, e -> ByteBuffer.wrap(e.getValue())))))
        .build();
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
            .map(AvroSerializeBench::toPartitionFieldSummary)
            .collect(Collectors.toList()));
  }

  @SuppressWarnings("DataFlowIssue")
  static ManifestFile.PartitionFieldSummary toPartitionFieldSummary(
      IcebergPartitionFieldSummary summary) {
    return new GenericPartitionFieldSummary(
        summary.containsNull(),
        summary.containsNan() != null && summary.containsNan(),
        ByteBuffer.wrap(summary.lowerBound()),
        ByteBuffer.wrap(summary.upperBound()));
  }
}
