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
package org.projectnessie.iceberg.metadata;

import static org.apache.iceberg.view.IcebergBridge.historyEntry;
import static org.apache.iceberg.view.IcebergBridge.viewVersionMetadataToJson;
import static org.projectnessie.iceberg.metadata.NessieIceberg.icebergTableMetadata;
import static org.projectnessie.iceberg.metadata.NessieIceberg.randomColumnName;
import static org.projectnessie.iceberg.metadata.NessieIceberg.randomFields;
import static org.projectnessie.iceberg.metadata.NessieIceberg.randomProperties;
import static org.projectnessie.iceberg.metadata.NessieIceberg.randomSnapshot;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.view.BaseVersion;
import org.apache.iceberg.view.Version;
import org.apache.iceberg.view.VersionSummary;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.ViewVersionMetadata;

public class GenerateIcebergFiles {
  public static void main(String[] args) throws Exception {
    Path targetBaseDir = Paths.get(args[0]);

    cleanTargetDir(targetBaseDir);

    Random random = new Random(42L);

    FileWriter fileWriter =
        (scenario, name, json) -> {
          Path dir = targetBaseDir.resolve(scenario);
          Files.createDirectories(dir);
          Path file = dir.resolve(scenario + '-' + name + ".json");
          Files.write(
              file,
              NessieIceberg.asJsonNode(json).toPrettyString().getBytes(StandardCharsets.UTF_8),
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING);
        };

    icebergTableSimple(random, fileWriter);
    icebergTableThreeSnapshots(random, fileWriter);

    icebergViewSimple(random, fileWriter);
  }

  private static void icebergViewSimple(Random random, FileWriter fileWriter) throws IOException {
    List<NestedField> fields = randomFields(random, 3);

    Schema schema = new Schema(StructType.of(fields).fields());

    ViewDefinition definition =
        ViewDefinition.of(
            randomColumnName(random),
            schema,
            randomColumnName(random),
            Arrays.asList(
                randomColumnName(random), randomColumnName(random), randomColumnName(random)));

    int versionId = ThreadLocalRandom.current().nextInt(42666);
    Version version =
        new BaseVersion(
            versionId,
            null,
            ThreadLocalRandom.current().nextLong(),
            new VersionSummary(randomProperties(random, 0)),
            definition);

    ViewVersionMetadata metadata =
        new ViewVersionMetadata(
            randomColumnName(random),
            version.viewDefinition(),
            randomProperties(random, 1),
            version.versionId(),
            Collections.singletonList(version),
            Collections.singletonList(
                historyEntry(version.timestampMillis(), version.versionId())));

    fileWriter.writeFile("view-simple", "1", viewVersionMetadataToJson(metadata));
  }

  private static void icebergTableSimple(Random random, FileWriter fileWriter) throws IOException {
    List<NestedField> fields = randomFields(random, 3);

    Schema schema = new Schema(StructType.of(fields).fields());

    TableMetadata metadata = icebergTableMetadata(random, fields, schema);

    fileWriter.writeFile("table-simple", "1", TableMetadataParser.toJson(metadata));
  }

  private static void icebergTableThreeSnapshots(Random random, FileWriter fileWriter)
      throws IOException {
    List<NestedField> fields = randomFields(random, 3);
    List<NestedField> fields2 =
        Stream.concat(fields.stream(), randomFields(random, fields.size() + 1, 3))
            .collect(Collectors.toList());
    List<NestedField> fields3 =
        Stream.concat(fields2.stream(), randomFields(random, fields2.size() + 1, 3))
            .collect(Collectors.toList());

    Schema schema = new Schema(StructType.of(fields).fields());
    Schema schema2 = new Schema(2, StructType.of(fields2).fields());
    Schema schema3 = new Schema(3, StructType.of(fields3).fields());

    TableMetadata metadata = icebergTableMetadata(random, fields, schema);

    metadata = TableMetadata.buildFrom(metadata).addSchema(schema2, fields2.size()).build();
    metadata = TableMetadata.buildFrom(metadata).addSchema(schema3, fields3.size()).build();

    fileWriter.writeFile("table-three-snapshots", "1", TableMetadataParser.toJson(metadata));

    Snapshot snapshot2 = randomSnapshot(random, metadata);
    metadata = TableMetadata.buildFrom(metadata).setCurrentSnapshot(snapshot2).build();

    fileWriter.writeFile("table-three-snapshots", "2", TableMetadataParser.toJson(metadata));

    Snapshot snapshot3 = randomSnapshot(random, metadata);
    metadata = TableMetadata.buildFrom(metadata).setCurrentSnapshot(snapshot3).build();

    fileWriter.writeFile("table-three-snapshots", "3", TableMetadataParser.toJson(metadata));
  }

  private static void cleanTargetDir(Path targetBaseDir) throws IOException {
    Files.walkFileTree(
        targetBaseDir,
        new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.TERMINATE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  @FunctionalInterface
  interface FileWriter {
    void writeFile(String scenario, String name, String json) throws IOException;
  }
}
