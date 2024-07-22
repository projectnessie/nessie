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
package org.projectnessie.gc.iceberg.inttest;

import static java.util.Collections.emptySet;
import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import jakarta.annotation.Nonnull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.expire.ContentToFiles;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.iceberg.IcebergContentToFiles;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.spark.extensions.SparkSqlTestBase;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Verifies that {@link IcebergContentToFiles} against data ingested via "real" Iceberg yields the
 * correct manifest-lists, manifest-files and data-files, the same files as returned by Iceberg's
 * {@code .snapshot}, {@code .manifests} and {@code .files} "meta tables".
 */
@ExtendWith(SoftAssertionsExtension.class)
public class ITContentToFilesCrossCheck extends SparkSqlTestBase {
  @InjectSoftAssertions SoftAssertions soft;

  @TempDir Path tempDir;

  @Override
  protected String warehouseURI() {
    return tempDir.toUri().toString();
  }

  @Test
  public void partitionedTable() throws Exception {

    Path tableBaseDir = tempDir.resolve("tables/foo");
    Files.createDirectories(tableBaseDir);

    spark.sql(
        "CREATE TABLE nessie.foo (part_by bigint, some_data string) "
            + "USING iceberg "
            + "PARTITIONED BY (part_by) "
            + "LOCATION '"
            + tableBaseDir.toUri()
            + "'");

    IcebergContentToFiles contentToFiles =
        IcebergContentToFiles.builder().io(new HadoopFileIO(new Configuration())).build();

    for (int val = 0; val < 10; val++) {
      StringBuilder sql = new StringBuilder("INSERT INTO nessie.foo (part_by, some_data) VALUES ");
      for (int part = 0; part < 10; part++) {
        for (int row = 0; row < 10; row++) {
          if (row > 0 || part > 0) {
            sql.append(", ");
          }
          sql.append(String.format("(%d, 'part_%d_val_%d_row_%d')", part, part, val, row));
        }
      }
      spark.sql(sql.toString());
    }

    for (ContentReference contentReference : contentReferencesFromCommitLog()) {
      Set<String> filesFromIceberg = getFilesFromIceberg(contentReference);
      Set<String> extractedFiles = getExtractedFiles(contentToFiles, contentReference);
      soft.assertThat(extractedFiles)
          .containsAll(filesFromIceberg)
          .contains(contentReference.metadataLocation());
    }

    // Delete 5 partitions - then check again

    for (int part = 0; part < 5; part++) {
      spark.sql(String.format("DELETE FROM nessie.foo WHERE part_by = %d", part));
    }

    for (ContentReference contentReference : contentReferencesFromCommitLog()) {
      Set<String> filesFromIceberg = getFilesFromIceberg(contentReference);
      Set<String> extractedFiles = getExtractedFiles(contentToFiles, contentReference);
      soft.assertThat(extractedFiles)
          .containsAll(filesFromIceberg)
          .contains(contentReference.metadataLocation());
    }

    // Some point deletes - then check again

    for (int part = 5; part < 10; part++) {
      spark.sql(
          String.format(
              "DELETE FROM nessie.foo WHERE part_by = %d AND some_data = 'part_%d_val_1_row_1'",
              part, part));
      spark.sql(
          String.format(
              "DELETE FROM nessie.foo WHERE part_by = %d AND some_data > 'part_%d_val_5'",
              part, part));
    }

    for (ContentReference contentReference : contentReferencesFromCommitLog()) {
      Set<String> filesFromIceberg = getFilesFromIceberg(contentReference);
      Set<String> extractedFiles = getExtractedFiles(contentToFiles, contentReference);
      soft.assertThat(extractedFiles)
          .containsAll(filesFromIceberg)
          .contains(contentReference.metadataLocation());
    }
  }

  private static Set<String> getExtractedFiles(
      ContentToFiles contentToFiles, ContentReference contentReference) {
    try (Stream<FileReference> extracted = contentToFiles.extractFiles(contentReference)) {
      return extracted
          .map(FileReference::absolutePath)
          .map(StorageUri::location)
          .collect(Collectors.toSet());
    }
  }

  private static Set<String> getFilesFromIceberg(ContentReference contentReference) {
    long snap = Objects.requireNonNull(contentReference.snapshotId());
    return snap == -1L
        ? emptySet()
        : spark
            .sql(
                String.format(
                    "SELECT path FROM ("
                        + "  SELECT manifest_list AS path FROM nessie.foo.snapshots WHERE snapshot_id = %d"
                        + "  UNION"
                        + "  SELECT path FROM nessie.foo.manifests VERSION AS OF %d"
                        + "  UNION"
                        + "  SELECT file_path AS path FROM nessie.foo.files VERSION AS OF %d"
                        + ")",
                    snap, snap, snap))
            .collectAsList()
            .stream()
            .map(row -> row.getString(0))
            .collect(Collectors.toSet());
  }

  @Nonnull
  private List<ContentReference> contentReferencesFromCommitLog() throws NessieNotFoundException {
    return api.getCommitLog().refName(defaultBranch()).fetch(FetchOption.ALL).stream()
        .map(LogEntry::getOperations)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .map(Put.class::cast)
        .map(Put::getContent)
        .map(IcebergTable.class::cast)
        .map(
            table ->
                icebergContent(
                    ICEBERG_TABLE,
                    table.getId(),
                    "12345678",
                    ContentKey.of("foo"),
                    table.getMetadataLocation(),
                    table.getSnapshotId()))
        .collect(Collectors.toList());
  }
}
