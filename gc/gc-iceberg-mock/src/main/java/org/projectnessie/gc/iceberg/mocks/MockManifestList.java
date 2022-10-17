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
package org.projectnessie.gc.iceberg.mocks;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockManifestList {

  public abstract List<MockManifestFile> manifestFiles();

  static final Schema V2_SCHEMA =
      new Schema(
          ManifestFile.PATH,
          ManifestFile.LENGTH,
          ManifestFile.SPEC_ID,
          ManifestFile.MANIFEST_CONTENT.asRequired(),
          ManifestFile.SEQUENCE_NUMBER.asRequired(),
          ManifestFile.MIN_SEQUENCE_NUMBER.asRequired(),
          ManifestFile.SNAPSHOT_ID.asRequired(),
          ManifestFile.ADDED_FILES_COUNT.asRequired(),
          ManifestFile.EXISTING_FILES_COUNT.asRequired(),
          ManifestFile.DELETED_FILES_COUNT.asRequired(),
          ManifestFile.ADDED_ROWS_COUNT.asRequired(),
          ManifestFile.EXISTING_ROWS_COUNT.asRequired(),
          ManifestFile.DELETED_ROWS_COUNT.asRequired(),
          ManifestFile.PARTITION_SUMMARIES);

  public void write(
      OutputFile output, long snapshotId, long parentSnapshotId, long sequenceNumber) {
    try {
      try (FileAppender<Object> writer =
          Avro.write(output)
              .schema(V2_SCHEMA)
              .named("manifest_file")
              .meta(
                  ImmutableMap.of(
                      "snapshot-id", String.valueOf(snapshotId),
                      "parent-snapshot-id", String.valueOf(parentSnapshotId),
                      "sequence-number", String.valueOf(sequenceNumber),
                      "format-version", "2"))
              .overwrite()
              .build()) {

        for (MockManifestFile manifestFile : manifestFiles()) {
          writer.add(manifestFile);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
