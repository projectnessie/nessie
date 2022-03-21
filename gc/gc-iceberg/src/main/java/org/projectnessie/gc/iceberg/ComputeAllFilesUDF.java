/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.gc.iceberg;

import java.util.List;
import org.apache.iceberg.GCMetadataUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.sql.api.java.UDF2;

class ComputeAllFilesUDF implements UDF2<String, Long, List<String>> {

  private final FileIO io;

  public ComputeAllFilesUDF(FileIO io) {
    this.io = io;
  }

  @Override
  public List<String> call(String metadataLocation, Long snapshotId) {
    TableMetadata metadata = TableMetadataParser.read(io, metadataLocation);
    Snapshot snapshot = metadata.snapshot(snapshotId);
    if (snapshot == null) {
      throw new RuntimeException(
          String.format(
              "Unable to find the snapshot %s in table metadata %s", snapshotId, metadataLocation));
    }
    return GCMetadataUtil.computeAllFiles(snapshot, io);
  }
}
