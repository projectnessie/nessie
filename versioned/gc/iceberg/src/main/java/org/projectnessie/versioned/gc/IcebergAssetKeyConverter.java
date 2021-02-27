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
package org.projectnessie.versioned.gc;

import java.io.Serializable;
import java.util.stream.Stream;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFileCollector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.spark.util.SerializableConfiguration;
import org.projectnessie.model.Contents;
import org.projectnessie.model.IcebergTable;

import com.google.common.base.Preconditions;


public class IcebergAssetKeyConverter implements AssetKeyConverter<Contents, IcebergAssetKey>, Serializable {

  private final SerializableConfiguration hadoopConfig;

  public IcebergAssetKeyConverter(SerializableConfiguration configuration) {
    hadoopConfig = configuration;
  }

  private Stream<IcebergAssetKey> allFiles(TableMetadata metadata) {
    //note the namespace is not stored here so we can't know for sure what the full table path is
    final String tableName = new Path(metadata.location()).getName();
    final long snapshotId = metadata.currentSnapshot() == null ? -1L : metadata.currentSnapshot().snapshotId();
    Stream<IcebergAssetKey> metadataFiles = Stream.of(
      new IcebergAssetKey(metadata.metadataFileLocation(), hadoopConfig, IcebergAssetKey.AssetKeyType.ICEBERG_METADATA, snapshotId,
          tableName), new IcebergAssetKey(metadata.location(), hadoopConfig, IcebergAssetKey.AssetKeyType.TABLE, 0, tableName));

    return Stream.concat(metadataFiles, metadata.snapshots().stream().flatMap(snapshot -> {
      final long thisSnapshotId = snapshot.snapshotId();

      Stream<IcebergAssetKey> manifestLists = Stream.of(new IcebergAssetKey(snapshot.manifestListLocation(), hadoopConfig,
          IcebergAssetKey.AssetKeyType.ICEBERG_MANIFEST_LIST, thisSnapshotId, tableName));

      Stream<IcebergAssetKey> manifestsStream = snapshot.allManifests().stream().map(ManifestFile::path)
          .map(x -> new IcebergAssetKey(x, hadoopConfig, IcebergAssetKey.AssetKeyType.ICEBERG_MANIFEST, thisSnapshotId, tableName));

      Stream<IcebergAssetKey> files = DataFileCollector.dataFiles(new HadoopFileIO(hadoopConfig.value()), snapshot.allManifests())
          .map(x -> new IcebergAssetKey(x, hadoopConfig, IcebergAssetKey.AssetKeyType.DATA_FILE, thisSnapshotId, tableName));
      return Stream.concat(Stream.concat(manifestsStream, files), manifestLists);
    }));
  }


  @Override
  public Stream<IcebergAssetKey> apply(Contents contents) {
    Preconditions.checkArgument(contents instanceof IcebergTable);
    TableMetadata metadata = TableMetadataParser.read(new HadoopFileIO(hadoopConfig.value()),
        ((IcebergTable) contents).getMetadataLocation());
    return allFiles(metadata);
  }

}
