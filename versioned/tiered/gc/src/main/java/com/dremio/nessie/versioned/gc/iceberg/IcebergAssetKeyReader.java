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
package com.dremio.nessie.versioned.gc.iceberg;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.gc.assets.FileSystemAssetKey;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.iceberg.DataFileCollector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.versioned.AssetKey;
import com.dremio.nessie.versioned.gc.AssetKeyReader;
import org.apache.spark.util.SerializableConfiguration;

public class IcebergAssetKeyReader<T> implements AssetKeyReader {

  private final IcebergTable table;
  private final SerializableConfiguration hadoopConfig;

  public IcebergAssetKeyReader(IcebergTable table, SerializableConfiguration configuration) {
    this.table = table;
    hadoopConfig = configuration;
  }

  @Override
  public Stream<AssetKey> getKeys() {
    TableMetadata metadata = TableMetadataParser.read(new HadoopFileIO(hadoopConfig.value()), table.getMetadataLocation());
    return allFiles(metadata);
  }

  private Stream<AssetKey> allFiles(TableMetadata metadata) {
    List<AssetKey> metadataFiles = Lists.newArrayList();
    Set<ManifestFile> manifests = Sets.newHashSet();
    Snapshot snapshot = metadata.currentSnapshot();
    if (snapshot != null) {
      Iterables.addAll(manifests, snapshot.allManifests());
      if (snapshot.manifestListLocation() != null) {
        metadataFiles.add(new FileSystemAssetKey(snapshot.manifestListLocation(), hadoopConfig, AssetKeyType.ICEBERG_MANIFEST_LIST));
      }
    }
    manifests.stream().map(ManifestFile::path).map(x -> new FileSystemAssetKey(x, hadoopConfig, AssetKeyType.ICEBERG_MANIFEST)).forEach(metadataFiles::add);
    metadataFiles.add(new FileSystemAssetKey(metadata.metadataFileLocation(), hadoopConfig, AssetKeyType.ICEBERG_METADATA));
    metadataFiles.add(new FileSystemAssetKey(metadata.location(), hadoopConfig, AssetKeyType.TABLE));

    Stream<FileSystemAssetKey> files = DataFileCollector.dataFiles(new HadoopFileIO(hadoopConfig.value()), manifests).map(x -> new FileSystemAssetKey(x, hadoopConfig, AssetKeyType.DATA_FILE));
    List<FileSystemAssetKey> keys = files.collect(Collectors.toList());
    return Stream.concat(metadataFiles.stream(), keys.stream());
  }


}
