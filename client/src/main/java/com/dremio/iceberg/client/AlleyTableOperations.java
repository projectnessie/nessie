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

package com.dremio.iceberg.client;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.iceberg.client.tag.Tag;
import com.dremio.iceberg.model.DataFile;
import com.dremio.iceberg.model.Snapshot;
import com.dremio.iceberg.model.Table;
import com.google.common.base.Joiner;

public class AlleyTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlleyTableOperations.class);
  private static final Joiner DOT =Joiner.on(".");
  private final Configuration conf;
  private final AlleyClient client;
  private final Tag tag;
  private Table table;
  private HadoopFileIO fileIO;

  public AlleyTableOperations(Configuration conf, Table table, Tag tag) {
    this.conf = conf;
    this.table = table;
    this.tag = tag;
    client = new AlleyClient(conf);
  }

  private static TableIdentifier tableIdentifier(Table table) {
    return TableIdentifier.parse(DOT.join(table.getNamespace(), table.getTableName()));
  }

  @Override
  protected void doRefresh() {
    TableIdentifier tableIdentifier = tableIdentifier(table);
    String metadataLocation = tag.getMetadataLocation(tableIdentifier);

    refreshFromMetadataLocation(metadataLocation);
  }

  private Table modifiedTable(String metadataLocation, TableMetadata metadata) {
    Table modifiedTable = table.newMetadataLocation(metadataLocation);
    modifiedTable.setSchema(SchemaParser.toJson(metadata.schema()));
    modifiedTable.setSourceId(metadata.uuid());
    return modifiedTable;
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    try {
      if (table.getVersion() == null) {
        table = client.getTableClient().createObject(modifiedTable(newMetadataLocation, metadata));
      } else {
        Table modifiedTable = modifiedTable(newMetadataLocation, metadata);
        modifiedTable.addSnapshot(transformSnapshot(metadata.currentSnapshot()));
        client.getTableClient().updateObject(modifiedTable);
        table = client.getTableClient().getObject(table.getUuid());
        updateTag();
      }
    } catch (Throwable e) {
      io().deleteFile(newMetadataLocation);
      throw new CommitFailedException(e, "failed");
    }
  }

  private void updateTag() {
    if (tag == null) {
      return;
    }
    tag.updateTags().updateTable(tableIdentifier(table), this).commit();
  }

  private Snapshot transformSnapshot(org.apache.iceberg.Snapshot currentSnapshot) {
    if (currentSnapshot == null) {
      return null;
    }
    return new Snapshot(
      currentSnapshot.snapshotId(),
      currentSnapshot.parentId(),
      currentSnapshot.timestampMillis(),
      currentSnapshot.operation(),
      currentSnapshot.summary(),
      transformDataFiles(currentSnapshot.addedFiles()),
      transformDataFiles(currentSnapshot.deletedFiles()),
      currentSnapshot.manifestListLocation()
    );
  }

  private Iterable<DataFile> transformDataFiles(Iterable<org.apache.iceberg.DataFile> deletedFiles) {
    return StreamSupport.stream(deletedFiles.spliterator(), false).map(id -> new DataFile(
      id.path().toString(),
      id.format().name(),
      id.recordCount(),
      id.fileSizeInBytes(),
      id.fileOrdinal(),
      id.sortColumns(),
      id.columnSizes(),
      id.valueCounts(),
      id.nullValueCounts()
    )).collect(Collectors.toList());
  }

  public Tag currentTag() {
    return tag;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

  Table alleyTable() {
    return table;
  }

  String metadataLocation() {
    return tag.getMetadataLocation(tableIdentifier(table));
  }
}
