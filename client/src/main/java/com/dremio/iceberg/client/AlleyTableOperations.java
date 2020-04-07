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

import com.dremio.iceberg.client.tag.Tag;
import com.dremio.iceberg.model.DataFile;
import com.dremio.iceberg.model.ImmutableDataFile;
import com.dremio.iceberg.model.ImmutableSnapshot;
import com.dremio.iceberg.model.ImmutableTable;
import com.dremio.iceberg.model.ImmutableTable.Builder;
import com.dremio.iceberg.model.Table;
import com.google.common.base.Joiner;
import java.util.List;
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

/**
 * Iceberg Alley implementation of Iceberg TableOperations.
 */
public class AlleyTableOperations extends BaseMetastoreTableOperations {
  private static final Logger logger = LoggerFactory.getLogger(AlleyTableOperations.class);

  private static final Joiner DOT = Joiner.on(".");
  private final Configuration conf;
  private final AlleyClient client;
  private final Tag tag;
  private Table table;
  private HadoopFileIO fileIO;

  public AlleyTableOperations(Configuration conf, Table table, Tag tag, AlleyClient client) {
    this.conf = conf;
    this.table = table;
    this.tag = tag;
    this.client = client;
  }

  private static TableIdentifier tableIdentifier(Table table) {
    if (table.getNamespace() != null) {
      return TableIdentifier.parse(DOT.join(table.getNamespace(), table.getTableName()));
    } else {
      return TableIdentifier.parse(table.getTableName());
    }
  }

  @Override
  protected void doRefresh() {
    TableIdentifier tableIdentifier = tableIdentifier(table);
    String metadataLocation = tag.getMetadataLocation(tableIdentifier);

    refreshFromMetadataLocation(metadataLocation);
  }

  private ImmutableTable.Builder modifiedTable(String metadataLocation, TableMetadata metadata) {
    return ImmutableTable.builder().from(table)
                         .metadataLocation(metadataLocation)
                         .sourceId(metadata.uuid())
                         .schema(SchemaParser.toJson(metadata.schema()));
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    try {
      if (table.getVersionList().isEmpty()) {
        table = client.getTableClient()
                      .createObject(modifiedTable(newMetadataLocation, metadata).build());
      } else {
        ImmutableTable.Builder modifiedTable = modifiedTable(newMetadataLocation, metadata);
        transformSnapshot(metadata.currentSnapshot(), modifiedTable);
        client.getTableClient().updateObject(modifiedTable.build());
        table = client.getTableClient().getObject(table.getId());
        updateTag();
      }
    } catch (Throwable e) {
      io().deleteFile(newMetadataLocation);
      throw new CommitFailedException(e, "failed");
    }
  }

  private void updateTag() {
    logger.error("tag is {}", tag);
    if (tag == null) {
      return;
    }
    tag.updateTags().updateTable(tableIdentifier(table), this).commit();
  }

  private void transformSnapshot(org.apache.iceberg.Snapshot currentSnapshot,
                                     Builder modifiedTable) {
    if (currentSnapshot == null) {
      return;
    }
    modifiedTable.addSnapshots(ImmutableSnapshot.builder()
                            .snapshotId(currentSnapshot.snapshotId())
                            .parentId(currentSnapshot.parentId())
                            .timestampMillis(currentSnapshot.timestampMillis())
                            .operation(currentSnapshot.operation())
                            .summary(currentSnapshot.summary())
                            .addedFiles(transformDataFiles(currentSnapshot.addedFiles()))
                            .deletedFiles(transformDataFiles(currentSnapshot.deletedFiles()))
                            .manifestLocation(currentSnapshot.manifestListLocation())
                            .build());
  }

  private List<DataFile> transformDataFiles(Iterable<org.apache.iceberg.DataFile> deletedFiles) {
    return StreamSupport.stream(deletedFiles.spliterator(), false)
                        .map(id -> ImmutableDataFile.builder()
                                                    .path(id.path().toString())
                                                    .fileFormat(id.format().name())
                                                    .recordCount(id.recordCount())
                                                    .fileSizeInBytes(id.fileSizeInBytes())
                                                    .ordinal(id.fileOrdinal())
                                                    .sortColumns(id.sortColumns())
                                                    .columnSizes(id.columnSizes())
                                                    .valueCounts(id.valueCounts())
                                                    .nullValueCounts(id.nullValueCounts())
                                                    .build()
                        )
                        .collect(Collectors.toList());
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

  public Table alleyTable() {
    return table;
  }

  String metadataLocation() {
    return tag.getMetadataLocation(tableIdentifier(table));
  }
}
