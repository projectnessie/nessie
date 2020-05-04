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

package com.dremio.nessie.client;

import com.dremio.nessie.client.branch.Branch;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.DataFile;
import com.dremio.nessie.model.ImmutableBranchTable;
import com.dremio.nessie.model.ImmutableDataFile;
import com.google.common.base.Joiner;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie implementation of Iceberg TableOperations.
 */
public class NessieTableOperations extends BaseMetastoreTableOperations {
  private static final Logger logger = LoggerFactory.getLogger(NessieTableOperations.class);

  private static final Joiner DOT = Joiner.on(".");
  private final Configuration conf;
  private final NessieClient client;
  private final Branch branch;
  private BranchTable table;
  private HadoopFileIO fileIO;

  public NessieTableOperations(Configuration conf,
                               BranchTable table,
                               Branch branch,
                               NessieClient client) {
    this.conf = conf;
    this.table = table;
    this.branch = branch;
    this.client = client;
  }

  private static TableIdentifier tableIdentifier(BranchTable table) {
    if (table.getNamespace() != null) {
      return TableIdentifier.parse(DOT.join(table.getNamespace(), table.getTableName()));
    } else {
      return TableIdentifier.parse(table.getTableName());
    }
  }

  @Override
  protected void doRefresh() {
    branch.refresh();
    TableIdentifier tableIdentifier = tableIdentifier(table);
    String metadataLocation = branch.getMetadataLocation(tableIdentifier);
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  private ImmutableBranchTable.Builder modifiedTable(String metadataLocation,
                                                     TableMetadata metadata) {
    return ImmutableBranchTable.builder().from(table)
                         .metadataLocation(metadataLocation)
                         .sourceId(metadata.uuid());
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    try {
      ImmutableBranchTable.Builder modifiedTable = modifiedTable(newMetadataLocation, metadata);
      transformSnapshot(metadata.currentSnapshot(), modifiedTable);
      client.commit(branch, modifiedTable.build());
    } catch (Throwable e) {
      io().deleteFile(newMetadataLocation);
      throw new CommitFailedException(e, "failed");
    }
  }

  private void transformSnapshot(org.apache.iceberg.Snapshot currentSnapshot,
                                     ImmutableBranchTable.Builder modifiedTable) {
    if (currentSnapshot == null) {
      return;
    }
    //modifiedTable.addSnapshots(ImmutableSnapshot.builder()
    //                        .snapshotId(currentSnapshot.snapshotId())
    //                        .parentId(currentSnapshot.parentId())
    //                        .timestampMillis(currentSnapshot.timestampMillis())
    //                        .operation(currentSnapshot.operation())
    //                        .summary(currentSnapshot.summary())
    //                        .addedFiles(transformDataFiles(currentSnapshot.addedFiles()))
    //                        .deletedFiles(transformDataFiles(currentSnapshot.deletedFiles()))
    //                        .manifestLocation(currentSnapshot.manifestListLocation())
    //                        .build());
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

  public Branch currentBranch() {
    return branch;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

  public BranchTable alleyTable() {
    return table;
  }

  String metadataLocation() {
    return branch.getMetadataLocation(tableIdentifier(table));
  }
}
