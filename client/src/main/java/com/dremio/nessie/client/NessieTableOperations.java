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

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.DataFile;
import com.dremio.nessie.model.ImmutableDataFile;
import com.dremio.nessie.model.ImmutableSnapshot;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.ImmutableTable.Builder;
import com.dremio.nessie.model.ImmutableTableMeta;
import com.dremio.nessie.model.Table;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
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

  private final Configuration conf;
  private final NessieClient client;
  private final TableIdentifier tableIdentifier;
  private AtomicReference<Branch> branch;
  private Table table;
  private HadoopFileIO fileIO;

  public NessieTableOperations(Configuration conf,
                               TableIdentifier table,
                               AtomicReference<Branch> branch,
                               NessieClient client) {
    this.conf = conf;
    this.tableIdentifier = table;
    this.branch = branch;
    this.client = client;
  }

  @Override
  protected void doRefresh() {
    Branch newBranch = client.getBranch(branch.get().getName());
    branch.set(newBranch);
    String metadataLocation = Optional.ofNullable(table(true))
                                      .map(Table::getMetadataLocation)
                                      .orElse(null);
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  private ImmutableTable.Builder modifiedTable(String metadataLocation) {
    Table table = table(false);
    Builder builder = ImmutableTable.builder();
    if (table != null) {
      builder.from(table);
    } else {
      builder.namespace(
        tableIdentifier.hasNamespace() ? tableIdentifier.namespace().toString() : null)
             .tableName(tableIdentifier.name())
             .id(tableIdentifier.toString());
    }
    return builder.metadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    try {
      ImmutableTable.Builder modifiedTable = modifiedTable(newMetadataLocation);
      transformSnapshot(metadata, modifiedTable);
      client.commit(branch.get(), modifiedTable.build());
    } catch (Throwable e) {
      io().deleteFile(newMetadataLocation);
      throw new CommitFailedException(e, "failed");
    }
  }

  private void transformSnapshot(TableMetadata metadata,
                                 ImmutableTable.Builder modifiedTable) {
    if (metadata == null) {
      return;
    }
    ImmutableTableMeta.Builder builder = ImmutableTableMeta.builder()
                                                           .sourceId(metadata.uuid())
                                                           .schema(SchemaParser.toJson(
                                                             metadata.schema()));
    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot != null) {
      builder.addSnapshots(ImmutableSnapshot.builder()
                                            .snapshotId(currentSnapshot.snapshotId())
                                            .parentId(currentSnapshot.parentId())
                                            .timestampMillis(currentSnapshot.timestampMillis())
                                            .operation(currentSnapshot.operation())
                                            .summary(currentSnapshot.summary())
                                            .addedFiles(transformDataFiles(
                                              currentSnapshot.addedFiles()))
                                            .deletedFiles(transformDataFiles(
                                              currentSnapshot.deletedFiles()))
                                            .manifestLocation(
                                              currentSnapshot.manifestListLocation())
                                            .build());
    }
    modifiedTable.metadata(builder.build());
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

  private Table table(boolean refresh) {
    if (table == null || refresh) {
      table = client.getTable(branch.get().getName(),
                              tableIdentifier.name(),
                              tableIdentifier.hasNamespace() ? tableIdentifier.namespace()
                                                                              .toString() : null);
    }
    return table;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

}
