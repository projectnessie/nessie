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

package com.dremio.nessie.jgit;

import com.dremio.nessie.jgit.GitStoreObjects.Counts;
import com.dremio.nessie.jgit.GitStoreObjects.DataFile;
import com.dremio.nessie.jgit.GitStoreObjects.Snapshot;
import com.dremio.nessie.jgit.GitStoreObjects.Table.Builder;
import com.dremio.nessie.model.ImmutableDataFile;
import com.dremio.nessie.model.ImmutableSnapshot;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.ImmutableTableMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.model.TableMeta;
import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ProtoUtil {

  private static final Joiner DOT = Joiner.on('.');

  /**
   * turn a {@link com.dremio.nessie.model.Table} into a protobuf object.
   *
   * @param table Table to persist
   * @param id optional ObjectId of the metadata
   * @return protobuf container
   */
  public static GitStoreObjects.Table tableToProtoc(Table table, String id) {
    Builder builder = GitStoreObjects.Table.newBuilder()
                                           .setPath(table.getId())
                                           .setMetadataLocation(table.getMetadataLocation());
    if (id != null) {
      builder.setMetadata(id);
    }
    return builder.build();
  }

  /**
   * convert a {@link com.dremio.nessie.model.TableMeta} object into protobuf.
   *
   * @param metadata metadata to store
   * @return serialized protobuf bytes
   */
  public static byte[] convertToProtoc(TableMeta metadata) {
    if (metadata == null) {
      return null;
    }
    GitStoreObjects.TableMeta tm = GitStoreObjects.TableMeta.newBuilder()
                                                            .setSchema(metadata.getSchema())
                                                            .setSourceId(metadata.getSourceId())
                                                            .addAllSnapshots(convertToProtoc(
                                                              metadata.getSnapshots()))
                                                            .build();
    return tm.toByteArray();
  }

  private static Iterable<? extends Snapshot> convertToProtoc(
      List<com.dremio.nessie.model.Snapshot> snapshots) {
    List<Snapshot> convertedSnapshot = new ArrayList<>();
    if (snapshots.isEmpty()) {
      return convertedSnapshot;
    }
    for (com.dremio.nessie.model.Snapshot snapshot : snapshots) {
      convertedSnapshot.add(GitStoreObjects.Snapshot.newBuilder()
                                                    .setSnapshotId(snapshot.getSnapshotId())
                                                    .setManifestLocation(
                                                      snapshot.getManifestLocation())
                                                    .setOperation(snapshot.getOperation())
                                                    .setParentId(
                                                      snapshot.getParentId() == null ? -1L
                                                        : snapshot.getParentId())
                                                    .setTimestampMillis(
                                                      snapshot.getTimestampMillis())
                                                    .addAllDataFiles(
                                                      convertToProtoc(snapshot.getAddedFiles(),
                                                                      snapshot.getDeletedFiles()))
                                                    .putAllSummary(snapshot.getSummary())
                                                    .build());
    }
    return convertedSnapshot;
  }

  private static Iterable<? extends DataFile> convertToProtoc(
      List<com.dremio.nessie.model.DataFile> addedFiles,
      List<com.dremio.nessie.model.DataFile> deletedFiles) {
    List<DataFile> dataFiles = new ArrayList<>();
    for (com.dremio.nessie.model.DataFile dataFile : addedFiles) {
      put(dataFiles, dataFile, false);

    }
    for (com.dremio.nessie.model.DataFile dataFile : deletedFiles) {
      put(dataFiles, dataFile, true);
    }
    return dataFiles;
  }

  private static Map<Integer, Counts> convertToProtoc(Map<Integer, Long> columnSizes,
                                                      Map<Integer, Long> valueCounts,
                                                      Map<Integer, Long> nullValueCounts) {
    Map<Integer, Counts> counts = new HashMap<>();
    if (columnSizes == null) {
      return counts;
    }
    for (int i : columnSizes.keySet()) {
      counts.put(i, Counts.newBuilder()
                          .setNullValueCount(nullValueCounts.get(i))
                          .setValueCount(valueCounts.get(i))
                          .setColumnSize(columnSizes.get(i))
                          .build());
    }
    return counts;
  }


  private static void put(List<DataFile> dataFiles,
                          com.dremio.nessie.model.DataFile dataFile,
                          boolean deleted) {
    DataFile.Builder builder = DataFile.newBuilder();
    builder.setDelete(deleted)
           .setFileFormat(dataFile.getFileFormat())
           .setOrdinal(dataFile.getOrdinal() == null ? -1 : dataFile.getOrdinal())
           .setFileSizeInBytes(dataFile.getFileSizeInBytes())
           .setPath(dataFile.getPath())
           .setRecordCount(dataFile.getRecordCount());
    if (dataFile.getSortColumns() != null) {
      builder.addAllSortColumns(dataFile.getSortColumns());
    }
    if (dataFile.getColumnSizes() != null) {
      builder.putAllTableStats(convertToProtoc(dataFile.getColumnSizes(),
                                               dataFile.getValueCounts(),
                                               dataFile.getNullValueCounts()));
    }
    dataFiles.add(builder.build());
  }

  /**
   * Turn a protobuf serialized buffer into a Table object.
   *
   * @param data raw bytes
   * @return Table object and optionally the reference to the metadata object
   */
  public static Map.Entry<Table, String> tableFromBytes(byte[] data) {
    try {
      GitStoreObjects.Table table = GitStoreObjects.Table.parseFrom(data);
      String[] names = table.getPath().split("\\.");
      String namespace = null;
      if (names.length > 1) {
        namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
      }
      String name = names[names.length - 1];
      return new SimpleImmutableEntry<>(ImmutableTable.builder()
                                                      .id(table.getPath())
                                                      .metadataLocation(table.getMetadataLocation())
                                                      .name(name)
                                                      .namespace(namespace)
                                                      .build(),
                                        table.getMetadata());

    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * convert serialized table metadata into model object.
   */
  public static TableMeta convertToModel(byte[] data) {
    try {
      GitStoreObjects.TableMeta metadata = GitStoreObjects.TableMeta.parseFrom(data);
      return ImmutableTableMeta.builder().addAllSnapshots(
        convertToModel(metadata.getSnapshotsList()))
                               .schema(metadata.getSchema())
                               .sourceId(metadata.getSourceId())
                               .build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static Iterable<? extends com.dremio.nessie.model.Snapshot> convertToModel(
      List<Snapshot> snapshotsList) {
    List<com.dremio.nessie.model.Snapshot> snapshots = new ArrayList<>();
    for (Snapshot snapshot : snapshotsList) {
      List<com.dremio.nessie.model.DataFile> added =
          convertDataFileToModel(snapshot.getDataFilesList()
                                       .stream()
                                       .filter(x -> !x.getDelete())
                                       .collect(
                                         Collectors.toList()));
      List<com.dremio.nessie.model.DataFile> removed =
          convertDataFileToModel(snapshot.getDataFilesList()
                                       .stream()
                                       .filter(DataFile::getDelete)
                                       .collect(
                                         Collectors.toList()));
      snapshots.add(ImmutableSnapshot.builder().manifestLocation(snapshot.getManifestLocation())
                                     .operation(snapshot.getOperation())
                                     .parentId(snapshot.getParentId())
                                     .snapshotId(snapshot.getSnapshotId())
                                     .timestampMillis(snapshot.getTimestampMillis())
                                     .summary(snapshot.getSummaryMap())
                                     .addAllAddedFiles(added)
                                     .addAllDeletedFiles(removed)
                                     .build());
    }
    return snapshots;
  }

  private static List<com.dremio.nessie.model.DataFile> convertDataFileToModel(
      List<DataFile> collect) {
    List<com.dremio.nessie.model.DataFile> dataFiles = new ArrayList<>();
    for (DataFile dataFile : collect) {
      dataFiles.add(ImmutableDataFile.builder()
                                     .fileFormat(dataFile.getFileFormat())
                                     .fileSizeInBytes(dataFile.getFileSizeInBytes())
                                     .ordinal(dataFile.getOrdinal())
                                     .path(dataFile.getPath())
                                     .recordCount(dataFile.getRecordCount())
                                     .sortColumns(dataFile.getSortColumnsList())
                                     .putAllColumnSizes(
                                       toMap(dataFile.getTableStatsMap(), Counts::getColumnSize))
                                     .putAllNullValueCounts(
                                       toMap(dataFile.getTableStatsMap(),
                                             Counts::getNullValueCount))
                                     .putAllValueCounts(
                                       toMap(dataFile.getTableStatsMap(), Counts::getValueCount))
                                     .build());
    }
    return dataFiles;
  }

  private static Map<Integer, Long> toMap(Map<Integer, Counts> in, Function<Counts, Long> mapper) {
    return in.entrySet().stream()
             .collect(Collectors.toMap(Entry::getKey, x -> mapper.apply(x.getValue())));
  }
}
