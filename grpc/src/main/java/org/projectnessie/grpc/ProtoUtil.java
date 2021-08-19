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
package org.projectnessie.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.stream.Collectors;
import org.projectnessie.api.grpc.CommitLogRequest;
import org.projectnessie.api.grpc.CommitLogResponse;
import org.projectnessie.api.grpc.CommitOperation;
import org.projectnessie.api.grpc.CommitOps;
import org.projectnessie.api.grpc.Contents;
import org.projectnessie.api.grpc.ContentsType;
import org.projectnessie.api.grpc.DeltaLakeTable.Builder;
import org.projectnessie.api.grpc.Dialect;
import org.projectnessie.api.grpc.EntriesRequest;
import org.projectnessie.api.grpc.NessieConfiguration;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.Hash;
import org.projectnessie.model.HiveDatabase;
import org.projectnessie.model.HiveTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDelete;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableHiveDatabase;
import org.projectnessie.model.ImmutableHiveTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.ImmutableUnchanged;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.Tag;

/** A simple utility class that translates between Protobuf classes and Nessie model classes. */
public class ProtoUtil {

  public static Reference refFromProto(org.projectnessie.api.grpc.Reference ref) {
    if (ref.hasBranch()) {
      return fromProto(ref.getBranch());
    }
    if (ref.hasTag()) {
      return fromProto(ref.getTag());
    }
    if (ref.hasHash()) {
      return fromProto(ref.getHash());
    }
    throw new IllegalArgumentException(String.format("'%s' should be a Branch/Tag/Hash", ref));
  }

  public static org.projectnessie.api.grpc.Reference refToProto(Reference ref) {
    if (ref instanceof Branch) {
      return org.projectnessie.api.grpc.Reference.newBuilder()
          .setBranch(toProto((Branch) ref))
          .build();
    }
    if (ref instanceof Tag) {
      return org.projectnessie.api.grpc.Reference.newBuilder().setTag(toProto((Tag) ref)).build();
    }
    if (ref instanceof Hash) {
      return org.projectnessie.api.grpc.Reference.newBuilder().setHash(toProto((Hash) ref)).build();
    }
    throw new IllegalArgumentException(String.format("'%s' should be a Branch/Tag/Hash", ref));
  }

  public static Branch fromProto(org.projectnessie.api.grpc.Branch branch) {
    return Branch.of(branch.getName(), "".equals(branch.getHash()) ? null : branch.getHash());
  }

  public static org.projectnessie.api.grpc.Branch toProto(Branch branch) {
    org.projectnessie.api.grpc.Branch.Builder builder =
        org.projectnessie.api.grpc.Branch.newBuilder().setName(branch.getName());
    if (null != branch.getHash()) builder.setHash(branch.getHash());
    return builder.build();
  }

  public static Tag fromProto(org.projectnessie.api.grpc.Tag tag) {
    return Tag.of(tag.getName(), "".equals(tag.getHash()) ? null : tag.getHash());
  }

  public static org.projectnessie.api.grpc.Tag toProto(Tag tag) {
    org.projectnessie.api.grpc.Tag.Builder builder =
        org.projectnessie.api.grpc.Tag.newBuilder().setName(tag.getName());
    if (null != tag.getHash()) builder.setHash(tag.getHash());
    return builder.build();
  }

  public static Hash fromProto(org.projectnessie.api.grpc.Hash hash) {
    return Hash.of(hash.getHash());
  }

  public static org.projectnessie.api.grpc.Hash toProto(Hash hash) {
    return org.projectnessie.api.grpc.Hash.newBuilder().setHash(hash.getHash()).build();
  }

  public static org.projectnessie.api.grpc.Contents toProto(org.projectnessie.model.Contents obj) {
    if (obj instanceof IcebergTable) {
      IcebergTable iceberg = (IcebergTable) obj;
      return Contents.newBuilder().setIceberg(toProto(iceberg)).build();
    }
    if (obj instanceof DeltaLakeTable) {
      return Contents.newBuilder()
          .setDeltaLake(toProto((org.projectnessie.model.DeltaLakeTable) obj))
          .build();
    }
    if (obj instanceof SqlView) {
      return Contents.newBuilder().setView(toProto((SqlView) obj)).build();
    }
    if (obj instanceof HiveDatabase) {
      return Contents.newBuilder().setHiveDb(toProto((HiveDatabase) obj)).build();
    }
    if (obj instanceof HiveTable) {
      return Contents.newBuilder().setHive(toProto((HiveTable) obj)).build();
    }
    throw new IllegalArgumentException(
        String.format(
            "'%s' must be an IcebergTable/DeltaLakeTable/SqlView/HiveDatabase/HiveTable", obj));
  }

  public static org.projectnessie.model.Contents fromProto(Contents obj) {
    if (obj.hasIceberg()) {
      return fromProto(obj.getIceberg());
    }
    if (obj.hasDeltaLake()) {
      return fromProto(obj.getDeltaLake());
    }
    if (obj.hasView()) {
      return fromProto(obj.getView());
    }
    if (obj.hasHiveDb()) {
      return fromProto(obj.getHiveDb());
    }
    if (obj.hasHive()) {
      return fromProto(obj.getHive());
    }

    throw new IllegalArgumentException(
        String.format(
            "'%s' must be an IcebergTable/DeltaLakeTable/SqlView/HiveDatabase/HiveTable", obj));
  }

  public static DeltaLakeTable fromProto(org.projectnessie.api.grpc.DeltaLakeTable deltaLakeTable) {
    return ImmutableDeltaLakeTable.builder()
        .id(deltaLakeTable.getId())
        .checkpointLocationHistory(deltaLakeTable.getCheckpointLocationHistoryList())
        .lastCheckpoint(deltaLakeTable.getLastCheckpoint())
        .metadataLocationHistory(deltaLakeTable.getMetadataLocationHistoryList())
        .build();
  }

  public static org.projectnessie.api.grpc.DeltaLakeTable toProto(DeltaLakeTable deltaLakeTable) {
    Builder builder =
        org.projectnessie.api.grpc.DeltaLakeTable.newBuilder().setId(deltaLakeTable.getId());
    if (null != deltaLakeTable.getLastCheckpoint())
      builder.setLastCheckpoint(deltaLakeTable.getLastCheckpoint());
    deltaLakeTable.getCheckpointLocationHistory().forEach(builder::addCheckpointLocationHistory);
    deltaLakeTable.getMetadataLocationHistory().forEach(builder::addMetadataLocationHistory);
    return builder.build();
  }

  private static ImmutableIcebergTable fromProto(
      org.projectnessie.api.grpc.IcebergTable icebergTable) {
    return ImmutableIcebergTable.builder()
        .id(icebergTable.getId())
        .metadataLocation(icebergTable.getMetadataLocation())
        .build();
  }

  public static org.projectnessie.api.grpc.IcebergTable toProto(IcebergTable iceberg) {
    return org.projectnessie.api.grpc.IcebergTable.newBuilder()
        .setId(iceberg.getId())
        .setMetadataLocation(iceberg.getMetadataLocation())
        .build();
  }

  public static SqlView fromProto(org.projectnessie.api.grpc.SqlView view) {
    return ImmutableSqlView.builder()
        .id(view.getId())
        .sqlText(view.getSqlText())
        .dialect(SqlView.Dialect.valueOf(view.getDialect().name()))
        .build();
  }

  public static org.projectnessie.api.grpc.SqlView toProto(SqlView view) {
    return org.projectnessie.api.grpc.SqlView.newBuilder()
        .setId(view.getId())
        .setDialect(Dialect.valueOf(view.getDialect().name()))
        .setSqlText(view.getSqlText())
        .build();
  }

  public static HiveDatabase fromProto(org.projectnessie.api.grpc.HiveDatabase hiveDatabase) {
    return ImmutableHiveDatabase.builder()
        .id(hiveDatabase.getId())
        .databaseDefinition(hiveDatabase.getDatabaseDefinition().toByteArray())
        .build();
  }

  public static org.projectnessie.api.grpc.HiveDatabase toProto(HiveDatabase hiveDatabase) {
    return org.projectnessie.api.grpc.HiveDatabase.newBuilder()
        .setId(hiveDatabase.getId())
        .setDatabaseDefinition(ByteString.copyFrom(hiveDatabase.getDatabaseDefinition()))
        .build();
  }

  public static HiveTable fromProto(org.projectnessie.api.grpc.HiveTable hiveTable) {
    return ImmutableHiveTable.builder()
        .id(hiveTable.getId())
        .partitions(
            hiveTable.getPartitionsList().stream()
                .map(ByteString::toByteArray)
                .collect(Collectors.toList()))
        .tableDefinition(hiveTable.getTableDefinition().toByteArray())
        .build();
  }

  public static org.projectnessie.api.grpc.HiveTable toProto(HiveTable hiveTable) {
    org.projectnessie.api.grpc.HiveTable.Builder builder =
        org.projectnessie.api.grpc.HiveTable.newBuilder()
            .setId(hiveTable.getId())
            .setTableDefinition(ByteString.copyFrom(hiveTable.getTableDefinition()));
    hiveTable.getPartitions().forEach(p -> builder.addPartitions(ByteString.copyFrom(p)));
    return builder.build();
  }

  public static NessieConfiguration fromProto(org.projectnessie.model.NessieConfiguration config) {
    return NessieConfiguration.newBuilder()
        .setDefaultBranch(config.getDefaultBranch())
        .setVersion(config.getVersion())
        .build();
  }

  public static org.projectnessie.api.grpc.ContentsKey toProto(ContentsKey key) {
    return org.projectnessie.api.grpc.ContentsKey.newBuilder()
        .addAllElements(key.getElements())
        .build();
  }

  public static ContentsKey fromProto(org.projectnessie.api.grpc.ContentsKey key) {
    return ContentsKey.of(key.getElementsList());
  }

  public static ContentsWithKey fromProto(org.projectnessie.api.grpc.ContentsWithKey c) {
    return ContentsWithKey.of(fromProto(c.getContentsKey()), fromProto(c.getContents()));
  }

  public static org.projectnessie.api.grpc.ContentsWithKey toProto(ContentsWithKey c) {
    return org.projectnessie.api.grpc.ContentsWithKey.newBuilder()
        .setContentsKey(ProtoUtil.toProto(c.getKey()))
        .setContents(ProtoUtil.toProto(c.getContents()))
        .build();
  }

  public static org.projectnessie.api.grpc.Entry toProto(Entry entry) {
    return org.projectnessie.api.grpc.Entry.newBuilder()
        .setContentsKey(toProto(entry.getName()))
        .setType(ContentsType.valueOf(entry.getType().name()))
        .build();
  }

  public static Entry fromProto(org.projectnessie.api.grpc.Entry entry) {
    return Entry.builder()
        .type(Type.valueOf(entry.getType().name()))
        .name(fromProto(entry.getContentsKey()))
        .build();
  }

  public static org.projectnessie.api.grpc.CommitMeta toProto(CommitMeta commitMeta) {
    org.projectnessie.api.grpc.CommitMeta.Builder builder =
        org.projectnessie.api.grpc.CommitMeta.newBuilder();
    if (null != commitMeta.getAuthor()) builder.setAuthor(commitMeta.getAuthor());
    // TODO: if the committer is an empty string, then it won't be included in the GRPC commit meta
    if (null != commitMeta.getCommitter()) builder.setCommitter(commitMeta.getCommitter());
    if (null != commitMeta.getCommitTime())
      builder.setCommitTime(
          Timestamp.newBuilder()
              .setSeconds(commitMeta.getCommitTime().getEpochSecond())
              .setNanos(commitMeta.getCommitTime().getNano())
              .build());
    if (null != commitMeta.getAuthorTime())
      builder.setAuthorTime(
          Timestamp.newBuilder()
              .setSeconds(commitMeta.getAuthorTime().getEpochSecond())
              .setNanos(commitMeta.getAuthorTime().getNano())
              .build());
    if (null != commitMeta.getHash()) builder.setHash(commitMeta.getHash());
    builder.putAllProperties(commitMeta.getProperties());
    return builder.setMessage(commitMeta.getMessage()).build();
  }

  public static CommitMeta fromProto(org.projectnessie.api.grpc.CommitMeta commitMeta) {
    return CommitMeta.builder()
        .author(commitMeta.getAuthor())
        // .committer(commitMeta.getCommitter())
        .hash(commitMeta.getHash())
        .message(commitMeta.getMessage())
        .properties(commitMeta.getPropertiesMap())
        .commitTime(
            Instant.ofEpochSecond(
                commitMeta.getCommitTime().getSeconds(), commitMeta.getCommitTime().getNanos()))
        .authorTime(
            Instant.ofEpochSecond(
                commitMeta.getAuthorTime().getSeconds(), commitMeta.getAuthorTime().getNanos()))
        .build();
  }

  public static CommitOperation toProto(Operation op) {
    if (op instanceof Put) {
      return CommitOperation.newBuilder()
          .setPut(
              org.projectnessie.api.grpc.Put.newBuilder()
                  .setKey(toProto(op.getKey()))
                  .setContents(toProto(((Put) op).getContents()))
                  .build())
          .build();
    }
    if (op instanceof Delete) {
      return CommitOperation.newBuilder()
          .setDelete(
              org.projectnessie.api.grpc.Delete.newBuilder().setKey(toProto(op.getKey())).build())
          .build();
    }
    if (op instanceof Unchanged) {
      return CommitOperation.newBuilder()
          .setUnchanged(
              org.projectnessie.api.grpc.Unchanged.newBuilder()
                  .setKey(toProto(op.getKey()))
                  .build())
          .build();
    }
    return null;
  }

  public static Operation fromProto(CommitOperation op) {
    if (op.hasPut()) {
      return ImmutablePut.builder()
          .key(fromProto(op.getPut().getKey()))
          .contents(fromProto(op.getPut().getContents()))
          .build();
    }
    if (op.hasDelete()) {
      return ImmutableDelete.builder().key(fromProto(op.getDelete().getKey())).build();
    }
    if (op.hasUnchanged()) {
      return ImmutableUnchanged.builder().key(fromProto(op.getUnchanged().getKey())).build();
    }
    throw new IllegalArgumentException("Op should be Put/Delete/Unchanged");
  }

  public static Operations fromProto(CommitOps ops) {
    return ImmutableOperations.builder()
        .commitMeta(fromProto(ops.getCommitMeta()))
        .operations(
            ops.getOperationsList().stream().map(ProtoUtil::fromProto).collect(Collectors.toList()))
        .build();
  }

  public static CommitOps toProto(Operations operations) {
    CommitOps.Builder builder =
        CommitOps.newBuilder().setCommitMeta(toProto(operations.getCommitMeta()));
    operations.getOperations().stream().map(ProtoUtil::toProto).forEach(builder::addOperations);
    return builder.build();
  }

  public static EntriesParams fromProto(EntriesRequest request) {
    EntriesParams.Builder builder = EntriesParams.builder();
    if (!"".equals(request.getHashOnRef())) builder.hashOnRef(request.getHashOnRef());
    if (!"".equals(request.getQueryExpression())) builder.expression(request.getQueryExpression());
    if (!"".equals(request.getPageToken())) builder.pageToken(request.getPageToken());
    return builder.maxRecords(request.getMaxRecords()).build();
  }

  public static EntriesRequest toProto(String refName, EntriesParams params) {
    EntriesRequest.Builder builder = EntriesRequest.newBuilder().setNamedRef(refName);
    if (null != params.hashOnRef()) builder.setHashOnRef(params.hashOnRef());
    if (null != params.maxRecords()) builder.setMaxRecords(params.maxRecords());
    if (null != params.pageToken()) builder.setPageToken(params.pageToken());
    if (null != params.queryExpression()) builder.setQueryExpression(params.queryExpression());
    return builder.build();
  }

  public static CommitLogParams fromProto(CommitLogRequest request) {
    CommitLogParams.Builder builder = CommitLogParams.builder();
    if (!"".equals(request.getStartHash())) builder.startHash(request.getStartHash());
    if (!"".equals(request.getEndHash())) builder.endHash(request.getEndHash());
    if (!"".equals(request.getQueryExpression())) builder.expression(request.getQueryExpression());
    if (!"".equals(request.getPageToken())) builder.pageToken(request.getPageToken());
    if (request.getMaxRecords() > 0) builder.maxRecords(request.getMaxRecords());
    return builder.build();
  }

  public static CommitLogRequest toProto(String refName, CommitLogParams params) {
    CommitLogRequest.Builder builder = CommitLogRequest.newBuilder().setNamedRef(refName);
    if (null != params.startHash()) builder.setStartHash(params.startHash());
    if (null != params.endHash()) builder.setEndHash(params.endHash());
    if (null != params.maxRecords()) builder.setMaxRecords(params.maxRecords());
    if (null != params.pageToken()) builder.setPageToken(params.pageToken());
    if (null != params.queryExpression()) builder.setQueryExpression(params.queryExpression());
    return builder.build();
  }

  public static LogResponse fromProto(CommitLogResponse commitLog) {
    ImmutableLogResponse.Builder builder =
        ImmutableLogResponse.builder()
            .addAllOperations(
                commitLog.getOperationsList().stream()
                    .map(ProtoUtil::fromProto)
                    .collect(Collectors.toList()))
            .hasMore(commitLog.getHasMore());
    if (!"".equals(commitLog.getToken())) builder.token(commitLog.getToken());
    return builder.build();
  }

  public static CommitLogResponse toProto(LogResponse commitLog) {
    CommitLogResponse.Builder builder =
        CommitLogResponse.newBuilder().setHasMore(commitLog.hasMore());
    commitLog.getOperations().forEach(c -> builder.addOperations(ProtoUtil.toProto(c)));
    if (null != commitLog.getToken()) builder.setToken(commitLog.getToken());
    return builder.build();
  }

  public static EntriesResponse fromProto(org.projectnessie.api.grpc.EntriesResponse entries) {
    ImmutableEntriesResponse.Builder builder =
        EntriesResponse.builder()
            .entries(
                entries.getEntriesList().stream()
                    .map(ProtoUtil::fromProto)
                    .collect(Collectors.toList()))
            .hasMore(entries.getHasMore());
    if (!"".equals(entries.getToken())) builder.token(entries.getToken());
    return builder.build();
  }

  public static org.projectnessie.api.grpc.EntriesResponse toProto(
      org.projectnessie.model.EntriesResponse entries) {
    org.projectnessie.api.grpc.EntriesResponse.Builder builder =
        org.projectnessie.api.grpc.EntriesResponse.newBuilder().setHasMore(entries.hasMore());
    entries.getEntries().forEach(e -> builder.addEntries(toProto(e)));
    if (null != entries.getToken()) builder.setToken(entries.getToken());
    return builder.build();
  }
}
