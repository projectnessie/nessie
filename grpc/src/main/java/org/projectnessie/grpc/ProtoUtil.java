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

import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.stream.Collectors;
import org.projectnessie.api.grpc.CommitLogRequest;
import org.projectnessie.api.grpc.CommitLogResponse;
import org.projectnessie.api.grpc.CommitOperation;
import org.projectnessie.api.grpc.CommitOps;
import org.projectnessie.api.grpc.Contents;
import org.projectnessie.api.grpc.ContentsRequest;
import org.projectnessie.api.grpc.ContentsType;
import org.projectnessie.api.grpc.DeltaLakeTable.Builder;
import org.projectnessie.api.grpc.Dialect;
import org.projectnessie.api.grpc.EntriesRequest;
import org.projectnessie.api.grpc.MultipleContentsRequest;
import org.projectnessie.api.grpc.MultipleContentsResponse;
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
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDelete;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.ImmutableUnchanged;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
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
    Preconditions.checkArgument(null != ref, "Reference must be non-null");
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
    Preconditions.checkArgument(null != ref, "Reference must be non-null");
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
    Preconditions.checkArgument(null != branch, "Branch must be non-null");
    return Branch.of(branch.getName(), "".equals(branch.getHash()) ? null : branch.getHash());
  }

  public static org.projectnessie.api.grpc.Branch toProto(Branch branch) {
    Preconditions.checkArgument(null != branch, "Branch must be non-null");
    org.projectnessie.api.grpc.Branch.Builder builder =
        org.projectnessie.api.grpc.Branch.newBuilder().setName(branch.getName());
    if (null != branch.getHash()) builder.setHash(branch.getHash());
    return builder.build();
  }

  public static Tag fromProto(org.projectnessie.api.grpc.Tag tag) {
    Preconditions.checkArgument(null != tag, "Tag must be non-null");
    return Tag.of(tag.getName(), "".equals(tag.getHash()) ? null : tag.getHash());
  }

  public static org.projectnessie.api.grpc.Tag toProto(Tag tag) {
    Preconditions.checkArgument(null != tag, "Tag must be non-null");
    org.projectnessie.api.grpc.Tag.Builder builder =
        org.projectnessie.api.grpc.Tag.newBuilder().setName(tag.getName());
    if (null != tag.getHash()) builder.setHash(tag.getHash());
    return builder.build();
  }

  public static Hash fromProto(org.projectnessie.api.grpc.Hash hash) {
    Preconditions.checkArgument(null != hash, "Hash must be non-null");
    return Hash.of(hash.getHash());
  }

  public static org.projectnessie.api.grpc.Hash toProto(Hash hash) {
    Preconditions.checkArgument(null != hash, "Hash must be non-null");
    return org.projectnessie.api.grpc.Hash.newBuilder().setHash(hash.getHash()).build();
  }

  public static org.projectnessie.api.grpc.Contents toProto(org.projectnessie.model.Contents obj) {
    Preconditions.checkArgument(null != obj, "Contents must be non-null");
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
    throw new IllegalArgumentException(
        String.format("'%s' must be an IcebergTable/DeltaLakeTable/SqlView", obj));
  }

  public static org.projectnessie.model.Contents fromProto(Contents obj) {
    Preconditions.checkArgument(null != obj, "Contents must be non-null");
    if (obj.hasIceberg()) {
      return fromProto(obj.getIceberg());
    }
    if (obj.hasDeltaLake()) {
      return fromProto(obj.getDeltaLake());
    }
    if (obj.hasView()) {
      return fromProto(obj.getView());
    }
    throw new IllegalArgumentException(
        String.format("'%s' must be an IcebergTable/DeltaLakeTable/SqlView", obj));
  }

  public static DeltaLakeTable fromProto(org.projectnessie.api.grpc.DeltaLakeTable deltaLakeTable) {
    Preconditions.checkArgument(null != deltaLakeTable, "DeltaLakeTable must be non-null");
    ImmutableDeltaLakeTable.Builder builder =
        ImmutableDeltaLakeTable.builder()
            .id(deltaLakeTable.getId())
            .checkpointLocationHistory(deltaLakeTable.getCheckpointLocationHistoryList())
            .metadataLocationHistory(deltaLakeTable.getMetadataLocationHistoryList());
    if (!"".equals(deltaLakeTable.getLastCheckpoint()))
      builder.lastCheckpoint(deltaLakeTable.getLastCheckpoint());
    return builder.build();
  }

  public static org.projectnessie.api.grpc.DeltaLakeTable toProto(DeltaLakeTable deltaLakeTable) {
    Preconditions.checkArgument(null != deltaLakeTable, "DeltaLakeTable must be non-null");
    Builder builder =
        org.projectnessie.api.grpc.DeltaLakeTable.newBuilder().setId(deltaLakeTable.getId());
    if (null != deltaLakeTable.getLastCheckpoint())
      builder.setLastCheckpoint(deltaLakeTable.getLastCheckpoint());
    deltaLakeTable.getCheckpointLocationHistory().forEach(builder::addCheckpointLocationHistory);
    deltaLakeTable.getMetadataLocationHistory().forEach(builder::addMetadataLocationHistory);
    return builder.build();
  }

  public static IcebergTable fromProto(org.projectnessie.api.grpc.IcebergTable icebergTable) {
    Preconditions.checkArgument(null != icebergTable, "IcebergTable must be non-null");
    return ImmutableIcebergTable.builder()
        .id(icebergTable.getId())
        .metadataLocation(icebergTable.getMetadataLocation())
        .snapshotId(icebergTable.getSnapshotId())
        .build();
  }

  public static org.projectnessie.api.grpc.IcebergTable toProto(IcebergTable icebergTable) {
    Preconditions.checkArgument(null != icebergTable, "IcebergTable must be non-null");
    return org.projectnessie.api.grpc.IcebergTable.newBuilder()
        .setId(icebergTable.getId())
        .setMetadataLocation(icebergTable.getMetadataLocation())
        .setSnapshotId(icebergTable.getSnapshotId())
        .build();
  }

  public static SqlView fromProto(org.projectnessie.api.grpc.SqlView view) {
    Preconditions.checkArgument(null != view, "SqlView must be non-null");
    return ImmutableSqlView.builder()
        .id(view.getId())
        .sqlText(view.getSqlText())
        .dialect(SqlView.Dialect.valueOf(view.getDialect().name()))
        .build();
  }

  public static org.projectnessie.api.grpc.SqlView toProto(SqlView view) {
    Preconditions.checkArgument(null != view, "SqlView must be non-null");
    return org.projectnessie.api.grpc.SqlView.newBuilder()
        .setId(view.getId())
        .setDialect(Dialect.valueOf(view.getDialect().name()))
        .setSqlText(view.getSqlText())
        .build();
  }

  public static NessieConfiguration toProto(org.projectnessie.model.NessieConfiguration config) {
    Preconditions.checkArgument(null != config, "NessieConfiguration must be non-null");
    return NessieConfiguration.newBuilder()
        .setDefaultBranch(config.getDefaultBranch())
        .setVersion(config.getVersion())
        .build();
  }

  public static org.projectnessie.model.NessieConfiguration fromProto(NessieConfiguration config) {
    Preconditions.checkArgument(null != config, "NessieConfiguration must be non-null");
    return ImmutableNessieConfiguration.builder()
        .defaultBranch(config.getDefaultBranch())
        .version(config.getVersion())
        .build();
  }

  public static org.projectnessie.api.grpc.ContentsKey toProto(ContentsKey key) {
    Preconditions.checkArgument(null != key, "ContentsKey must be non-null");
    return org.projectnessie.api.grpc.ContentsKey.newBuilder()
        .addAllElements(key.getElements())
        .build();
  }

  public static ContentsKey fromProto(org.projectnessie.api.grpc.ContentsKey key) {
    Preconditions.checkArgument(null != key, "ContentsKey must be non-null");
    return ContentsKey.of(key.getElementsList());
  }

  public static ContentsWithKey fromProto(org.projectnessie.api.grpc.ContentsWithKey c) {
    Preconditions.checkArgument(null != c, "ContentsWithKey must be non-null");
    return ContentsWithKey.of(fromProto(c.getContentsKey()), fromProto(c.getContents()));
  }

  public static org.projectnessie.api.grpc.ContentsWithKey toProto(ContentsWithKey c) {
    Preconditions.checkArgument(null != c, "ContentsWithKey must be non-null");
    return org.projectnessie.api.grpc.ContentsWithKey.newBuilder()
        .setContentsKey(toProto(c.getKey()))
        .setContents(toProto(c.getContents()))
        .build();
  }

  public static org.projectnessie.api.grpc.Entry toProto(Entry entry) {
    Preconditions.checkArgument(null != entry, "Entry must be non-null");
    return org.projectnessie.api.grpc.Entry.newBuilder()
        .setContentsKey(toProto(entry.getName()))
        .setType(ContentsType.valueOf(entry.getType().name()))
        .build();
  }

  public static Entry fromProto(org.projectnessie.api.grpc.Entry entry) {
    Preconditions.checkArgument(null != entry, "Entry must be non-null");
    return Entry.builder()
        .type(Type.valueOf(entry.getType().name()))
        .name(fromProto(entry.getContentsKey()))
        .build();
  }

  public static org.projectnessie.api.grpc.CommitMeta toProto(CommitMeta commitMeta) {
    Preconditions.checkArgument(null != commitMeta, "CommitMeta must be non-null");
    // note that if the committer/author/commitmsg is an empty string, then it won't be included in
    // the GRPC commit meta
    org.projectnessie.api.grpc.CommitMeta.Builder builder =
        org.projectnessie.api.grpc.CommitMeta.newBuilder();
    if (null != commitMeta.getAuthor()) builder.setAuthor(commitMeta.getAuthor());
    if (null != commitMeta.getCommitter()) builder.setCommitter(commitMeta.getCommitter());
    if (null != commitMeta.getCommitTime())
      builder.setCommitTime(toProto(commitMeta.getCommitTime()));
    if (null != commitMeta.getAuthorTime())
      builder.setAuthorTime(toProto(commitMeta.getAuthorTime()));
    if (null != commitMeta.getHash()) builder.setHash(commitMeta.getHash());
    if (null != commitMeta.getSignedOffBy()) builder.setSignedOffBy(commitMeta.getSignedOffBy());
    return builder
        .setMessage(commitMeta.getMessage())
        .putAllProperties(commitMeta.getProperties())
        .build();
  }

  public static CommitMeta fromProto(org.projectnessie.api.grpc.CommitMeta commitMeta) {
    Preconditions.checkArgument(null != commitMeta, "CommitMeta must be non-null");
    // we can't set the committer here as it's set on the server-side and there's a check that
    // prevents this field from being set
    ImmutableCommitMeta.Builder builder = CommitMeta.builder();
    if (!"".equals(commitMeta.getAuthor())) builder.author(commitMeta.getAuthor());
    if (!"".equals(commitMeta.getHash())) builder.hash(commitMeta.getHash());
    if (!"".equals(commitMeta.getSignedOffBy())) builder.signedOffBy(commitMeta.getSignedOffBy());
    if (!"".equals(commitMeta.getCommitTime().toString()))
      builder.commitTime(fromProto(commitMeta.getCommitTime()));
    if (!"".equals(commitMeta.getAuthorTime().toString()))
      builder.authorTime(fromProto(commitMeta.getAuthorTime()));
    return builder
        .message(commitMeta.getMessage())
        .properties(commitMeta.getPropertiesMap())
        .build();
  }

  public static Timestamp toProto(Instant timestamp) {
    Preconditions.checkArgument(null != timestamp, "Timestamp must be non-null");
    return Timestamp.newBuilder()
        .setSeconds(timestamp.getEpochSecond())
        .setNanos(timestamp.getNano())
        .build();
  }

  public static Instant fromProto(Timestamp timestamp) {
    Preconditions.checkArgument(null != timestamp, "Timestamp must be non-null");
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  public static CommitOperation toProto(Operation op) {
    Preconditions.checkArgument(null != op, "CommitOperation must be non-null");
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
    throw new IllegalArgumentException("CommitOperation should be Put/Delete/Unchanged");
  }

  public static Operation fromProto(CommitOperation op) {
    Preconditions.checkArgument(null != op, "CommitOperation must be non-null");
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
    throw new IllegalArgumentException("CommitOperation should be Put/Delete/Unchanged");
  }

  public static Operations fromProto(CommitOps ops) {
    Preconditions.checkArgument(null != ops, "CommitOperations must be non-null");
    return ImmutableOperations.builder()
        .commitMeta(fromProto(ops.getCommitMeta()))
        .operations(
            ops.getOperationsList().stream().map(ProtoUtil::fromProto).collect(Collectors.toList()))
        .build();
  }

  public static CommitOps toProto(Operations operations) {
    Preconditions.checkArgument(null != operations, "CommitOperations must be non-null");
    CommitOps.Builder builder =
        CommitOps.newBuilder().setCommitMeta(toProto(operations.getCommitMeta()));
    operations.getOperations().stream().map(ProtoUtil::toProto).forEach(builder::addOperations);
    return builder.build();
  }

  public static EntriesParams fromProto(EntriesRequest request) {
    Preconditions.checkArgument(null != request, "EntriesRequest must be non-null");
    EntriesParams.Builder builder = EntriesParams.builder();
    if (!"".equals(request.getHashOnRef())) builder.hashOnRef(request.getHashOnRef());
    if (!"".equals(request.getQueryExpression())) builder.expression(request.getQueryExpression());
    if (!"".equals(request.getPageToken())) builder.pageToken(request.getPageToken());
    if (0 != request.getMaxRecords()) builder.maxRecords(request.getMaxRecords());
    if (0 != request.getNamespaceDepth()) builder.namespaceDepth(request.getNamespaceDepth());
    return builder.build();
  }

  public static EntriesRequest toProto(String refName, EntriesParams params) {
    Preconditions.checkArgument(null != refName, "refName must be non-null");
    Preconditions.checkArgument(null != params, "EntriesParams must be non-null");
    EntriesRequest.Builder builder = EntriesRequest.newBuilder().setNamedRef(refName);
    if (null != params.hashOnRef()) builder.setHashOnRef(params.hashOnRef());
    if (null != params.maxRecords()) builder.setMaxRecords(params.maxRecords());
    if (null != params.pageToken()) builder.setPageToken(params.pageToken());
    if (null != params.queryExpression()) builder.setQueryExpression(params.queryExpression());
    if (null != params.namespaceDepth()) builder.setNamespaceDepth(params.namespaceDepth());
    return builder.build();
  }

  public static CommitLogParams fromProto(CommitLogRequest request) {
    Preconditions.checkArgument(null != request, "CommitLogRequest must be non-null");
    CommitLogParams.Builder builder = CommitLogParams.builder();
    if (!"".equals(request.getStartHash())) builder.startHash(request.getStartHash());
    if (!"".equals(request.getEndHash())) builder.endHash(request.getEndHash());
    if (!"".equals(request.getQueryExpression())) builder.expression(request.getQueryExpression());
    if (!"".equals(request.getPageToken())) builder.pageToken(request.getPageToken());
    if (0 != request.getMaxRecords()) builder.maxRecords(request.getMaxRecords());
    return builder.build();
  }

  public static CommitLogRequest toProto(String refName, CommitLogParams params) {
    Preconditions.checkArgument(null != refName, "refName must be non-null");
    Preconditions.checkArgument(null != params, "CommitLogParams must be non-null");
    CommitLogRequest.Builder builder = CommitLogRequest.newBuilder().setNamedRef(refName);
    if (null != params.startHash()) builder.setStartHash(params.startHash());
    if (null != params.endHash()) builder.setEndHash(params.endHash());
    if (null != params.maxRecords()) builder.setMaxRecords(params.maxRecords());
    if (null != params.pageToken()) builder.setPageToken(params.pageToken());
    if (null != params.queryExpression()) builder.setQueryExpression(params.queryExpression());
    return builder.build();
  }

  public static LogResponse fromProto(CommitLogResponse commitLog) {
    Preconditions.checkArgument(null != commitLog, "CommitLogResponse must be non-null");
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
    Preconditions.checkArgument(null != commitLog, "CommitLogResponse must be non-null");
    CommitLogResponse.Builder builder =
        CommitLogResponse.newBuilder().setHasMore(commitLog.hasMore());
    commitLog.getOperations().forEach(c -> builder.addOperations(toProto(c)));
    if (null != commitLog.getToken()) builder.setToken(commitLog.getToken());
    return builder.build();
  }

  public static EntriesResponse fromProto(org.projectnessie.api.grpc.EntriesResponse entries) {
    Preconditions.checkArgument(null != entries, "EntriesResponse must be non-null");
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
    Preconditions.checkArgument(null != entries, "EntriesResponse must be non-null");
    org.projectnessie.api.grpc.EntriesResponse.Builder builder =
        org.projectnessie.api.grpc.EntriesResponse.newBuilder().setHasMore(entries.hasMore());
    entries.getEntries().forEach(e -> builder.addEntries(toProto(e)));
    if (null != entries.getToken()) builder.setToken(entries.getToken());
    return builder.build();
  }

  public static ContentsRequest toProto(ContentsKey key, String ref, String hashOnRef) {
    Preconditions.checkArgument(null != ref, "ref must be non-null");
    ContentsRequest.Builder builder =
        ContentsRequest.newBuilder().setContentsKey(toProto(key)).setRef(ref);
    builder = null != hashOnRef ? builder.setHashOnRef(hashOnRef) : builder;
    return builder.build();
  }

  public static MultipleContentsRequest toProto(
      String ref, String hashOnRef, MultiGetContentsRequest request) {
    Preconditions.checkArgument(null != ref, "ref must be non-null");
    final MultipleContentsRequest.Builder builder =
        MultipleContentsRequest.newBuilder().setRef(ref);
    if (null != hashOnRef) builder.setHashOnRef(hashOnRef);
    if (null != request)
      request.getRequestedKeys().forEach(k -> builder.addRequestedKeys(toProto(k)));

    return builder.build();
  }

  public static MultipleContentsResponse toProto(MultiGetContentsResponse response) {
    Preconditions.checkArgument(null != response, "MultiGetContentsResponse must be non-null");
    MultipleContentsResponse.Builder builder = MultipleContentsResponse.newBuilder();
    response.getContents().forEach(c -> builder.addContentsWithKey(toProto(c)));
    return builder.build();
  }

  public static MultiGetContentsResponse fromProto(MultipleContentsResponse response) {
    Preconditions.checkArgument(null != response, "MultipleContentsResponse must be non-null");
    return MultiGetContentsResponse.of(
        response.getContentsWithKeyList().stream()
            .map(ProtoUtil::fromProto)
            .collect(Collectors.toList()));
  }
}
