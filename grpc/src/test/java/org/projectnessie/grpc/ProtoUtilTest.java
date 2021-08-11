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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.grpc.ProtoUtil.fromProto;
import static org.projectnessie.grpc.ProtoUtil.refFromProto;
import static org.projectnessie.grpc.ProtoUtil.refToProto;
import static org.projectnessie.grpc.ProtoUtil.toProto;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.api.grpc.CommitLogRequest;
import org.projectnessie.api.grpc.CommitLogResponse;
import org.projectnessie.api.grpc.CommitOperation;
import org.projectnessie.api.grpc.CommitOps;
import org.projectnessie.api.grpc.ContentsRequest;
import org.projectnessie.api.grpc.EntriesRequest;
import org.projectnessie.api.grpc.EntriesResponse;
import org.projectnessie.api.grpc.MultipleContentsRequest;
import org.projectnessie.api.grpc.MultipleContentsResponse;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.Hash;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableEntry;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Operations;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.SqlView.Dialect;
import org.projectnessie.model.Tag;

public class ProtoUtilTest {

  @Test
  public void referenceConversion() {
    assertThatThrownBy(() -> refToProto(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Reference must be non-null");

    assertThatThrownBy(() -> refFromProto(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Reference must be non-null");

    Branch b = Branch.of("main", "1234567890123456");
    Tag t = Tag.of("tag", "1234567890123456");
    Hash h = Hash.of("1234567890123456");

    assertThat(refFromProto(refToProto(b))).isEqualTo(b);
    assertThat(refFromProto(refToProto(t))).isEqualTo(t);
    assertThat(refFromProto(refToProto(h))).isEqualTo(h);
  }

  @Test
  public void branchConversion() {
    assertThatThrownBy(() -> toProto((Branch) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch must be non-null");

    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.Branch) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch must be non-null");

    String branchName = "main";
    Branch b = Branch.of(branchName, "1234567890123456");
    assertThat(fromProto(toProto(b))).isEqualTo(b);

    Branch branchWithoutHash = Branch.of(branchName, null);
    assertThat(fromProto(toProto(branchWithoutHash))).isEqualTo(branchWithoutHash);
  }

  @Test
  public void tagConversion() {
    assertThatThrownBy(() -> toProto((Tag) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tag must be non-null");

    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.Tag) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tag must be non-null");

    String tagName = "main";
    Tag tag = Tag.of(tagName, "1234567890123456");
    assertThat(fromProto(toProto(tag))).isEqualTo(tag);

    Tag tagWithoutHash = Tag.of(tagName, null);
    assertThat(fromProto(toProto(tagWithoutHash))).isEqualTo(tagWithoutHash);
  }

  @Test
  public void hashConversion() {
    assertThatThrownBy(() -> toProto((Hash) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hash must be non-null");

    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.Hash) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hash must be non-null");

    String h = "1234567890123456";
    Hash hash = Hash.of(h);

    assertThat(fromProto(toProto(hash))).isEqualTo(hash);
  }

  @Test
  public void icebergTableConversion() {
    assertThatThrownBy(() -> toProto((IcebergTable) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("IcebergTable must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.IcebergTable) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("IcebergTable must be non-null");

    IcebergTable icebergTable = IcebergTable.of("test.me.txt");
    assertThat(fromProto(toProto(icebergTable))).isEqualTo(icebergTable);
  }

  @Test
  public void deltaLakeTableConversion() {
    assertThatThrownBy(() -> toProto((DeltaLakeTable) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DeltaLakeTable must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.DeltaLakeTable) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DeltaLakeTable must be non-null");

    DeltaLakeTable deltaLakeTable =
        ImmutableDeltaLakeTable.builder()
            .addMetadataLocationHistory("a", "b")
            .addCheckpointLocationHistory("c", "d")
            .lastCheckpoint("c")
            .build();

    assertThat(fromProto(toProto(deltaLakeTable))).isEqualTo(deltaLakeTable);

    DeltaLakeTable deltaLakeTableWithoutLastCheckpoint =
        ImmutableDeltaLakeTable.builder().from(deltaLakeTable).lastCheckpoint(null).build();
    assertThat(fromProto(toProto(deltaLakeTableWithoutLastCheckpoint)))
        .isEqualTo(deltaLakeTableWithoutLastCheckpoint);
  }

  @Test
  public void sqlViewConversion() {
    assertThatThrownBy(() -> toProto((SqlView) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("SqlView must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.SqlView) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("SqlView must be non-null");

    SqlView view =
        ImmutableSqlView.builder().sqlText("select * from X").dialect(Dialect.PRESTO).build();
    assertThat(fromProto(toProto(view))).isEqualTo(view);
  }

  @Test
  public void nessieConfigurationConversion() {
    assertThatThrownBy(() -> toProto((NessieConfiguration) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("NessieConfiguration must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.NessieConfiguration) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("NessieConfiguration must be non-null");

    NessieConfiguration config =
        ImmutableNessieConfiguration.builder().version("1.2.3").defaultBranch("main").build();
    assertThat(fromProto(toProto(config))).isEqualTo(config);
  }

  @Test
  public void contentsConversion() {
    assertThatThrownBy(() -> toProto((Contents) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Contents must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.Contents) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Contents must be non-null");

    IcebergTable icebergTable = IcebergTable.of("test.me.txt");
    assertThat(fromProto(toProto(icebergTable))).isEqualTo(icebergTable);
  }

  @Test
  public void contentsKeyConversion() {
    assertThatThrownBy(() -> toProto((ContentsKey) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ContentsKey must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.ContentsKey) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ContentsKey must be non-null");

    ContentsKey key = ContentsKey.of("a.b.c.txt");
    assertThat(fromProto(toProto(key))).isEqualTo(key);
  }

  @Test
  public void contentsWithKeyConversion() {
    assertThatThrownBy(() -> toProto((ContentsWithKey) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ContentsWithKey must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.ContentsWithKey) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ContentsWithKey must be non-null");

    ContentsKey key = ContentsKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt");
    ContentsWithKey c = ContentsWithKey.of(key, icebergTable);
    assertThat(fromProto(toProto(c))).isEqualTo(c);
  }

  @Test
  public void entryConversion() {
    assertThatThrownBy(() -> toProto((Entry) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Entry must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.Entry) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Entry must be non-null");

    Entry entry =
        ImmutableEntry.builder().name(ContentsKey.of("a.b.c.txt")).type(Type.ICEBERG_TABLE).build();
    assertThat(fromProto(toProto(entry))).isEqualTo(entry);
  }

  @Test
  public void commitMetaConversion() {
    assertThatThrownBy(() -> toProto((CommitMeta) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitMeta must be non-null");
    assertThatThrownBy(() -> fromProto((org.projectnessie.api.grpc.CommitMeta) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitMeta must be non-null");

    CommitMeta commitMeta =
        CommitMeta.builder()
            .author("eduard")
            .message("commit msg")
            .commitTime(Instant.now())
            .authorTime(Instant.now())
            .properties(ImmutableMap.of("a", "b"))
            .hash("1234567890123456")
            .signedOffBy("me")
            .build();
    assertThat(fromProto(toProto(commitMeta))).isEqualTo(commitMeta);

    CommitMeta minimalCommitMeta =
        CommitMeta.builder().message("commit msg").properties(ImmutableMap.of("a", "b")).build();
    assertThat(fromProto(toProto(minimalCommitMeta))).isEqualTo(minimalCommitMeta);
  }

  @Test
  public void instantConversion() {
    assertThatThrownBy(() -> toProto((Instant) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Timestamp must be non-null");

    assertThatThrownBy(() -> fromProto((Timestamp) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Timestamp must be non-null");

    Instant instant = Instant.now();
    assertThat(fromProto(toProto(instant))).isEqualTo(instant);
  }

  @Test
  public void operationConversion() {
    assertThatThrownBy(() -> toProto((Operation) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitOperation must be non-null");

    assertThatThrownBy(() -> fromProto((CommitOperation) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitOperation must be non-null");

    ContentsKey key = ContentsKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt");

    Put put = Operation.Put.of(key, icebergTable);
    Delete delete = Operation.Delete.of(key);
    Unchanged unchanged = Operation.Unchanged.of(key);

    assertThat(fromProto(toProto(put))).isEqualTo(put);
    assertThat(fromProto(toProto(delete))).isEqualTo(delete);
    assertThat(fromProto(toProto(unchanged))).isEqualTo(unchanged);
  }

  @Test
  public void operationsConversion() {
    assertThatThrownBy(() -> toProto((Operations) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitOperations must be non-null");

    assertThatThrownBy(() -> fromProto((CommitOps) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitOperations must be non-null");

    CommitMeta commitMeta =
        CommitMeta.builder()
            .author("eduard")
            .message("commit msg")
            .commitTime(Instant.now())
            .authorTime(Instant.now())
            .properties(ImmutableMap.of("a", "b"))
            .hash("1234567890123456")
            .signedOffBy("me")
            .build();

    ContentsKey key = ContentsKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt");

    Put put = Operation.Put.of(key, icebergTable);
    Delete delete = Operation.Delete.of(key);
    Unchanged unchanged = Operation.Unchanged.of(key);
    Operations commitOps =
        ImmutableOperations.builder()
            .commitMeta(commitMeta)
            .addOperations(put, delete, unchanged)
            .build();

    assertThat(fromProto(toProto(commitOps))).isEqualTo(commitOps);
  }

  @Test
  public void entriesRequestConversion() {
    assertThatThrownBy(() -> fromProto((EntriesRequest) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("EntriesRequest must be non-null");

    assertThatThrownBy(() -> toProto(null, EntriesParams.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("refName must be non-null");

    assertThatThrownBy(() -> toProto("main", (EntriesParams) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("EntriesParams must be non-null");

    EntriesParams params =
        EntriesParams.builder()
            .expression("a > b")
            .hashOnRef("123")
            .maxRecords(23)
            .pageToken("abc")
            .build();
    assertThat(fromProto(toProto("main", params))).isEqualTo(params);

    EntriesParams empty = EntriesParams.empty();
    assertThat(fromProto(toProto("main", empty))).isEqualTo(empty);
  }

  @Test
  public void commitLogRequestConversion() {
    assertThatThrownBy(() -> fromProto((CommitLogRequest) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitLogRequest must be non-null");

    assertThatThrownBy(() -> toProto(null, CommitLogParams.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("refName must be non-null");

    assertThatThrownBy(() -> toProto("main", (CommitLogParams) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitLogParams must be non-null");

    CommitLogParams params =
        CommitLogParams.builder()
            .expression("a > b")
            .startHash("123")
            .endHash("456")
            .maxRecords(23)
            .pageToken("abc")
            .build();
    assertThat(fromProto(toProto("main", params))).isEqualTo(params);

    CommitLogParams empty = CommitLogParams.empty();
    assertThat(fromProto(toProto("main", empty))).isEqualTo(empty);
  }

  @Test
  public void entriesResponseConversion() {
    assertThatThrownBy(() -> fromProto((EntriesResponse) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("EntriesResponse must be non-null");

    assertThatThrownBy(() -> toProto((org.projectnessie.model.EntriesResponse) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("EntriesResponse must be non-null");

    List<Entry> entries =
        Arrays.asList(
            ImmutableEntry.builder()
                .name(ContentsKey.of("a.b.c.txt"))
                .type(Type.ICEBERG_TABLE)
                .build(),
            ImmutableEntry.builder()
                .name(ContentsKey.of("a.b.d.txt"))
                .type(Type.DELTA_LAKE_TABLE)
                .build(),
            ImmutableEntry.builder().name(ContentsKey.of("a.b.e.txt")).type(Type.VIEW).build());
    org.projectnessie.model.EntriesResponse response =
        org.projectnessie.model.EntriesResponse.builder().entries(entries).build();
    assertThat(fromProto(toProto(response))).isEqualTo(response);

    org.projectnessie.model.EntriesResponse responseWithToken =
        org.projectnessie.model.EntriesResponse.builder().entries(entries).token("abc").build();
    assertThat(fromProto(toProto(responseWithToken))).isEqualTo(responseWithToken);
  }

  @Test
  public void commitLogResponseConversion() {
    assertThatThrownBy(() -> fromProto((CommitLogResponse) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitLogResponse must be non-null");

    assertThatThrownBy(() -> toProto((LogResponse) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CommitLogResponse must be non-null");

    List<CommitMeta> commits =
        Arrays.asList(
            CommitMeta.builder()
                .author("eduard")
                .message("commit msg")
                .commitTime(Instant.now())
                .authorTime(Instant.now())
                .properties(ImmutableMap.of("a", "b"))
                .signedOffBy("me")
                .build(),
            CommitMeta.builder()
                .message("commit msg2")
                .properties(ImmutableMap.of("a", "b"))
                .build());

    LogResponse logResponse = ImmutableLogResponse.builder().operations(commits).build();
    assertThat(fromProto(toProto(logResponse))).isEqualTo(logResponse);

    LogResponse logResponseWithToken =
        ImmutableLogResponse.builder().operations(commits).token("abc").build();
    assertThat(fromProto(toProto(logResponseWithToken))).isEqualTo(logResponseWithToken);
  }

  @Test
  public void contentsRequestConversion() {
    assertThatThrownBy(() -> toProto((ContentsKey) null, "ref", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ContentsKey must be non-null");

    ContentsKey key = ContentsKey.of("test.me.txt");
    assertThatThrownBy(() -> toProto(key, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ref must be non-null");

    String ref = "main";
    String hashOnRef = "x";
    ContentsRequest request = toProto(key, ref, null);
    assertThat(request.getContentsKey()).isEqualTo(toProto(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEmpty();

    request = toProto(key, ref, hashOnRef);
    assertThat(request.getContentsKey()).isEqualTo(toProto(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEqualTo(hashOnRef);
  }

  @Test
  public void multipleContentsRequestConversion() {
    assertThatThrownBy(() -> toProto(null, null, (MultiGetContentsRequest) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ref must be non-null");

    String ref = "main";
    String hashOnRef = "x";
    ContentsKey key = ContentsKey.of("test.me.txt");
    MultipleContentsRequest request = toProto(ref, null, MultiGetContentsRequest.of(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEmpty();
    assertThat(request.getRequestedKeysList()).containsExactly(toProto(key));

    request = toProto(ref, hashOnRef, MultiGetContentsRequest.of(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEqualTo(hashOnRef);
    assertThat(request.getRequestedKeysList()).containsExactly(toProto(key));
  }

  @Test
  public void multipleContentsResponseConversion() {
    assertThatThrownBy(() -> toProto((MultiGetContentsResponse) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("MultiGetContentsResponse must be non-null");

    assertThatThrownBy(() -> fromProto((MultipleContentsResponse) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("MultipleContentsResponse must be non-null");

    ContentsKey key = ContentsKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt");
    ContentsWithKey c = ContentsWithKey.of(key, icebergTable);

    MultiGetContentsResponse response = MultiGetContentsResponse.of(Collections.singletonList(c));
    assertThat(fromProto(toProto(response))).isEqualTo(response);
  }
}
