/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.cassandra.serializers;

import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.cassandra.CassandraSerde;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.HeaderEntry;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Headers;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripe;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripes;

public class CommitObjSerializer extends ObjSerializer<CommitObj> {

  private static final CqlColumn COL_COMMIT_CREATED =
      new CqlColumn("c_created", CqlColumnType.BIGINT);
  private static final CqlColumn COL_COMMIT_SEQ = new CqlColumn("c_seq", CqlColumnType.BIGINT);
  private static final CqlColumn COL_COMMIT_MESSAGE =
      new CqlColumn("c_message", CqlColumnType.VARCHAR);
  private static final CqlColumn COL_COMMIT_HEADERS =
      new CqlColumn("c_headers", CqlColumnType.VARBINARY);
  private static final CqlColumn COL_COMMIT_REFERENCE_INDEX =
      new CqlColumn("c_reference_index", CqlColumnType.OBJ_ID);
  private static final CqlColumn COL_COMMIT_REFERENCE_INDEX_STRIPES =
      new CqlColumn("c_reference_index_stripes", CqlColumnType.VARBINARY);
  private static final CqlColumn COL_COMMIT_TAIL =
      new CqlColumn("c_tail", CqlColumnType.OBJ_ID_LIST);
  private static final CqlColumn COL_COMMIT_SECONDARY_PARENTS =
      new CqlColumn("c_secondary_parents", CqlColumnType.OBJ_ID_LIST);
  private static final CqlColumn COL_COMMIT_INCREMENTAL_INDEX =
      new CqlColumn("c_incremental_index", CqlColumnType.VARBINARY);
  private static final CqlColumn COL_COMMIT_INCOMPLETE_INDEX =
      new CqlColumn("c_incomplete_index", CqlColumnType.BOOL);
  private static final CqlColumn COL_COMMIT_TYPE =
      new CqlColumn("c_commit_type", CqlColumnType.NAME);

  private static final Set<CqlColumn> COLS =
      ImmutableSet.<CqlColumn>builder()
          .add(COL_COMMIT_CREATED)
          .add(COL_COMMIT_SEQ)
          .add(COL_COMMIT_MESSAGE)
          .add(COL_COMMIT_HEADERS)
          .add(COL_COMMIT_REFERENCE_INDEX)
          .add(COL_COMMIT_REFERENCE_INDEX_STRIPES)
          .add(COL_COMMIT_TAIL)
          .add(COL_COMMIT_SECONDARY_PARENTS)
          .add(COL_COMMIT_INCREMENTAL_INDEX)
          .add(COL_COMMIT_INCOMPLETE_INDEX)
          .add(COL_COMMIT_TYPE)
          .build();

  public static final ObjSerializer<CommitObj> INSTANCE = new CommitObjSerializer();

  private CommitObjSerializer() {
    super(COLS);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void serialize(
      CommitObj obj,
      BoundStatementBuilder stmt,
      int incrementalIndexLimit,
      int maxSerializedIndexSize)
      throws ObjTooLargeException {

    stmt.setLong(COL_COMMIT_CREATED.name(), obj.created());
    stmt.setLong(COL_COMMIT_SEQ.name(), obj.seq());
    stmt.setString(COL_COMMIT_MESSAGE.name(), obj.message());

    Headers.Builder hb = Headers.newBuilder();
    for (String h : obj.headers().keySet()) {
      hb.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(obj.headers().getAll(h)));
    }
    stmt.setByteBuffer(COL_COMMIT_HEADERS.name(), ByteBuffer.wrap(hb.build().toByteArray()));
    stmt.setString(
        COL_COMMIT_REFERENCE_INDEX.name(), CassandraSerde.serializeObjId(obj.referenceIndex()));

    Stripes.Builder b = Stripes.newBuilder();
    obj.referenceIndexStripes().stream()
        .map(
            s ->
                Stripe.newBuilder()
                    .setFirstKey(s.firstKey().rawString())
                    .setLastKey(s.lastKey().rawString())
                    .setSegment(s.segment().asBytes()))
        .forEach(b::addStripes);
    stmt.setByteBuffer(
        COL_COMMIT_REFERENCE_INDEX_STRIPES.name(), b.build().toByteString().asReadOnlyByteBuffer());

    stmt.setList(COL_COMMIT_TAIL.name(), CassandraSerde.serializeObjIds(obj.tail()), String.class);
    stmt.setList(
        COL_COMMIT_SECONDARY_PARENTS.name(),
        CassandraSerde.serializeObjIds(obj.secondaryParents()),
        String.class);

    ByteString index = obj.incrementalIndex();
    if (index.size() > incrementalIndexLimit) {
      throw new ObjTooLargeException(index.size(), incrementalIndexLimit);
    }
    stmt.setByteBuffer(COL_COMMIT_INCREMENTAL_INDEX.name(), index.asReadOnlyByteBuffer());

    stmt.setBoolean(COL_COMMIT_INCOMPLETE_INDEX.name(), obj.incompleteIndex());
    stmt.setString(COL_COMMIT_TYPE.name(), obj.commitType().name());
  }

  @Override
  public CommitObj deserialize(
      Row row, ObjType type, ObjId id, long referenced, String versionToken) {
    CommitObj.Builder b =
        CommitObj.commitBuilder()
            .id(id)
            .referenced(referenced)
            .created(row.getLong(COL_COMMIT_CREATED.name()))
            .seq(row.getLong(COL_COMMIT_SEQ.name()))
            .message(row.getString(COL_COMMIT_MESSAGE.name()))
            .referenceIndex(
                CassandraSerde.deserializeObjId(row.getString(COL_COMMIT_REFERENCE_INDEX.name())))
            .incrementalIndex(
                CassandraSerde.deserializeBytes(row, COL_COMMIT_INCREMENTAL_INDEX.name()))
            .incompleteIndex(row.getBoolean(COL_COMMIT_INCOMPLETE_INDEX.name()))
            .commitType(CommitType.valueOf(row.getString(COL_COMMIT_TYPE.name())));
    CassandraSerde.deserializeObjIds(row, COL_COMMIT_TAIL.name(), b::addTail);
    CassandraSerde.deserializeObjIds(
        row, COL_COMMIT_SECONDARY_PARENTS.name(), b::addSecondaryParents);

    try {
      CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
      Headers headers = Headers.parseFrom(row.getByteBuffer("c_headers"));
      for (HeaderEntry e : headers.getHeadersList()) {
        for (String v : e.getValuesList()) {
          h.add(e.getName(), v);
        }
      }
      b.headers(h.build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      Stripes stripes = Stripes.parseFrom(row.getByteBuffer("c_reference_index_stripes"));
      stripes.getStripesList().stream()
          .map(
              s ->
                  indexStripe(
                      keyFromString(s.getFirstKey()),
                      keyFromString(s.getLastKey()),
                      objIdFromByteBuffer(s.getSegment().asReadOnlyByteBuffer())))
          .forEach(b::addReferenceIndexStripes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return b.build();
  }
}
