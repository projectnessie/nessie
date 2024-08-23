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
package org.projectnessie.versioned.storage.jdbc.serializers;

import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeObjId;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeObjIds;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeObjId;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeObjIds;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
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
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class CommitObjSerializer implements ObjSerializer<CommitObj> {

  public static final ObjSerializer<CommitObj> INSTANCE = new CommitObjSerializer();

  private static final String COL_COMMIT_CREATED = "c_created";
  private static final String COL_COMMIT_SEQ = "c_seq";
  private static final String COL_COMMIT_MESSAGE = "c_message";
  private static final String COL_COMMIT_HEADERS = "c_headers";
  private static final String COL_COMMIT_REFERENCE_INDEX = "c_reference_index";
  private static final String COL_COMMIT_REFERENCE_INDEX_STRIPES = "c_reference_index_stripes";
  private static final String COL_COMMIT_TAIL = "c_tail";
  private static final String COL_COMMIT_SECONDARY_PARENTS = "c_secondary_parents";
  private static final String COL_COMMIT_INCREMENTAL_INDEX = "c_incremental_index";
  private static final String COL_COMMIT_INCOMPLETE_INDEX = "c_incomplete_index";
  private static final String COL_COMMIT_TYPE = "c_commit_type";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.<String, JdbcColumnType>builder()
          .put(COL_COMMIT_CREATED, JdbcColumnType.BIGINT)
          .put(COL_COMMIT_SEQ, JdbcColumnType.BIGINT)
          .put(COL_COMMIT_MESSAGE, JdbcColumnType.VARCHAR)
          .put(COL_COMMIT_HEADERS, JdbcColumnType.VARBINARY)
          .put(COL_COMMIT_REFERENCE_INDEX, JdbcColumnType.OBJ_ID)
          .put(COL_COMMIT_REFERENCE_INDEX_STRIPES, JdbcColumnType.VARBINARY)
          .put(COL_COMMIT_TAIL, JdbcColumnType.OBJ_ID_LIST)
          .put(COL_COMMIT_SECONDARY_PARENTS, JdbcColumnType.OBJ_ID_LIST)
          .put(COL_COMMIT_INCREMENTAL_INDEX, JdbcColumnType.VARBINARY)
          .put(COL_COMMIT_INCOMPLETE_INDEX, JdbcColumnType.BOOL)
          .put(COL_COMMIT_TYPE, JdbcColumnType.NAME)
          .build();

  private CommitObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      CommitObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException, ObjTooLargeException {
    ps.setLong(nameToIdx.apply(COL_COMMIT_CREATED), obj.created());
    ps.setLong(nameToIdx.apply(COL_COMMIT_SEQ), obj.seq());
    ps.setString(nameToIdx.apply(COL_COMMIT_MESSAGE), obj.message());

    obj.headers();
    Headers.Builder hb = Headers.newBuilder();
    for (String h : obj.headers().keySet()) {
      hb.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(obj.headers().getAll(h)));
    }
    ps.setBytes(nameToIdx.apply(COL_COMMIT_HEADERS), hb.build().toByteArray());

    serializeObjId(
        ps, nameToIdx.apply(COL_COMMIT_REFERENCE_INDEX), obj.referenceIndex(), databaseSpecific);

    Stripes.Builder b = Stripes.newBuilder();
    obj.referenceIndexStripes().stream()
        .map(
            s ->
                Stripe.newBuilder()
                    .setFirstKey(s.firstKey().rawString())
                    .setLastKey(s.lastKey().rawString())
                    .setSegment(s.segment().asBytes()))
        .forEach(b::addStripes);
    serializeBytes(
        ps,
        nameToIdx.apply(COL_COMMIT_REFERENCE_INDEX_STRIPES),
        b.build().toByteString(),
        databaseSpecific);

    serializeObjIds(ps, nameToIdx.apply(COL_COMMIT_TAIL), obj.tail(), databaseSpecific);
    serializeObjIds(
        ps,
        nameToIdx.apply(COL_COMMIT_SECONDARY_PARENTS),
        obj.secondaryParents(),
        databaseSpecific);

    ByteString index = obj.incrementalIndex();
    if (index.size() > incrementalIndexLimit) {
      throw new ObjTooLargeException(index.size(), incrementalIndexLimit);
    }
    serializeBytes(ps, nameToIdx.apply(COL_COMMIT_INCREMENTAL_INDEX), index, databaseSpecific);

    ps.setBoolean(nameToIdx.apply(COL_COMMIT_INCOMPLETE_INDEX), obj.incompleteIndex());
    ps.setString(nameToIdx.apply(COL_COMMIT_TYPE), obj.commitType().name());
  }

  @Override
  public CommitObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    CommitObj.Builder b =
        CommitObj.commitBuilder()
            .id(id)
            .referenced(referenced)
            .created(rs.getLong(COL_COMMIT_CREATED))
            .seq(rs.getLong(COL_COMMIT_SEQ))
            .message(rs.getString(COL_COMMIT_MESSAGE))
            .referenceIndex(deserializeObjId(rs, COL_COMMIT_REFERENCE_INDEX))
            .incrementalIndex(deserializeBytes(rs, COL_COMMIT_INCREMENTAL_INDEX))
            .incompleteIndex(rs.getBoolean(COL_COMMIT_INCOMPLETE_INDEX))
            .commitType(CommitType.valueOf(rs.getString(COL_COMMIT_TYPE)));
    deserializeObjIds(rs, COL_COMMIT_TAIL, b::addTail);
    deserializeObjIds(rs, COL_COMMIT_SECONDARY_PARENTS, b::addSecondaryParents);

    try {
      CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
      Headers headers = Headers.parseFrom(rs.getBytes(COL_COMMIT_HEADERS));
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
      Stripes stripes = Stripes.parseFrom(rs.getBytes(COL_COMMIT_REFERENCE_INDEX_STRIPES));
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
