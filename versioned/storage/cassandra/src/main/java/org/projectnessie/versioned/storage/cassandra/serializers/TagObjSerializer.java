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

import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_PREFIX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_VALUES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.STORE_OBJ_SUFFIX;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.cassandra.CassandraSerde;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.HeaderEntry;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Headers;

public class TagObjSerializer implements ObjSerializer<TagObj> {

  public static final ObjSerializer<TagObj> INSTANCE = new TagObjSerializer();

  private static final CqlColumn COL_TAG_MESSAGE =
      new CqlColumn("t_message", CqlColumnType.VARCHAR);
  private static final CqlColumn COL_TAG_HEADERS =
      new CqlColumn("t_headers", CqlColumnType.VARBINARY);
  private static final CqlColumn COL_TAG_SIGNATURE =
      new CqlColumn("t_signature", CqlColumnType.VARBINARY);

  private static final Set<CqlColumn> COLS =
      ImmutableSet.of(COL_TAG_MESSAGE, COL_TAG_HEADERS, COL_TAG_SIGNATURE);

  private static final String INSERT_CQL =
      INSERT_OBJ_PREFIX
          + COLS.stream().map(CqlColumn::name).collect(Collectors.joining(","))
          + INSERT_OBJ_VALUES
          + COLS.stream().map(c -> ":" + c.name()).collect(Collectors.joining(","))
          + ")";

  private static final String STORE_CQL = INSERT_CQL + STORE_OBJ_SUFFIX;

  private TagObjSerializer() {}

  @Override
  public Set<CqlColumn> columns() {
    return COLS;
  }

  @Override
  public String insertCql(boolean upsert) {
    return upsert ? INSERT_CQL : STORE_CQL;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void serialize(
      TagObj obj, BoundStatementBuilder stmt, int incrementalIndexLimit, int maxSerializedIndexSize)
      throws ObjTooLargeException {
    stmt.setString(COL_TAG_MESSAGE.name(), obj.message());
    Headers.Builder hb = Headers.newBuilder();
    CommitHeaders headers = obj.headers();
    if (headers != null) {
      for (String h : headers.keySet()) {
        hb.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(headers.getAll(h)));
      }
    }
    stmt.setByteBuffer(COL_TAG_HEADERS.name(), ByteBuffer.wrap(hb.build().toByteArray()));
    ByteString signature = obj.signature();
    stmt.setByteBuffer(
        COL_TAG_SIGNATURE.name(), signature != null ? signature.asReadOnlyByteBuffer() : null);
  }

  @Override
  public TagObj deserialize(Row row, ObjId id) {
    CommitHeaders tagHeaders = null;
    try {
      Headers headers = Headers.parseFrom(row.getByteBuffer(COL_TAG_HEADERS.name()));
      if (headers.getHeadersCount() > 0) {
        CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
        for (HeaderEntry e : headers.getHeadersList()) {
          for (String v : e.getValuesList()) {
            h.add(e.getName(), v);
          }
        }
        tagHeaders = h.build();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return tag(
        id,
        row.getString(COL_TAG_MESSAGE.name()),
        tagHeaders,
        CassandraSerde.deserializeBytes(row, COL_TAG_SIGNATURE.name()));
  }
}
