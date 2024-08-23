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

import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeBytes;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.HeaderEntry;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Headers;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class TagObjSerializer implements ObjSerializer<TagObj> {

  public static final ObjSerializer<TagObj> INSTANCE = new TagObjSerializer();

  private static final String COL_TAG_MESSAGE = "t_message";
  private static final String COL_TAG_HEADERS = "t_headers";
  private static final String COL_TAG_SIGNATURE = "t_signature";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.of(
          COL_TAG_MESSAGE, JdbcColumnType.VARCHAR,
          COL_TAG_HEADERS, JdbcColumnType.VARBINARY,
          COL_TAG_SIGNATURE, JdbcColumnType.VARBINARY);

  private TagObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      TagObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException {
    ps.setString(nameToIdx.apply(COL_TAG_MESSAGE), obj.message());
    Headers.Builder hb = Headers.newBuilder();
    CommitHeaders headers = obj.headers();
    if (headers != null) {
      for (String h : headers.keySet()) {
        hb.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(headers.getAll(h)));
      }
    }
    ps.setBytes(nameToIdx.apply(COL_TAG_HEADERS), hb.build().toByteArray());
    serializeBytes(ps, nameToIdx.apply(COL_TAG_SIGNATURE), obj.signature(), databaseSpecific);
  }

  @Override
  public TagObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    CommitHeaders tagHeaders = null;
    try {
      Headers headers = Headers.parseFrom(deserializeBytes(rs, COL_TAG_HEADERS));
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
        referenced,
        rs.getString(COL_TAG_MESSAGE),
        tagHeaders,
        deserializeBytes(rs, COL_TAG_SIGNATURE));
  }
}
