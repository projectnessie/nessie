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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeBytes;

import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class ContentValueObjSerializer implements ObjSerializer<ContentValueObj> {

  public static final ObjSerializer<ContentValueObj> INSTANCE = new ContentValueObjSerializer();

  private static final String COL_VALUE_CONTENT_ID = "v_content_id";
  private static final String COL_VALUE_PAYLOAD = "v_payload";
  private static final String COL_VALUE_DATA = "v_data";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.of(
          COL_VALUE_CONTENT_ID, JdbcColumnType.NAME,
          COL_VALUE_PAYLOAD, JdbcColumnType.BIGINT,
          COL_VALUE_DATA, JdbcColumnType.VARBINARY);

  private ContentValueObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      ContentValueObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException {
    ps.setString(nameToIdx.apply(COL_VALUE_CONTENT_ID), obj.contentId());
    ps.setInt(nameToIdx.apply(COL_VALUE_PAYLOAD), obj.payload());
    serializeBytes(ps, nameToIdx.apply(COL_VALUE_DATA), obj.data(), databaseSpecific);
  }

  @Override
  public ContentValueObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    return contentValue(
        id,
        referenced,
        rs.getString(COL_VALUE_CONTENT_ID),
        rs.getInt(COL_VALUE_PAYLOAD),
        requireNonNull(deserializeBytes(rs, COL_VALUE_DATA)));
  }
}
