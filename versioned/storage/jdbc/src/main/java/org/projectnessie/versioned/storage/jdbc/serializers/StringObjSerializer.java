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

import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeObjIds;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeObjIds;

import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class StringObjSerializer implements ObjSerializer<StringObj> {

  public static final ObjSerializer<StringObj> INSTANCE = new StringObjSerializer();

  private static final String COL_STRING_CONTENT_TYPE = "s_content_type";
  private static final String COL_STRING_COMPRESSION = "s_compression";
  private static final String COL_STRING_FILENAME = "s_filename";
  private static final String COL_STRING_PREDECESSORS = "s_predecessors";
  private static final String COL_STRING_TEXT = "s_text";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.of(
          COL_STRING_CONTENT_TYPE, JdbcColumnType.NAME,
          COL_STRING_COMPRESSION, JdbcColumnType.NAME,
          COL_STRING_FILENAME, JdbcColumnType.NAME,
          COL_STRING_PREDECESSORS, JdbcColumnType.OBJ_ID_LIST,
          COL_STRING_TEXT, JdbcColumnType.VARBINARY);

  private StringObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      StringObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException {
    ps.setString(nameToIdx.apply(COL_STRING_CONTENT_TYPE), obj.contentType());
    ps.setString(nameToIdx.apply(COL_STRING_COMPRESSION), obj.compression().name());
    ps.setString(nameToIdx.apply(COL_STRING_FILENAME), obj.filename());
    serializeObjIds(
        ps, nameToIdx.apply(COL_STRING_PREDECESSORS), obj.predecessors(), databaseSpecific);
    serializeBytes(ps, nameToIdx.apply(COL_STRING_TEXT), obj.text(), databaseSpecific);
  }

  @Override
  public StringObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    return stringData(
        id,
        referenced,
        rs.getString(COL_STRING_CONTENT_TYPE),
        Compression.valueOf(rs.getString(COL_STRING_COMPRESSION)),
        rs.getString(COL_STRING_FILENAME),
        deserializeObjIds(rs, COL_STRING_PREDECESSORS),
        deserializeBytes(rs, COL_STRING_TEXT));
  }
}
