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

import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeObjId;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeObjId;

import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class RefObjSerializer implements ObjSerializer<RefObj> {

  public static final ObjSerializer<RefObj> INSTANCE = new RefObjSerializer();

  private static final String COL_REF_NAME = "r_name";
  private static final String COL_REF_INITIAL_POINTER = "r_initial_pointer";
  private static final String COL_REF_CREATED_AT = "r_created_at";
  private static final String COL_REF_EXTENDED_INFO = "r_extended_info";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.of(
          COL_REF_NAME, JdbcColumnType.NAME,
          COL_REF_INITIAL_POINTER, JdbcColumnType.OBJ_ID,
          COL_REF_CREATED_AT, JdbcColumnType.BIGINT,
          COL_REF_EXTENDED_INFO, JdbcColumnType.OBJ_ID);

  private RefObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      RefObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException {
    ps.setString(nameToIdx.apply(COL_REF_NAME), obj.name());
    serializeObjId(
        ps, nameToIdx.apply(COL_REF_INITIAL_POINTER), obj.initialPointer(), databaseSpecific);
    ps.setLong(nameToIdx.apply(COL_REF_CREATED_AT), obj.createdAtMicros());
    serializeObjId(
        ps, nameToIdx.apply(COL_REF_EXTENDED_INFO), obj.extendedInfoObj(), databaseSpecific);
  }

  @Override
  public RefObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    return ref(
        id,
        referenced,
        rs.getString(COL_REF_NAME),
        deserializeObjId(rs, COL_REF_INITIAL_POINTER),
        rs.getLong(COL_REF_CREATED_AT),
        deserializeObjId(rs, COL_REF_EXTENDED_INFO));
  }
}
