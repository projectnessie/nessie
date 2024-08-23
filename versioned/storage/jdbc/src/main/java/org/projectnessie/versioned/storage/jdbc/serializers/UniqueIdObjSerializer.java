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

import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeBytes;

import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class UniqueIdObjSerializer implements ObjSerializer<UniqueIdObj> {

  public static final ObjSerializer<UniqueIdObj> INSTANCE = new UniqueIdObjSerializer();

  private static final String COL_UNIQUE_SPACE = "u_space";
  private static final String COL_UNIQUE_VALUE = "u_value";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.of(
          COL_UNIQUE_SPACE, JdbcColumnType.VARCHAR,
          COL_UNIQUE_VALUE, JdbcColumnType.VARBINARY);

  private UniqueIdObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      UniqueIdObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException {
    ps.setString(nameToIdx.apply(COL_UNIQUE_SPACE), obj.space());
    serializeBytes(ps, nameToIdx.apply(COL_UNIQUE_VALUE), obj.value(), databaseSpecific);
  }

  @Override
  public UniqueIdObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    return uniqueId(
        id, referenced, rs.getString(COL_UNIQUE_SPACE), deserializeBytes(rs, COL_UNIQUE_VALUE));
  }
}
