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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public interface ObjSerializer<O extends Obj> {

  Map<String, JdbcColumnType> columns();

  void serialize(
      PreparedStatement ps,
      O obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException, ObjTooLargeException;

  O deserialize(ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException;

  default void setNull(
      PreparedStatement ps, Function<String, Integer> nameToIdx, DatabaseSpecific databaseSpecific)
      throws SQLException {
    Map<JdbcColumnType, Integer> typeMap = databaseSpecific.columnTypeIds();
    for (Entry<String, JdbcColumnType> entry : columns().entrySet()) {
      String col = entry.getKey();
      JdbcColumnType type = entry.getValue();
      ps.setNull(nameToIdx.apply(col), typeMap.get(type));
    }
  }
}
