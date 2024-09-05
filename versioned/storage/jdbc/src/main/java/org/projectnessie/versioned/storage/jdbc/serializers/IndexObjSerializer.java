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

import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeBytes;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeBytes;

import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class IndexObjSerializer implements ObjSerializer<IndexObj> {

  public static final ObjSerializer<IndexObj> INSTANCE = new IndexObjSerializer();

  private static final String COL_INDEX_INDEX = "i_index";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.of(COL_INDEX_INDEX, JdbcColumnType.VARBINARY);

  private IndexObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      IndexObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException, ObjTooLargeException {
    ByteString index = obj.index();
    if (index.size() > maxSerializedIndexSize) {
      throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
    }
    serializeBytes(ps, nameToIdx.apply(COL_INDEX_INDEX), index, databaseSpecific);
  }

  @Override
  public IndexObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    ByteString index = deserializeBytes(rs, COL_INDEX_INDEX);
    if (index != null) {
      return index(id, referenced, index);
    }
    throw new IllegalStateException("Index column for object ID " + id + " is null");
  }
}
