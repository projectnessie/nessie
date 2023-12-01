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
package org.projectnessie.versioned.storage.jdbc;

import static java.util.Collections.emptyList;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_CREATED_AT;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_DELETED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_POINTER;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

public class JdbcSerde {

  public static void serializeBytes(
      PreparedStatement ps, int idx, ByteString blob, DatabaseSpecific databaseSpecific)
      throws SQLException {
    if (blob == null) {
      ps.setNull(idx, databaseSpecific.columnTypeIds().get(JdbcColumnType.VARBINARY));
      return;
    }
    ps.setBinaryStream(idx, blob.newInput());
  }

  public static ByteString deserializeBytes(ResultSet rs, String col) throws SQLException {
    byte[] bytes = rs.getBytes(col);
    return bytes != null ? unsafeWrap(bytes) : null;
  }

  public static ObjId deserializeObjId(ResultSet rs, String col) throws SQLException {
    String s = rs.getString(col);
    return s != null ? objIdFromString(s) : null;
  }

  public static void serializeObjId(
      PreparedStatement ps, int col, ObjId value, DatabaseSpecific databaseSpecific)
      throws SQLException {
    if (value != null) {
      ps.setString(col, value.toString());
    } else {
      ps.setNull(col, databaseSpecific.columnTypeIds().get(JdbcColumnType.VARCHAR));
    }
  }

  @SuppressWarnings("SameParameterValue")
  public static List<ObjId> deserializeObjIds(ResultSet rs, String col) throws SQLException {
    List<ObjId> r = new ArrayList<>();
    deserializeObjIds(rs, col, r::add);
    return r;
  }

  public static void deserializeObjIds(ResultSet rs, String col, Consumer<ObjId> consumer)
      throws SQLException {
    String s = rs.getString(col);
    if (s == null || s.isEmpty()) {
      return;
    }
    int i = 0;
    while (true) {
      int next = s.indexOf(',', i);
      String idAsString;
      if (next == -1) {
        idAsString = s.substring(i);
      } else {
        idAsString = s.substring(i, next);
        i = next + 1;
      }
      consumer.accept(objIdFromString(idAsString));
      if (next == -1) {
        return;
      }
    }
  }

  public static void serializeObjIds(
      PreparedStatement ps, int col, List<ObjId> values, DatabaseSpecific databaseSpecific)
      throws SQLException {
    if (values != null && !values.isEmpty()) {
      ps.setString(col, values.stream().map(ObjId::toString).collect(Collectors.joining(",")));
    } else {
      ps.setNull(col, databaseSpecific.columnTypeIds().get(JdbcColumnType.VARCHAR));
    }
  }

  public static Reference deserializeReference(ResultSet rs) throws SQLException {
    byte[] prevBytes = rs.getBytes(6);
    List<Reference.PreviousPointer> previousPointers =
        prevBytes != null ? deserializePreviousPointers(prevBytes) : emptyList();
    return Reference.reference(
        rs.getString(COL_REFS_NAME),
        deserializeObjId(rs, COL_REFS_POINTER),
        rs.getBoolean(COL_REFS_DELETED),
        rs.getLong(COL_REFS_CREATED_AT),
        deserializeObjId(rs, COL_REFS_EXTENDED_INFO),
        previousPointers);
  }
}
