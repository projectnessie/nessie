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
package org.projectnessie.versioned.storage.jdbc2;

import static java.util.Collections.emptyList;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteArray;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_CREATED_AT;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_DELETED;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_POINTER;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

public class Jdbc2Serde {

  public static ObjId deserializeObjId(ResultSet rs, String col) throws SQLException {
    byte[] s = rs.getBytes(col);
    return s != null ? objIdFromByteArray(s) : null;
  }

  public static void serializeObjId(
      PreparedStatement ps, int col, ObjId value, DatabaseSpecific databaseSpecific)
      throws SQLException {
    if (value != null) {
      ps.setBytes(col, value.asByteArray());
    } else {
      ps.setNull(col, databaseSpecific.columnTypeIds().get(Jdbc2ColumnType.OBJ_ID));
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
