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

import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.projectnessie.versioned.storage.cassandra.CassandraSerde;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class RefObjSerializer extends ObjSerializer<RefObj> {

  private static final CqlColumn COL_REF_NAME = new CqlColumn("r_name", CqlColumnType.NAME);
  private static final CqlColumn COL_REF_INITIAL_POINTER =
      new CqlColumn("r_initial_pointer", CqlColumnType.OBJ_ID);
  private static final CqlColumn COL_REF_CREATED_AT =
      new CqlColumn("r_created_at", CqlColumnType.BIGINT);
  private static final CqlColumn COL_REF_EXTENDED_INFO =
      new CqlColumn("r_extended_info", CqlColumnType.OBJ_ID);

  private static final Set<CqlColumn> COLS =
      ImmutableSet.of(
          COL_REF_NAME, COL_REF_INITIAL_POINTER, COL_REF_CREATED_AT, COL_REF_EXTENDED_INFO);

  public static final ObjSerializer<RefObj> INSTANCE = new RefObjSerializer();

  private RefObjSerializer() {
    super(COLS);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void serialize(
      RefObj obj,
      BoundStatementBuilder stmt,
      int incrementalIndexLimit,
      int maxSerializedIndexSize) {
    stmt.setString(COL_REF_NAME.name(), obj.name());
    stmt.setString(
        COL_REF_INITIAL_POINTER.name(), CassandraSerde.serializeObjId(obj.initialPointer()));
    stmt.setLong(COL_REF_CREATED_AT.name(), obj.createdAtMicros());
    stmt.setString(
        COL_REF_EXTENDED_INFO.name(), CassandraSerde.serializeObjId(obj.extendedInfoObj()));
  }

  @Override
  public RefObj deserialize(Row row, ObjType type, ObjId id, long referenced, String versionToken) {
    return ref(
        id,
        referenced,
        row.getString(COL_REF_NAME.name()),
        CassandraSerde.deserializeObjId(row.getString(COL_REF_INITIAL_POINTER.name())),
        row.getLong(COL_REF_CREATED_AT.name()),
        CassandraSerde.deserializeObjId(row.getString(COL_REF_EXTENDED_INFO.name())));
  }
}
