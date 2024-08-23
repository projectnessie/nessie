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

import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.projectnessie.versioned.storage.cassandra.CassandraSerde;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class UniqueIdObjSerializer extends ObjSerializer<UniqueIdObj> {

  private static final CqlColumn COL_UNIQUE_SPACE = new CqlColumn("u_space", CqlColumnType.VARCHAR);
  private static final CqlColumn COL_UNIQUE_VALUE =
      new CqlColumn("u_value", CqlColumnType.VARBINARY);

  private static final Set<CqlColumn> COLS = ImmutableSet.of(COL_UNIQUE_SPACE, COL_UNIQUE_VALUE);

  public static final ObjSerializer<UniqueIdObj> INSTANCE = new UniqueIdObjSerializer();

  private UniqueIdObjSerializer() {
    super(COLS);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void serialize(
      UniqueIdObj obj,
      BoundStatementBuilder stmt,
      int incrementalIndexLimit,
      int maxSerializedIndexSize) {
    stmt.setString(COL_UNIQUE_SPACE.name(), obj.space());
    stmt.setByteBuffer(COL_UNIQUE_VALUE.name(), obj.value().asReadOnlyByteBuffer());
  }

  @Override
  public UniqueIdObj deserialize(
      Row row, ObjType type, ObjId id, long referenced, String versionToken) {
    return uniqueId(
        id,
        referenced,
        row.getString(COL_UNIQUE_SPACE.name()),
        CassandraSerde.deserializeBytes(row, COL_UNIQUE_VALUE.name()));
  }
}
