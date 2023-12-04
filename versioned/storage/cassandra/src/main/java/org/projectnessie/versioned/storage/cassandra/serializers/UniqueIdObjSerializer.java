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

import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_PREFIX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_VALUES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.STORE_OBJ_SUFFIX;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.cassandra.CassandraSerde;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public class UniqueIdObjSerializer implements ObjSerializer<UniqueIdObj> {

  public static final ObjSerializer<UniqueIdObj> INSTANCE = new UniqueIdObjSerializer();

  private static final CqlColumn COL_UNIQUE_SPACE = new CqlColumn("u_space", CqlColumnType.VARCHAR);
  private static final CqlColumn COL_UNIQUE_VALUE =
      new CqlColumn("u_value", CqlColumnType.VARBINARY);

  private static final Set<CqlColumn> COLS = ImmutableSet.of(COL_UNIQUE_SPACE, COL_UNIQUE_VALUE);

  private static final String INSERT_CQL =
      INSERT_OBJ_PREFIX
          + COLS.stream().map(CqlColumn::name).collect(Collectors.joining(","))
          + INSERT_OBJ_VALUES
          + COLS.stream().map(c -> ":" + c.name()).collect(Collectors.joining(","))
          + ")";

  private static final String STORE_CQL = INSERT_CQL + STORE_OBJ_SUFFIX;

  private UniqueIdObjSerializer() {}

  @Override
  public Set<CqlColumn> columns() {
    return COLS;
  }

  @Override
  public String insertCql(boolean upsert) {
    return upsert ? INSERT_CQL : STORE_CQL;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void serialize(
      UniqueIdObj obj,
      BoundStatementBuilder stmt,
      int incrementalIndexLimit,
      int maxSerializedIndexSize)
      throws ObjTooLargeException {
    stmt.setString(COL_UNIQUE_SPACE.name(), obj.space());
    stmt.setByteBuffer(COL_UNIQUE_VALUE.name(), obj.value().asReadOnlyByteBuffer());
  }

  @Override
  public UniqueIdObj deserialize(Row row, ObjId id) {
    return uniqueId(
        id,
        row.getString(COL_UNIQUE_SPACE.name()),
        CassandraSerde.deserializeBytes(row, COL_UNIQUE_VALUE.name()));
  }
}
