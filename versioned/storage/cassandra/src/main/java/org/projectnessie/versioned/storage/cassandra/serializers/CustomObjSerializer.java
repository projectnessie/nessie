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

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.serialize.SmileSerialization;

public class CustomObjSerializer implements ObjSerializer<Obj> {

  public static final ObjSerializer<?> INSTANCE = new CustomObjSerializer();

  // Do not reuse 'x_class' column name
  private static final CqlColumn COL_CUSTOM_DATA = new CqlColumn("x_data", CqlColumnType.VARBINARY);
  private static final CqlColumn COL_CUSTOM_COMPRESSION =
      new CqlColumn("x_compress", CqlColumnType.NAME);

  private static final Set<CqlColumn> COLS =
      ImmutableSet.of(COL_CUSTOM_DATA, COL_CUSTOM_COMPRESSION);

  private static final String INSERT_CQL =
      INSERT_OBJ_PREFIX
          + COLS.stream().map(CqlColumn::name).collect(Collectors.joining(","))
          + INSERT_OBJ_VALUES
          + COLS.stream().map(c -> ":" + c.name()).collect(Collectors.joining(","))
          + ")";

  private static final String STORE_CQL = INSERT_CQL + STORE_OBJ_SUFFIX;

  private CustomObjSerializer() {}

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
      Obj obj, BoundStatementBuilder stmt, int incrementalIndexLimit, int maxSerializedIndexSize)
      throws ObjTooLargeException {
    stmt.setByteBuffer(
        COL_CUSTOM_DATA.name(),
        ByteBuffer.wrap(
            SmileSerialization.serializeObj(
                obj,
                compression ->
                    stmt.setString(COL_CUSTOM_COMPRESSION.name(), compression.valueString()))));
    ;
  }

  @Override
  public Obj deserialize(Row row, ObjType type, ObjId id) {
    ByteBuffer buffer = Objects.requireNonNull(row.getByteBuffer(COL_CUSTOM_DATA.name()));
    return SmileSerialization.deserializeObj(
        id, buffer, type.targetClass(), row.getString(COL_CUSTOM_COMPRESSION.name()));
  }
}
