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
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.UPDATE_COND_IF;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.UPDATE_COND_PREFIX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.UPDATE_COND_WHERE;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public abstract class ObjSerializer<O extends Obj> {

  private final Set<CqlColumn> columns;
  private final String insertCql;
  private final String storeCql;
  private final String updateConditionalCql;

  protected ObjSerializer(Set<CqlColumn> columns) {
    this.columns = columns;

    this.insertCql =
        INSERT_OBJ_PREFIX
            + columns.stream().map(CqlColumn::name).collect(Collectors.joining(","))
            + INSERT_OBJ_VALUES
            + columns.stream().map(c -> ":" + c.name()).collect(Collectors.joining(","))
            + ")";

    this.storeCql = this.insertCql + STORE_OBJ_SUFFIX;

    this.updateConditionalCql =
        UPDATE_COND_PREFIX
            + columns.stream()
                .map(c -> c.name() + " = :" + c.name())
                .collect(Collectors.joining(","))
            + " WHERE "
            + UPDATE_COND_WHERE
            + " "
            + UPDATE_COND_IF;
  }

  public final Set<CqlColumn> columns() {
    return columns;
  }

  public final String insertCql(boolean upsert) {
    return upsert ? insertCql : storeCql;
  }

  public final String updateConditionalCql() {
    return updateConditionalCql;
  }

  public abstract void serialize(
      O obj, BoundStatementBuilder stmt, int incrementalIndexLimit, int maxSerializedIndexSize)
      throws ObjTooLargeException;

  public abstract O deserialize(
      Row row, ObjType type, ObjId id, long referenced, String versionToken);
}
