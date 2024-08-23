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

import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.cassandra.CassandraSerde;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class IndexObjSerializer extends ObjSerializer<IndexObj> {

  private static final CqlColumn COL_INDEX_INDEX =
      new CqlColumn("i_index", CqlColumnType.VARBINARY);

  private static final Set<CqlColumn> COLS = ImmutableSet.of(COL_INDEX_INDEX);

  public static final ObjSerializer<IndexObj> INSTANCE = new IndexObjSerializer();

  private IndexObjSerializer() {
    super(COLS);
  }

  @Override
  public void serialize(
      IndexObj obj,
      BoundStatementBuilder stmt,
      int incrementalIndexLimit,
      int maxSerializedIndexSize)
      throws ObjTooLargeException {
    ByteString index = obj.index();
    if (index.size() > maxSerializedIndexSize) {
      throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
    }
    stmt.setByteBuffer(COL_INDEX_INDEX.name(), index.asReadOnlyByteBuffer());
  }

  @Override
  public IndexObj deserialize(
      Row row, ObjType type, ObjId id, long referenced, String versionToken) {
    ByteString indexValue = CassandraSerde.deserializeBytes(row, COL_INDEX_INDEX.name());
    if (indexValue != null) {
      return index(id, referenced, indexValue);
    }
    throw new IllegalStateException("Index value of obj " + id + " of type INDEX is null");
  }
}
