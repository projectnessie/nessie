/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cassandra2;

import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;

import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.ByteBuffer;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

public final class Cassandra2Serde {

  private Cassandra2Serde() {}

  public static ByteString deserializeBytes(Row row, String col) {
    ByteBuffer bytes = row.getByteBuffer(col);
    return bytes != null ? unsafeWrap(bytes) : null;
  }

  public static Reference deserializeReference(Row row) {
    ByteBuffer previous = row.getByteBuffer(5);
    byte[] bytes;
    if (previous != null) {
      bytes = new byte[previous.remaining()];
      previous.get(bytes);
    } else {
      bytes = null;
    }
    return Reference.reference(
        row.getString(0),
        deserializeObjId(row.getByteBuffer(1)),
        row.getBoolean(2),
        row.getLong(3),
        deserializeObjId(row.getByteBuffer(4)),
        deserializePreviousPointers(bytes));
  }

  public static ObjId deserializeObjId(ByteBuffer id) {
    return id != null ? objIdFromByteBuffer(id) : null;
  }

  public static ByteBuffer serializeObjId(ObjId id) {
    return id != null ? id.asByteBuffer() : null;
  }
}
