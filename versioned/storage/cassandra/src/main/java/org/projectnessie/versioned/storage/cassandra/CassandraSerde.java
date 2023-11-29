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
package org.projectnessie.versioned.storage.cassandra;

import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;

import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

public final class CassandraSerde {

  private CassandraSerde() {}

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
        deserializeObjId(row.getString(1)),
        row.getBoolean(2),
        row.getLong(3),
        deserializeObjId(row.getString(4)),
        deserializePreviousPointers(bytes));
  }

  public static ObjId deserializeObjId(String id) {
    return id != null ? objIdFromString(id) : null;
  }

  public static String serializeObjId(ObjId id) {
    return id != null ? id.toString() : null;
  }

  public static List<ObjId> deserializeObjIds(Row row, String col) {
    List<ObjId> r = new ArrayList<>();
    deserializeObjIds(row, col, r::add);
    return r;
  }

  public static void deserializeObjIds(Row row, String col, Consumer<ObjId> consumer) {
    List<String> s = row.getList(col, String.class);
    if (s == null || s.isEmpty()) {
      return;
    }
    s.stream().map(ObjId::objIdFromString).forEach(consumer);
  }

  public static List<String> serializeObjIds(List<ObjId> values) {
    return (values != null && !values.isEmpty())
        ? values.stream().map(ObjId::toString).collect(Collectors.toList())
        : null;
  }
}
