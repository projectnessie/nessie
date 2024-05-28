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
package org.projectnessie.catalog.formats.iceberg.types;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public final class IcebergUuidType extends IcebergPrimitiveType {
  private static final Schema UUID_SCHEMA =
      LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16));

  @Override
  public String type() {
    return TYPE_UUID;
  }

  @Override
  public Schema avroSchema(int fieldId) {
    return UUID_SCHEMA;
  }

  @Override
  public byte[] serializeSingleValue(Object value) {
    UUID uuid = (UUID) value;
    byte[] buf = new byte[16];
    ByteBuffer buffer = ByteBuffer.wrap(buf);
    buffer.putLong(0, uuid.getMostSignificantBits());
    buffer.putLong(8, uuid.getLeastSignificantBits());
    return buf;
  }

  @Override
  public Object deserializeSingleValue(byte[] value) {
    ByteBuffer bb = ByteBuffer.wrap(value);
    long msb = bb.getLong();
    long lsb = bb.getLong();
    return new UUID(msb, lsb);
  }
}
