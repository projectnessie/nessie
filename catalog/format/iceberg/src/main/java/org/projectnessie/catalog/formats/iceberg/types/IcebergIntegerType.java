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
import org.apache.avro.Schema;

public final class IcebergIntegerType extends IcebergPrimitiveType {
  private static final Schema INTEGER_SCHEMA = Schema.create(Schema.Type.INT);

  @Override
  public String type() {
    return TYPE_INT;
  }

  @Override
  public Schema avroSchema(int fieldId) {
    return INTEGER_SCHEMA;
  }

  @Override
  public byte[] serializeSingleValue(Object value) {
    return serializeInt((Integer) value);
  }

  @Override
  public Object deserializeSingleValue(byte[] value) {
    return deserializeInt(value);
  }

  static byte[] serializeInt(int value) {
    byte[] buf = new byte[4];
    ByteBuffer buffer = ByteBuffer.wrap(buf);
    buffer.putInt(0, value);
    return buf;
  }

  static int deserializeInt(byte[] value) {
    return ByteBuffer.wrap(value).getInt();
  }
}
