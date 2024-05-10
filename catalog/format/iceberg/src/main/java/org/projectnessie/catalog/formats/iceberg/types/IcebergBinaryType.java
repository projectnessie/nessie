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

public final class IcebergBinaryType extends IcebergPrimitiveType {
  private static final Schema BINARY_SCHEMA = Schema.create(Schema.Type.BYTES);

  @Override
  public String type() {
    return TYPE_BINARY;
  }

  @Override
  public Schema avroSchema(int fieldId) {
    return BINARY_SCHEMA;
  }

  @Override
  public int compare(Object left, Object right) {
    return compareBinary(left, right);
  }

  @Override
  public byte[] serializeSingleValue(Object value) {
    return serializeBinary(value);
  }

  @Override
  public Object deserializeSingleValue(byte[] value) {
    return deserializeBinary(value);
  }

  static int compareBinary(Object left, Object right) {
    ByteBuffer bufferLeft = (ByteBuffer) left;
    ByteBuffer bufferRight = (ByteBuffer) right;
    return bufferLeft.compareTo(bufferRight);
  }

  static Object deserializeBinary(byte[] value) {
    return value;
  }

  static byte[] serializeBinary(Object value) {
    if (value instanceof ByteBuffer) {
      return toByteArray((ByteBuffer) value);
    }
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    throw new IllegalArgumentException(value.getClass().toString());
  }

  private static byte[] toByteArray(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(bytes);
    return bytes;
  }
}
