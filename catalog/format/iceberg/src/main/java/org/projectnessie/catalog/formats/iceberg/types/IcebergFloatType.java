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

import org.apache.avro.Schema;

public final class IcebergFloatType extends IcebergPrimitiveType {
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);

  @Override
  public String type() {
    return TYPE_FLOAT;
  }

  @Override
  public Schema avroSchema(int fieldId) {
    return FLOAT_SCHEMA;
  }

  @Override
  public byte[] serializeSingleValue(Object value) {
    return IcebergIntegerType.serializeInt(Float.floatToIntBits((Float) value));
  }

  @Override
  public Object deserializeSingleValue(byte[] value) {
    return Float.intBitsToFloat(IcebergIntegerType.deserializeInt(value));
  }
}
