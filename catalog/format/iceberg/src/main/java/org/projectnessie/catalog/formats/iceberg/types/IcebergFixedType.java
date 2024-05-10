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
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class IcebergFixedType extends IcebergPrimitiveType {

  public abstract int length();

  @Override
  public String type() {
    return TYPE_FIXED + "[" + length() + ']';
  }

  @Override
  @Value.NonAttribute
  @Value.Lazy
  public Schema avroSchema(int fieldId) {
    return Schema.createFixed(TYPE_FIXED + "_" + length(), null, null, length());
  }

  @Override
  public int compare(Object left, Object right) {
    return IcebergBinaryType.compareBinary(left, right);
  }

  @Override
  public byte[] serializeSingleValue(Object value) {
    return IcebergBinaryType.serializeBinary(value);
  }

  @Override
  public Object deserializeSingleValue(byte[] value) {
    return IcebergBinaryType.deserializeBinary(value);
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public String toString() {
    return type();
  }
}
