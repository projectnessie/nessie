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

import static org.projectnessie.catalog.formats.iceberg.manifest.Avro.decimalRequiredBytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class IcebergDecimalType extends IcebergPrimitiveType {

  public abstract int precision();

  public abstract int scale();

  @Override
  public String type() {
    return TYPE_DECIMAL + "(" + precision() + ", " + scale() + ')';
  }

  @Override
  @Value.Lazy
  public Schema avroSchema(int fieldId) {
    return LogicalTypes.decimal(precision(), scale())
        .addToSchema(
            Schema.createFixed(
                TYPE_DECIMAL + "_" + precision() + "_" + scale(),
                null,
                null,
                decimalRequiredBytes(precision())));
  }

  @Override
  public byte[] serializeSingleValue(Object value) {
    BigDecimal decimal = (BigDecimal) value;
    return decimal.unscaledValue().toByteArray();
  }

  @Override
  public Object deserializeSingleValue(byte[] value) {
    return new BigDecimal(new BigInteger(value));
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
