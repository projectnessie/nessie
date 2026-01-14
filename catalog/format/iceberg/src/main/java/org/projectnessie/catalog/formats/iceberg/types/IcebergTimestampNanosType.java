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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/** Iceberg timestamp, nanosecond precision. */
public final class IcebergTimestampNanosType extends IcebergPrimitiveType {
  private static final Schema TIMESTAMP_NS_SCHEMA =
      LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMPTZ_NS_SCHEMA =
      LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
  public static final String ADJUST_TO_UTC_PROP = "adjust-to-utc";

  static {
    TIMESTAMP_NS_SCHEMA.addProp(ADJUST_TO_UTC_PROP, false);
    TIMESTAMPTZ_NS_SCHEMA.addProp(ADJUST_TO_UTC_PROP, true);
  }

  private final boolean adjustToUTC;

  IcebergTimestampNanosType(boolean adjustToUTC) {
    this.adjustToUTC = adjustToUTC;
  }

  public boolean adjustToUTC() {
    return adjustToUTC;
  }

  @Override
  public String type() {
    return adjustToUTC() ? TYPE_TIMESTAMP_NS_TZ : TYPE_TIMESTAMP_NS;
  }

  @Override
  public Schema avroSchema(int fieldId) {
    return adjustToUTC() ? TIMESTAMPTZ_NS_SCHEMA : TIMESTAMP_NS_SCHEMA;
  }

  @Override
  public byte[] serializeSingleValue(Object value) {
    return IcebergLongType.serializeLong((Long) value);
  }

  @Override
  public Object deserializeSingleValue(byte[] value) {
    return IcebergLongType.deserializeLong(value);
  }

  @Override
  public int hashCode() {
    return type().hashCode() ^ (adjustToUTC ? 1 : 0);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof IcebergTimestampNanosType)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    IcebergTimestampNanosType o = (IcebergTimestampNanosType) obj;
    return o.adjustToUTC == adjustToUTC;
  }
}
