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

public final class IcebergTimestampType extends IcebergPrimitiveType {
  private static final Schema TIMESTAMP_SCHEMA =
      LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMPTZ_SCHEMA =
      LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
  public static final String ADJUST_TO_UTC_PROP = "adjust-to-utc";

  static {
    TIMESTAMP_SCHEMA.addProp(ADJUST_TO_UTC_PROP, false);
    TIMESTAMPTZ_SCHEMA.addProp(ADJUST_TO_UTC_PROP, true);
  }

  private final boolean adjustToUTC;

  IcebergTimestampType(boolean adjustToUTC) {
    this.adjustToUTC = adjustToUTC;
  }

  public boolean adjustToUTC() {
    return adjustToUTC;
  }

  @Override
  public String type() {
    return adjustToUTC() ? TYPE_TIMESTAMP_TZ : TYPE_TIMESTAMP;
  }

  @Override
  public Schema avroSchema(int fieldId) {
    return adjustToUTC() ? TIMESTAMPTZ_SCHEMA : TIMESTAMP_SCHEMA;
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
    if (!(obj instanceof IcebergTimestampType)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    IcebergTimestampType o = (IcebergTimestampType) obj;
    return o.adjustToUTC == adjustToUTC;
  }
}
