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

import static org.projectnessie.catalog.formats.iceberg.manifest.Avro.LogicalMap.logicalMap;
import static org.projectnessie.catalog.formats.iceberg.manifest.Avro.avroNullable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergMapType.class)
@JsonDeserialize(as = ImmutableIcebergMapType.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(IcebergMapType.TYPE_NAME)
public interface IcebergMapType extends IcebergComplexType {
  String KEY_ID_PROP = "key-id";
  String VALUE_ID_PROP = "value-id";
  String TYPE_NAME = "map";

  @Override
  @Value.Default
  default String type() {
    return TYPE_NAME;
  }

  int keyId();

  IcebergType key();

  int valueId();

  IcebergType value();

  boolean valueRequired();

  @Override
  @Value.NonAttribute
  default Schema avroSchema(int fieldId) {
    Schema keySchema = key().avroSchema(keyId());
    Schema valueSchema = value().avroSchema(valueId());
    if (!valueRequired()) {
      valueSchema = avroNullable(valueSchema);
    }

    if (keySchema.getType() == Schema.Type.STRING) {

      // "native" Avro map
      Schema schema = Schema.createMap(valueSchema);
      schema.addProp(KEY_ID_PROP, keyId());
      schema.addProp(VALUE_ID_PROP, valueId());
      return schema;
    }

    String keyValueName = "k" + keyId() + "_v" + valueId();
    Schema.Field keyField = new Schema.Field("key", keySchema, null, null);
    keyField.addProp(IcebergStructType.FIELD_ID_PROP, keyId());

    Schema.Field valueField =
        valueRequired()
            ? new Schema.Field("value", valueSchema, null)
            : new Schema.Field("value", valueSchema, null, JsonProperties.NULL_VALUE);
    valueField.addProp(IcebergStructType.FIELD_ID_PROP, valueId());

    return logicalMap()
        .addToSchema(
            Schema.createArray(
                Schema.createRecord(
                    keyValueName, null, null, false, ImmutableList.of(keyField, valueField))));
  }

  static Builder builder() {
    return ImmutableIcebergMapType.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergType instance);

    @CanIgnoreReturnValue
    Builder type(String type);

    @CanIgnoreReturnValue
    Builder keyId(int keyId);

    @CanIgnoreReturnValue
    Builder key(IcebergType key);

    @CanIgnoreReturnValue
    Builder valueId(int valueId);

    @CanIgnoreReturnValue
    Builder value(IcebergType value);

    @CanIgnoreReturnValue
    Builder valueRequired(boolean valueRequired);

    org.projectnessie.catalog.formats.iceberg.types.IcebergMapType build();
  }
}
