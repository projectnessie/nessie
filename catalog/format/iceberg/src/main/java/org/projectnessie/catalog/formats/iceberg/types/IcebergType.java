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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.apache.avro.Schema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;

@JsonSerialize(using = IcebergTypes.IcebergTypeSerializer.class)
@JsonDeserialize(using = IcebergTypes.IcebergTypeDeserializer.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergType {

  String TYPE_BOOLEAN = "boolean";
  String TYPE_UUID = "uuid";
  String TYPE_INT = "int";
  String TYPE_LONG = "long";
  String TYPE_FLOAT = "float";
  String TYPE_DOUBLE = "double";
  String TYPE_DATE = "date";
  String TYPE_TIME = "time";
  String TYPE_STRING = "string";
  String TYPE_BINARY = "binary";
  String TYPE_TIMESTAMP = "timestamp";
  String TYPE_TIMESTAMP_TZ = "timestamptz";
  String TYPE_FIXED = "fixed";
  String TYPE_DECIMAL = "decimal";
  String TYPE_STRUCT = "struct";
  String TYPE_LIST = "list";
  String TYPE_MAP = "map";

  static IcebergBooleanType booleanType() {
    return IcebergTypes.BOOLEAN;
  }

  static IcebergUuidType uuidType() {
    return IcebergTypes.UUID;
  }

  static IcebergIntegerType integerType() {
    return IcebergTypes.INTEGER;
  }

  static IcebergLongType longType() {
    return IcebergTypes.LONG;
  }

  static IcebergFloatType floatType() {
    return IcebergTypes.FLOAT;
  }

  static IcebergDoubleType doubleType() {
    return IcebergTypes.DOUBLE;
  }

  static IcebergDateType dateType() {
    return IcebergTypes.DATE;
  }

  static IcebergTimeType timeType() {
    return IcebergTypes.TIME;
  }

  static IcebergStringType stringType() {
    return IcebergTypes.STRING;
  }

  static IcebergBinaryType binaryType() {
    return IcebergTypes.BINARY;
  }

  static IcebergTimestampType timestamptzType() {
    return IcebergTypes.TIMESTAMPTZ;
  }

  static IcebergTimestampType timestampType() {
    return IcebergTypes.TIMESTAMP;
  }

  static IcebergFixedType fixedType(int length) {
    return ImmutableIcebergFixedType.of(length);
  }

  static IcebergDecimalType decimalType(int precision, int scale) {
    return ImmutableIcebergDecimalType.of(precision, scale);
  }

  static IcebergStructType structType(List<IcebergNestedField> fields, String recordName) {
    return ImmutableIcebergStructType.of(TYPE_STRUCT, fields, recordName);
  }

  static IcebergListType listType(int elementId, IcebergType element, boolean required) {
    return ImmutableIcebergListType.of(TYPE_LIST, elementId, element, required);
  }

  static IcebergMapType mapType(
      int keyId, IcebergType key, int valueId, IcebergType value, boolean valueRequired) {
    return ImmutableIcebergMapType.of(TYPE_MAP, keyId, key, valueId, value, valueRequired);
  }

  String type();

  int compare(Object left, Object right);

  byte[] serializeSingleValue(@NotNull Object value);

  Object deserializeSingleValue(byte[] value);

  Schema avroSchema(int fieldId);
}
