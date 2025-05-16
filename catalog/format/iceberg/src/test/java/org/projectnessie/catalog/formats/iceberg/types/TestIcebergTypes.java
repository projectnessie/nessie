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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.binaryType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.booleanType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.dateType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.decimalType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.doubleType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.fixedType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.floatType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.integerType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.listType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.longType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.mapType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.structType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timeType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timestampType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timestamptzType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.uuidType;

import java.util.stream.Stream;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergTypes {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void types(IcebergType type, String expected) throws Exception {
    String json = IcebergSpec.V2.jsonWriter().writeValueAsString(type);
    soft.assertThat(json).isEqualTo(expected);
    IcebergType deserialized = IcebergSpec.V2.jsonReader().readValue(json, IcebergType.class);
    soft.assertThat(deserialized).isEqualTo(type);
  }

  static Stream<Arguments> types() {
    return Stream.of(
        arguments(booleanType(), "\"boolean\""),
        arguments(integerType(), "\"int\""),
        arguments(longType(), "\"long\""),
        arguments(floatType(), "\"float\""),
        arguments(doubleType(), "\"double\""),
        arguments(stringType(), "\"string\""),
        arguments(binaryType(), "\"binary\""),
        arguments(dateType(), "\"date\""),
        arguments(timeType(), "\"time\""),
        arguments(timestampType(), "\"timestamp\""),
        arguments(timestamptzType(), "\"timestamptz\""),
        arguments(uuidType(), "\"uuid\""),
        arguments(fixedType(42), "\"fixed[42]\""),
        arguments(decimalType(33, 11), "\"decimal(33, 11)\""),
        arguments(
            mapType(42, stringType(), 43, binaryType(), true),
            "{\"type\":\"map\",\"key-id\":42,\"key\":\"string\",\"value-id\":43,\"value\":\"binary\",\"value-required\":true}"),
        arguments(
            listType(44, stringType(), true),
            "{\"type\":\"list\",\"element-id\":44,\"element\":\"string\",\"element-required\":true}"),
        arguments(
            structType(
                asList(
                    nestedField(201, "one", true, dateType(), null),
                    nestedField(202, "two", true, binaryType(), "doc for two"),
                    nestedField(203, "three", false, listType(666, stringType(), true), null)),
                null),
            "{\"type\":\"struct\",\"fields\":["
                + "{\"id\":201,\"name\":\"one\",\"required\":true,\"type\":\"date\"},"
                + "{\"id\":202,\"name\":\"two\",\"required\":true,\"type\":\"binary\",\"doc\":\"doc for two\"},"
                + "{\"id\":203,\"name\":\"three\",\"required\":false,\"type\":"
                + "{\"type\":\"list\",\"element-id\":666,\"element\":\"string\",\"element-required\":true}}]}"));
  }

  @ParameterizedTest
  @MethodSource
  public void icebergTypes(Type type, IcebergType icebergType) {
    org.apache.avro.Schema avroSchema;
    if (type instanceof Types.StructType) {
      avroSchema = AvroSchemaUtil.convert((Types.StructType) type, "r1");
    } else {
      avroSchema = AvroSchemaUtil.convert(type);
    }
    org.apache.avro.Schema avroIcebergSchema = icebergType.avroSchema(1);
    soft.assertThat(avroIcebergSchema).isEqualTo(avroSchema);
  }

  static Stream<Arguments> icebergTypes() {
    return Stream.of(
        arguments(Types.BooleanType.get(), booleanType()),
        arguments(Types.UUIDType.get(), uuidType()),
        arguments(Types.StringType.get(), stringType()),
        arguments(Types.BinaryType.get(), binaryType()),
        arguments(Types.IntegerType.get(), integerType()),
        arguments(Types.LongType.get(), longType()),
        arguments(Types.FloatType.get(), floatType()),
        arguments(Types.DoubleType.get(), doubleType()),
        arguments(Types.DateType.get(), dateType()),
        arguments(Types.TimeType.get(), timeType()),
        arguments(
            Types.StructType.of(Types.NestedField.required(11, "field11", Types.StringType.get())),
            structType(singletonList(nestedField(11, "field11", true, stringType(), null)), null)),
        arguments(
            Types.StructType.of(Types.NestedField.optional(11, "field11", Types.StringType.get())),
            structType(singletonList(nestedField(11, "field11", false, stringType(), null)), null)),
        arguments(
            Types.ListType.ofRequired(1, Types.StringType.get()), listType(1, stringType(), true)),
        arguments(
            Types.ListType.ofOptional(1, Types.StringType.get()), listType(1, stringType(), false)),
        arguments(
            Types.ListType.ofRequired(1, Types.UUIDType.get()), listType(1, uuidType(), true)),
        arguments(
            Types.ListType.ofOptional(1, Types.UUIDType.get()), listType(1, uuidType(), false)),
        arguments(
            Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.DateType.get()),
            mapType(3, stringType(), 4, dateType(), true)),
        arguments(
            Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.DateType.get()),
            mapType(3, stringType(), 4, dateType(), false)),
        arguments(
            Types.MapType.ofRequired(3, 4, Types.UUIDType.get(), Types.TimeType.get()),
            mapType(3, uuidType(), 4, timeType(), true)),
        arguments(
            Types.MapType.ofOptional(3, 4, Types.UUIDType.get(), Types.TimeType.get()),
            mapType(3, uuidType(), 4, timeType(), false)),
        arguments(Types.DecimalType.of(10, 3), decimalType(10, 3)),
        arguments(Types.FixedType.ofLength(42), fixedType(42)),
        arguments(Types.TimestampType.withoutZone(), timestampType()),
        arguments(Types.TimestampType.withZone(), timestamptzType()));
  }
}
