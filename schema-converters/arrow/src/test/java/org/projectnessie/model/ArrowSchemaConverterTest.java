/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ArrowSchemaConverterTest {

  @ParameterizedTest
  @MethodSource("arrowFields")
  void testPrimitiveSchema(ArrowType type) {
    Schema expectedSchema = new Schema(Collections.singletonList(new Field("x", FieldType.nullable(type), Collections.emptyList())));
    org.projectnessie.model.Schema nessieSchema = ArrowSchemaConverter.fromArrow(expectedSchema);
    Schema actualSchema = ArrowSchemaConverter.toArrow(nessieSchema);
    Assertions.assertEquals(expectedSchema, actualSchema);
  }

  @ParameterizedTest
  @MethodSource("arrowFields")
  void testComplexSchema(ArrowType type) {
    List<Field> children = Collections.singletonList(new Field("x", FieldType.nullable(type), Collections.emptyList()));

    List<Field> schemaFields = new ArrayList<>();
    schemaFields.add(new Field("a", FieldType.nullable(ArrowType.List.INSTANCE), children));
    schemaFields.add(new Field("b", FieldType.nullable(new ArrowType.Map(false)), children));
    schemaFields.add(new Field("c", FieldType.nullable(ArrowType.Struct.INSTANCE), children));
    schemaFields.add(new Field("d", FieldType.nullable(ArrowType.LargeList.INSTANCE), children));
    schemaFields.add(new Field("e", FieldType.nullable(new ArrowType.FixedSizeList(10)), children));
    Schema expectedSchema = new Schema(schemaFields);
    org.projectnessie.model.Schema nessieSchema = ArrowSchemaConverter.fromArrow(expectedSchema);
    Schema actualSchema = ArrowSchemaConverter.toArrow(nessieSchema);
    Assertions.assertEquals(expectedSchema, actualSchema);
  }

  private static Stream<ArrowType> arrowFields() {
    return Stream.of(
      ArrowType.Null.INSTANCE,
      new ArrowType.Int(8, false),
      new ArrowType.Int(8, false),
      new ArrowType.Int(16, false),
      new ArrowType.Int(16, true),
      new ArrowType.Int(32, false),
      new ArrowType.Int(32, true),
      new ArrowType.Int(64, false),
      new ArrowType.Int(64, true),
      new ArrowType.FloatingPoint(FloatingPointPrecision.HALF),
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
      ArrowType.Binary.INSTANCE,
      new ArrowType.FixedSizeBinary(12),
      ArrowType.LargeBinary.INSTANCE,
      ArrowType.Utf8.INSTANCE,
      ArrowType.LargeUtf8.INSTANCE,
      new ArrowType.Date(DateUnit.MILLISECOND),
      new ArrowType.Date(DateUnit.DAY),
      new ArrowType.Time(TimeUnit.SECOND, 8),
      new ArrowType.Time(TimeUnit.MILLISECOND, 8),
      new ArrowType.Time(TimeUnit.MICROSECOND, 8),
      new ArrowType.Time(TimeUnit.NANOSECOND, 8),
      new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"),
      new ArrowType.Timestamp(TimeUnit.MILLISECOND, "EST"),
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, "GMT"),
      new ArrowType.Timestamp(TimeUnit.NANOSECOND, "CET"),
      new ArrowType.Duration(TimeUnit.SECOND),
      new ArrowType.Duration(TimeUnit.MILLISECOND),
      new ArrowType.Duration(TimeUnit.MICROSECOND),
      new ArrowType.Duration(TimeUnit.NANOSECOND),
      new ArrowType.Interval(IntervalUnit.YEAR_MONTH),
      new ArrowType.Interval(IntervalUnit.DAY_TIME),
      new ArrowType.Decimal(1,2,64)
    );
  }
}


//new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(), FieldType.nullable(ArrowType.List.INSTANCE), children);
//new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(), FieldType.nullable(ArrowType.LargeList.INSTANCE),
//new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(), ieldType.nullable(new ArrowType.FixedSizeList(listField.getSize())), children);
//new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(), FieldType.nullable(new ArrowType.Map(false)), children);
