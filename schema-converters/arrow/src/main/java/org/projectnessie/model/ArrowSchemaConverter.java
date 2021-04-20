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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;


public final class ArrowSchemaConverter {

  private ArrowSchemaConverter() {

  }

  public static Schema toArrow(org.projectnessie.model.Schema nessieSchema) {
    return new Schema(nessieSchema.getFields().stream().map(ArrowSchemaConverter::toArrow).collect(Collectors.toList()));
  }

  private static org.apache.arrow.vector.types.pojo.Field toArrow(org.projectnessie.model.Field nessieField) {
    List<org.apache.arrow.vector.types.pojo.Field> children = Collections.emptyList();
    switch (nessieField.getType()) {
      case NULL:
        return getPrimitive(nessieField.getName(), ArrowType.Null.INSTANCE);
      case BOOLEAN:
        return getPrimitive(nessieField.getName(), ArrowType.Bool.INSTANCE);
      case INT8:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(8, true));
      case UINT8:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(8, false));
      case INT16:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(16, true));
      case UINT16:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(16, false));
      case INT32:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(32, true));
      case UINT32:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(32, false));
      case INT64:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(64, true));
      case UINT64:
        return getPrimitive(nessieField.getName(), new ArrowType.Int(64, false));
      case FLOAT16:
        return getPrimitive(nessieField.getName(), new ArrowType.FloatingPoint(FloatingPointPrecision.HALF));
      case FLOAT32:
        return getPrimitive(nessieField.getName(), new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
      case FLOAT64:
        return getPrimitive(nessieField.getName(), new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
      case BINARY:
        return getPrimitive(nessieField.getName(), ArrowType.Binary.INSTANCE);
      case FIXED_BINARY:
        return getPrimitive(nessieField.getName(), new ArrowType.FixedSizeBinary(((Fields.FixedBinary) nessieField).getLength()));
      case LARGE_BINARY:
        return getPrimitive(nessieField.getName(), ArrowType.LargeBinary.INSTANCE);
      case UTF8:
        return getPrimitive(nessieField.getName(), ArrowType.Utf8.INSTANCE);
      case LARGE_UTF8:
        return getPrimitive(nessieField.getName(), ArrowType.LargeUtf8.INSTANCE);
      case DATE:
        Fields.Date dateField = (Fields.Date) nessieField;
        return getPrimitive(nessieField.getName(), new ArrowType.Date(DateUnit.valueOf(dateField.getTimeUnit().name())));
      case TIME:
        Fields.Time timeField = (Fields.Time) nessieField;
        return getPrimitive(nessieField.getName(), new ArrowType.Time(TimeUnit.valueOf(timeField.getTimeUnit().name()),
          timeField.getBitWidth()));
      case TIMESTAMP:
        Fields.Timestamp timestampField = (Fields.Timestamp) nessieField;
        return getPrimitive(nessieField.getName(), new ArrowType.Timestamp(TimeUnit.valueOf(timestampField.getTimeUnit().name()),
          timestampField.getTimezone()));
      case DURATION:
        Fields.Duration durationField = (Fields.Duration) nessieField;
        return getPrimitive(nessieField.getName(), new ArrowType.Duration(TimeUnit.valueOf(durationField.getTimeUnit().name())));
      case INTERVAL:
        Fields.Interval intervalField = (Fields.Interval) nessieField;
        return getPrimitive(nessieField.getName(), new ArrowType.Interval(IntervalUnit.valueOf(intervalField.getTimeUnit().name())));
      case DECIMAL:
        Fields.Decimal decimalField = (Fields.Decimal) nessieField;
        return getPrimitive(nessieField.getName(), new ArrowType.Decimal(decimalField.getPrecision(), decimalField.getScale(),
          decimalField.getBitwidth()));
      case LIST:
        children = nessieField.getChildren().stream().map(ArrowSchemaConverter::toArrow).collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(), FieldType.nullable(ArrowType.List.INSTANCE), children);
      case LARGE_LIST:
        children = nessieField.getChildren().stream().map(ArrowSchemaConverter::toArrow).collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(), FieldType.nullable(ArrowType.LargeList.INSTANCE),
          children);
      case FIXED_LIST:
        Fields.FixedList listField = (Fields.FixedList) nessieField;
        children = nessieField.getChildren().stream().map(ArrowSchemaConverter::toArrow).collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(),
          FieldType.nullable(new ArrowType.FixedSizeList(listField.getSize())), children);
      case MAP:
        children = nessieField.getChildren().stream().map(ArrowSchemaConverter::toArrow).collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(),
          FieldType.nullable(new ArrowType.Map(false)), children);
      case STRUCT:
        children = nessieField.getChildren().stream().map(ArrowSchemaConverter::toArrow).collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Field(nessieField.getName(), FieldType.nullable(ArrowType.Struct.INSTANCE),
          children);
      case DENSE_UNION:
      case SPARSE_UNION:
      default:
        throw new UnsupportedOperationException(String.format("uknown field type %s", nessieField.getType()));
    }
  }

  private static org.apache.arrow.vector.types.pojo.Field getPrimitive(String name, ArrowType type) {
    return new org.apache.arrow.vector.types.pojo.Field(name, FieldType.nullable(type), Collections.emptyList());
  }

  public static org.projectnessie.model.Schema fromArrow(Schema arrowSchema) {
    ImmutableSchema.Builder builder = ImmutableSchema.builder();
    arrowSchema.getFields().stream().map(arrowField -> arrowField.getFieldType().getType().accept(new Visitor(arrowField)))
      .forEach(builder::addFields);
    return builder.build();
  }

  private static class Visitor implements ArrowType.ArrowTypeVisitor<org.projectnessie.model.Field> {
    private final org.apache.arrow.vector.types.pojo.Field arrowField;

    private Visitor(org.apache.arrow.vector.types.pojo.Field arrowField) {
      this.arrowField = arrowField;
    }

    @Override
    public Field visit(ArrowType.Null type) {
      return new Fields.Null(arrowField.getName());
    }

    @Override
    public Field visit(ArrowType.Struct type) {
      List<Field> children =
        arrowField.getChildren().stream().map(x -> x.getFieldType().getType().accept(new Visitor(x))).collect(Collectors.toList());
      return new Fields.Struct(arrowField.getName(), children);
    }

    @Override
    public Field visit(ArrowType.List type) {
      List<Field> children =
        arrowField.getChildren().stream().map(x -> x.getFieldType().getType().accept(new Visitor(x))).collect(Collectors.toList());
      return new Fields.List(arrowField.getName(), children);
    }

    @Override
    public Field visit(ArrowType.LargeList type) {
      List<Field> children =
        arrowField.getChildren().stream().map(x -> x.getFieldType().getType().accept(new Visitor(x))).collect(Collectors.toList());
      return new Fields.LargeList(arrowField.getName(), children);
    }

    @Override
    public Field visit(ArrowType.FixedSizeList type) {
      List<Field> children =
        arrowField.getChildren().stream().map(x -> x.getFieldType().getType().accept(new Visitor(x))).collect(Collectors.toList());
      return new Fields.FixedList(arrowField.getName(), type.getListSize(), children);
    }

    @Override
    public Field visit(ArrowType.Union type) {
      throw new UnsupportedOperationException("Union is not yet supported");
    }

    @Override
    public Field visit(ArrowType.Map type) {
      List<Field> children =
        arrowField.getChildren().stream().map(x -> x.getFieldType().getType().accept(new Visitor(x))).collect(Collectors.toList());
      return new Fields.Map(arrowField.getName(), children);
    }

    @Override
    public Field visit(ArrowType.Int type) {
      switch (type.getBitWidth()) {
        case 8:
          return type.getIsSigned() ? new Fields.Int8(arrowField.getName()) : new Fields.UInt8(arrowField.getName());
        case 16:
          return type.getIsSigned() ? new Fields.Int16(arrowField.getName()) : new Fields.UInt16(arrowField.getName());
        case 32:
          return type.getIsSigned() ? new Fields.Int32(arrowField.getName()) : new Fields.UInt32(arrowField.getName());
        case 64:
          return type.getIsSigned() ? new Fields.Int64(arrowField.getName()) : new Fields.UInt64(arrowField.getName());
        default:
          throw new IllegalStateException(String.format("Bitwidth %s is invalid for integer type", type.getBitWidth()));
      }
    }

    @Override
    public Field visit(ArrowType.FloatingPoint type) {
      switch (type.getPrecision()) {
        case HALF:
          return new Fields.Float16(arrowField.getName());
        case SINGLE:
          return new Fields.Float32(arrowField.getName());
        case DOUBLE:
          return new Fields.Float64(arrowField.getName());
        default:
          throw new IllegalStateException(String.format("precision %s is invalid for float type", type.getPrecision()));
      }
    }

    @Override
    public Field visit(ArrowType.Utf8 type) {
      return new Fields.Utf8(arrowField.getName());
    }

    @Override
    public Field visit(ArrowType.LargeUtf8 type) {
      return new Fields.LargeUtf8(arrowField.getName());
    }

    @Override
    public Field visit(ArrowType.Binary type) {
      return new Fields.Binary(arrowField.getName());
    }

    @Override
    public Field visit(ArrowType.LargeBinary type) {
      return new Fields.LargeBinary(arrowField.getName());
    }

    @Override
    public Field visit(ArrowType.FixedSizeBinary type) {
      return new Fields.FixedBinary(arrowField.getName(), type.getByteWidth());
    }

    @Override
    public Field visit(ArrowType.Bool type) {
      return new Fields.Boolean(arrowField.getName());
    }

    @Override
    public Field visit(ArrowType.Decimal type) {
      return new Fields.Decimal(arrowField.getName(), type.getPrecision(), type.getScale(), type.getBitWidth());
    }

    @Override
    public Field visit(ArrowType.Date type) {
      return new Fields.Date(arrowField.getName(), Field.DateUnit.valueOf(type.getUnit().toString()));
    }

    @Override
    public Field visit(ArrowType.Time type) {
      return new Fields.Time(arrowField.getName(), Field.TimeUnit.valueOf(type.getUnit().toString()), type.getBitWidth());
    }

    @Override
    public Field visit(ArrowType.Timestamp type) {
      return new Fields.Timestamp(arrowField.getName(), Field.TimeUnit.valueOf(type.getUnit().toString()), type.getTimezone());
    }

    @Override
    public Field visit(ArrowType.Interval type) {
      return new Fields.Interval(arrowField.getName(), Field.IntervalUnit.valueOf(type.getUnit().toString()));
    }

    @Override
    public Field visit(ArrowType.Duration type) {
      return new Fields.Duration(arrowField.getName(), Field.TimeUnit.valueOf(type.getUnit().toString()));
    }
  }
}
