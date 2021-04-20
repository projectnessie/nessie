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

import java.util.List;
import java.util.Objects;

public class Fields {

  private static class PrimitiveField implements Field {
    private final FieldType fieldType;
    private final String name;

    private PrimitiveField(FieldType fieldType, String name) {
      this.fieldType = assertNonNull(fieldType, "fieldType");
      this.name = assertNonNull(name, "name");
    }

    @Override
    public FieldType getType() {
      return fieldType;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public java.util.List<Field> getChildren() {
      throw new UnsupportedOperationException("Primitive types don't have children");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PrimitiveField that = (PrimitiveField) o;
      return fieldType == that.fieldType && name.equals(that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldType, name);
    }

    public FieldType getFieldType() {
      return fieldType;
    }
  }

  static class TimeLikeField<T extends Enum<T>> extends PrimitiveField {

    private final T timeUnit;

    private TimeLikeField(FieldType fieldType, String name, T timeUnit) {
      super(fieldType, name);
      this.timeUnit = assertNonNull(timeUnit, "timeUnit");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      TimeLikeField that = (TimeLikeField) o;
      return timeUnit == that.timeUnit;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), timeUnit);
    }

    public T getTimeUnit() {
      return timeUnit;
    }
  }

  static class Null extends PrimitiveField {
     Null(String name) {
       super(FieldType.NULL, name);
     }
  }
  static class Boolean extends PrimitiveField {
    Boolean(String name) {
      super(FieldType.BOOLEAN, name);
    }
  }
  static class Int8 extends PrimitiveField {
    Int8(String name) {
      super(FieldType.INT8, name);
    }
  }
  static class UInt8 extends PrimitiveField {
    UInt8(String name) {
      super(FieldType.UINT8, name);
    }
  }
  static class Int16 extends PrimitiveField {
    Int16(String name) {
      super(FieldType.INT16, name);
    }
  }
  static class UInt16 extends PrimitiveField {
    UInt16(String name) {
      super(FieldType.UINT16, name);
    }
  }
  static class Int32 extends PrimitiveField {
    Int32(String name) {
      super(FieldType.INT32, name);
    }
  }
  static class UInt32 extends PrimitiveField {
    UInt32(String name) {
      super(FieldType.UINT32, name);
    }
  }
  static class Int64 extends PrimitiveField {
    Int64(String name) {
      super(FieldType.INT64, name);
    }
  }
  static class UInt64 extends PrimitiveField {
    UInt64(String name) {
      super(FieldType.UINT64, name);
    }
  }
  static class Float16 extends PrimitiveField {
    Float16(String name) {
      super(FieldType.FLOAT16, name);
    }
  }
  static class Float32 extends PrimitiveField {
    Float32(String name) {
      super(FieldType.FLOAT32, name);
    }
  }
  static class Float64 extends PrimitiveField {
    Float64(String name) {
      super(FieldType.FLOAT64, name);
    }
  }
  static class Binary extends PrimitiveField {
    Binary(String name) {
      super(FieldType.BINARY, name);
    }
  }
  static class LargeBinary extends PrimitiveField {
    LargeBinary(String name) {
      super(FieldType.LARGE_BINARY, name);
    }
  }
  static class Utf8 extends PrimitiveField {
    Utf8(String name) {
      super(FieldType.UTF8, name);
    }
  }
  static class LargeUtf8 extends PrimitiveField {
    LargeUtf8(String name) {
      super(FieldType.LARGE_UTF8, name);
    }
  }

  static class Date extends TimeLikeField<Field.DateUnit> {
    Date(String name, DateUnit timeUnit) {
      super(FieldType.DATE, name, timeUnit);
    }
  }
  static class Time extends TimeLikeField<Field.TimeUnit> {
    private final int bitWidth;

    Time(String name, TimeUnit timeUnit, int bitWidth) {
      super(FieldType.TIME, name, timeUnit);
      this.bitWidth = bitWidth;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Time time = (Time) o;
      return bitWidth == time.bitWidth;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), bitWidth);
    }

    public int getBitWidth() {
      return bitWidth;
    }
  }
  static class Timestamp extends TimeLikeField<Field.TimeUnit> {
    private final String timezone;
    Timestamp(String name, TimeUnit timeUnit, String timezone) {
      super(FieldType.TIMESTAMP, name, timeUnit);
      this.timezone = timezone;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Timestamp timestamp = (Timestamp) o;
      return timezone.equals(timestamp.timezone);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), timezone);
    }

    public String getTimezone() {
      return timezone;
    }
  }
  static class Duration extends TimeLikeField<Field.TimeUnit> {
    Duration(String name, TimeUnit timeUnit) {
      super(FieldType.DURATION, name, timeUnit);
    }
  }
  static class Interval extends TimeLikeField<Field.IntervalUnit> {
    Interval(String name, IntervalUnit timeUnit) {
      super(FieldType.INTERVAL, name, timeUnit);
    }
  }
  static class FixedBinary extends PrimitiveField {
    private final int length;
    FixedBinary(String name, int length) {
      super(FieldType.FIXED_BINARY, name);
      this.length = length;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      FixedBinary that = (FixedBinary) o;
      return length == that.length;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), length);
    }

    public int getLength() {
      return length;
    }
  }

  static class Decimal extends PrimitiveField {
    private final int precision;
    private final int scale;
    private final int bitwidth;
    Decimal(String name, int precision, int scale, int bitwidth) {
      super(FieldType.DECIMAL, name);
      this.precision = precision;
      this.scale = scale;
      this.bitwidth = bitwidth;
    }

    Decimal(String name, int precision, int scale) {
      this(name, precision, scale, 64);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Decimal decimal = (Decimal) o;
      return precision == decimal.precision && scale == decimal.scale && bitwidth == decimal.bitwidth;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), precision, scale, bitwidth);
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    public int getBitwidth() {
      return bitwidth;
    }
  }

  private static class ComplexField implements Field {
    private final String name;
    private final FieldType type;
    private final java.util.List<Field> children;

    ComplexField(String name, FieldType type, java.util.List<Field> children) {
      this.name = assertNonNull(name, "name");
      this.type = assertNonNull(type, "type");
      this.children = assertNonNull(children, "children");
    }

    @Override
    public FieldType getType() {
      return type;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public java.util.List<Field> getChildren() {
      return children;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ComplexField that = (ComplexField) o;
      return name.equals(that.name) && type == that.type && children.equals(that.children);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type, children);
    }
  }

  static class List extends ComplexField {

    List(String name, java.util.List<Field> children) {
      super(name, FieldType.LIST, children);
    }
  }

  static class Map extends ComplexField {

    Map(String name, java.util.List<Field> children) {
      super(name, FieldType.MAP, children);
    }
  }

  static class Struct extends ComplexField {

    Struct(String name, java.util.List<Field> children) {
      super(name, FieldType.STRUCT, children);
    }
  }

  static class LargeList extends ComplexField {

    LargeList(String name, java.util.List<Field> children) {
      super(name, FieldType.LARGE_LIST, children);
    }
  }

  static class FixedList extends ComplexField {

    private final int size;

    FixedList(String name, int size, java.util.List<Field> children) {
      super(name, FieldType.FIXED_LIST, children);
      this.size = size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      FixedList fixedList = (FixedList) o;
      return size == fixedList.size;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), size);
    }

    public int getSize() {
      return size;
    }
  }
  private static <T> T assertNonNull(T val, String name) {
    if (val == null) {
      throw new IllegalStateException(String.format("Value can't be null in %s", name));
    }
    return val;
  }
}
