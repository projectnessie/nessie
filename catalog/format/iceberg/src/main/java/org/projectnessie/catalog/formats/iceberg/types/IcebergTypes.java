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

import static java.lang.Integer.parseInt;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class IcebergTypes {
  static final Pattern DECIMAL_PATTERN =
      Pattern.compile(IcebergDecimalType.TYPE_DECIMAL + "[ ]*\\([ ]*(\\d+)[ ]*,[ ]*(\\d+)[ ]*\\)");
  static final Pattern FIXED_PATTERN =
      Pattern.compile(IcebergFixedType.TYPE_FIXED + "[ ]*\\[[ ]*(\\d*)[ ]*]");

  static final IcebergBooleanType BOOLEAN = new IcebergBooleanType();
  static final IcebergUuidType UUID = new IcebergUuidType();
  static final IcebergIntegerType INTEGER = new IcebergIntegerType();
  static final IcebergLongType LONG = new IcebergLongType();
  static final IcebergFloatType FLOAT = new IcebergFloatType();
  static final IcebergDoubleType DOUBLE = new IcebergDoubleType();
  static final IcebergDateType DATE = new IcebergDateType();
  static final IcebergTimeType TIME = new IcebergTimeType();
  static final IcebergStringType STRING = new IcebergStringType();
  static final IcebergBinaryType BINARY = new IcebergBinaryType();
  static final IcebergTimestampType TIMESTAMPTZ = new IcebergTimestampType(true);
  static final IcebergTimestampType TIMESTAMP = new IcebergTimestampType(false);

  private IcebergTypes() {}

  static IcebergPrimitiveType primitiveFromString(String primitiveType) {
    switch (primitiveType) {
      case IcebergBooleanType.TYPE_BOOLEAN:
        return IcebergType.booleanType();
      case IcebergUuidType.TYPE_UUID:
        return IcebergType.uuidType();
      case IcebergIntegerType.TYPE_INT:
        return IcebergType.integerType();
      case IcebergLongType.TYPE_LONG:
        return IcebergType.longType();
      case IcebergFloatType.TYPE_FLOAT:
        return IcebergType.floatType();
      case IcebergDoubleType.TYPE_DOUBLE:
        return IcebergType.doubleType();
      case IcebergDateType.TYPE_DATE:
        return IcebergType.dateType();
      case IcebergTimeType.TYPE_TIME:
        return IcebergType.timeType();
      case IcebergStringType.TYPE_STRING:
        return IcebergType.stringType();
      case IcebergBinaryType.TYPE_BINARY:
        return IcebergType.binaryType();
      case IcebergTimestampType.TYPE_TIMESTAMP_TZ:
        return IcebergType.timestamptzType();
      case IcebergTimestampType.TYPE_TIMESTAMP:
        return IcebergType.timestampType();
      default:
        Matcher m = DECIMAL_PATTERN.matcher(primitiveType);
        if (m.matches()) {
          return IcebergType.decimalType(parseInt(m.group(1)), parseInt(m.group(2)));
        }
        m = FIXED_PATTERN.matcher(primitiveType);
        if (m.matches()) {
          return IcebergType.fixedType(parseInt(m.group(1)));
        }
        break;
    }
    throw new IllegalArgumentException("Unknown Iceberg primitive type '" + primitiveType + "'");
  }

  static final class IcebergPrimitiveSerializer extends JsonSerializer<IcebergPrimitiveType> {
    @Override
    public void serialize(
        IcebergPrimitiveType value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(value.type());
    }
  }

  static final class IcebergPrimitiveDeserializer extends JsonDeserializer<IcebergPrimitiveType> {
    @Override
    public IcebergPrimitiveType deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      return primitiveFromString(p.getValueAsString());
    }
  }

  static final class IcebergTypeDeserializer extends JsonDeserializer<IcebergType> {
    @Override
    public IcebergType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      switch (p.currentToken()) {
        case VALUE_STRING:
          return primitiveFromString(p.getValueAsString());
        case START_OBJECT:
          return p.readValueAs(IcebergComplexType.class);
        default:
          throw new IllegalArgumentException("unexpected token " + p.currentToken());
      }
    }
  }

  static final class IcebergTypeSerializer extends JsonSerializer<IcebergType> {

    @Override
    public void serialize(IcebergType value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value instanceof IcebergPrimitiveType) {
        gen.writeString(value.type());
      } else {
        gen.writeObject(value);
      }
    }
  }
}
