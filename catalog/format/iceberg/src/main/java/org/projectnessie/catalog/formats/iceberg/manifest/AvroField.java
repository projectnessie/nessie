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
package org.projectnessie.catalog.formats.iceberg.manifest;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

public final class AvroField {
  private final String name;
  private final Schema schema;
  private final Type type;
  private final MethodHandle getter;
  private final MethodHandle setter;
  private final FieldReader fieldReader;
  private final FieldWriter fieldWriter;

  AvroField(
      String name,
      Schema schema,
      Type type,
      MethodHandle getter,
      MethodHandle setter,
      AvroBundle bundle) {
    this.name = name;
    this.schema = schema;
    this.type = type;
    this.getter = getter;
    this.setter = setter;

    this.fieldReader = fieldReaderFunction(schema, type, bundle);
    this.fieldWriter = fieldWriterFunction(schema, type, bundle);
  }

  public String name() {
    return name;
  }

  public Schema schema() {
    return schema;
  }

  public Type type() {
    return type;
  }

  @Override
  public String toString() {
    return name;
  }

  public Object valueFrom(Object entityInstance) {
    try {
      return getter.bindTo(entityInstance).invoke();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public <B> void valueTo(B builderInstance, Object value) {
    try {
      if (value != null) {
        setter.bindTo(builderInstance).invoke(value);
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  interface FieldReader {
    Object read(Decoder decoder, Schema schema) throws IOException;
  }

  static Object readNull(Decoder decoder, Schema schema) throws IOException {
    checkReadSchema(schema, Schema.Type.NULL);
    decoder.readNull();
    return null;
  }

  static Object readBoolean(Decoder decoder, Schema schema) throws IOException {
    checkReadSchema(schema, Schema.Type.BOOLEAN);
    return decoder.readBoolean();
  }

  static Object readInt(Decoder decoder, Schema schema) throws IOException {
    checkReadSchema(schema, Schema.Type.INT);
    return decoder.readInt();
  }

  static Object readLong(Decoder decoder, Schema schema) throws IOException {
    checkReadSchema(schema, Schema.Type.LONG);
    return decoder.readLong();
  }

  static Object readFloat(Decoder decoder, Schema schema) throws IOException {
    checkReadSchema(schema, Schema.Type.FLOAT);
    return decoder.readFloat();
  }

  static Object readDouble(Decoder decoder, Schema schema) throws IOException {
    checkReadSchema(schema, Schema.Type.DOUBLE);
    return decoder.readDouble();
  }

  static Object readString(Decoder decoder, Schema schema) throws IOException {
    checkReadSchema(schema, Schema.Type.STRING);
    return decoder.readString();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static FieldReader readStringEnum(Class<? extends Enum> type) {
    return (decoder, readSchema) -> {
      checkReadSchema(readSchema, Schema.Type.STRING);
      String enumName = decoder.readString().toUpperCase(Locale.ROOT);
      return Enum.valueOf(type, enumName);
    };
  }

  static Object readByteBuffer(Decoder decoder, @SuppressWarnings("unused") Schema schema)
      throws IOException {
    return decoder.readBytes(null);
  }

  static Object readByteArray(Decoder decoder, @SuppressWarnings("unused") Schema schema)
      throws IOException {
    ByteBuffer byteBuffer = decoder.readBytes(null);
    if (byteBuffer == null) {
      return null;
    }
    if (byteBuffer.hasArray()) {
      byte[] array = byteBuffer.array();
      int pos = byteBuffer.position();
      int rem = byteBuffer.remaining();
      if (array.length == rem && pos == 0) {
        return array;
      }
      if (pos == 0) {
        return Arrays.copyOf(array, rem);
      }
    }
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(bytes);
    return bytes;
  }

  static Object readFixed(Decoder decoder, @SuppressWarnings("unused") Schema schema) {
    throw new UnsupportedOperationException();
  }

  static Object readMap(Decoder decoder, Schema schema) {
    // TODO Iceberg encodes maps with key-type STRING as "native" Avro maps
    //  optional (nullable) values as a UNION(NULL, valueSchema)
    //
    //  Maps with a different key-type are encoded as Avro arrays containing
    //  RECORDS with a 'key' + 'value' fields, the value gets a the default
    //  Avro 'JsonProperties.NULL_VALUE', if it's optional.
    //
    //  Currently there are no `Map<String, ?>` types in the manifest types.
    throw new UnsupportedOperationException();
  }

  static FieldReader readList(FieldReader elementReader, Schema elementSchema) {
    return (decoder, schema) -> {
      checkReadSchema(schema, Schema.Type.ARRAY);
      return readListElements(elementReader, decoder, elementSchema);
    };
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static Object readListElements(FieldReader elementReader, Decoder decoder, Schema elementSchema)
      throws IOException {
    // TODO use to the `Builder.put*()` method instead of an intermediate map
    List elements = new ArrayList();

    for (long chunkLength = decoder.readArrayStart();
        chunkLength != 0;
        chunkLength = decoder.arrayNext()) {
      for (long idx = 0; idx < chunkLength; idx++) {
        Object element = elementReader.read(decoder, elementSchema);
        elements.add(element);
      }
    }
    return elements;
  }

  static Object readUnion(Decoder decoder, Schema schema) {
    throw new UnsupportedOperationException();
  }

  static Object readEnum(Decoder decoder, Schema schema) {
    throw new UnsupportedOperationException();
  }

  public Object read(Decoder decoder, Schema fieldSchema) {
    try {
      return fieldReader.read(decoder, fieldSchema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static FieldReader readArrayMapRecord(
      Schema elementType, Type keyType, Type valueType, AvroBundle bundle) {
    Schema keySchema = elementType.getField("key").schema();
    FieldReader keyReader = fieldReaderFunction(keySchema, keyType, bundle);
    Schema valueSchema = elementType.getField("value").schema();
    FieldReader valueReader = fieldReaderFunction(valueSchema, valueType, bundle);

    return (decoder, schema) -> {
      checkReadSchema(schema, Schema.Type.ARRAY);

      // TODO use to the `Builder.put*()` method instead of an intermediate map
      Map map = new HashMap();

      for (long chunkLength = decoder.readArrayStart();
          chunkLength != 0;
          chunkLength = decoder.arrayNext()) {
        for (long idx = 0; idx < chunkLength; idx++) {
          Object key = keyReader.read(decoder, keySchema);
          Object value = valueReader.read(decoder, valueSchema);
          map.put(key, value);
        }
      }

      return map;
    };
  }

  private static FieldReader readNullable(
      Schema schema, Type type, boolean nullFirst, AvroBundle bundle) {
    FieldReader reader = fieldReaderFunction(schema, type, bundle);

    int nullIndex = nullFirst ? 0 : 1;
    int valueIndex = nullFirst ? 1 : 0;

    return (decoder, readSchema) -> {
      checkReadSchema(readSchema, Schema.Type.UNION);
      int index = decoder.readIndex();
      if (index == nullIndex) {
        decoder.readNull();
        return null;
      } else {
        return reader.read(decoder, readSchema.getTypes().get(valueIndex));
      }
    };
  }

  private static FieldReader readIntEnum(Class<? extends Enum<?>> type) {
    Enum<?>[] enumValues = type.getEnumConstants();
    return (decoder, readSchema) -> {
      checkReadSchema(readSchema, Schema.Type.INT);
      int enumOrdinal = decoder.readInt();
      // TODO add some bounds check
      return enumValues[enumOrdinal];
    };
  }

  private static void checkReadSchema(Schema readSchema, Schema.Type schemaType) {
    checkArgument(
        readSchema.getType() == schemaType,
        "Avro schema types differ, expected %s but reading %s",
        schemaType,
        readSchema.getType());
  }

  static FieldReader fieldReaderFunction(Schema schema, Type type, AvroBundle bundle) {
    switch (schema.getType()) {
      case NULL:
        return AvroField::readNull;
      case BOOLEAN:
        if (type == Boolean.class || type == boolean.class) {
          return AvroField::readBoolean;
        }
        break;
      case INT:
        if (type == Integer.class || type == int.class) {
          return AvroField::readInt;
        }
        if (Enum.class.isAssignableFrom((Class<?>) type)) {
          @SuppressWarnings("unchecked")
          Class<? extends Enum<?>> enumType = (Class<? extends Enum<?>>) type;
          return readIntEnum(enumType);
        }
        break;
      case LONG:
        if (type == Long.class || type == long.class) {
          return AvroField::readLong;
        }
        break;
      case FLOAT:
        if (type == Float.class || type == float.class) {
          return AvroField::readFloat;
        }
        break;
      case DOUBLE:
        if (type == Double.class || type == double.class) {
          return AvroField::readDouble;
        }
        break;
      case STRING:
        if (type == String.class) {
          return AvroField::readString;
        }
        if (Enum.class.isAssignableFrom((Class<?>) type)) {
          @SuppressWarnings("unchecked")
          Class<? extends Enum<?>> enumType = (Class<? extends Enum<?>>) type;
          return readStringEnum(enumType);
        }
        break;
      case BYTES:
        if (type.equals(byte[].class)) {
          return AvroField::readByteArray;
        }
        if (((Class<?>) type).isAssignableFrom(ByteBuffer.class)) {
          return AvroField::readByteBuffer;
        }
        break;
      case FIXED:
        return AvroField::readFixed;
      case MAP:
        return AvroField::readMap;
      case ARRAY:
        Schema elementType = schema.getElementType();
        if (type instanceof ParameterizedType) {
          ParameterizedType parameterizedType = (ParameterizedType) type;
          Type rawType = parameterizedType.getRawType();
          Type[] args = parameterizedType.getActualTypeArguments();
          if (Map.class.isAssignableFrom((Class<?>) rawType)) {
            if (Objects.requireNonNull(elementType.getType()) == Schema.Type.RECORD) {
              return readArrayMapRecord(elementType, args[0], args[1], bundle);
            }
          } else if (List.class.isAssignableFrom((Class<?>) rawType)) {
            FieldReader elementReader = fieldReaderFunction(elementType, args[0], bundle);
            return readList(elementReader, elementType);
          }
        }
        break;
      case ENUM:
        return AvroField::readEnum;
      case RECORD:
        AvroTyped<?> avroSchema = bundle.lookupSchema((Class<?>) type);
        return avroSchema::read;
      case UNION:
        List<Schema> types = schema.getTypes();
        if (types.size() == 2) {
          if (types.get(0).getType() == Schema.Type.NULL) {
            return readNullable(types.get(1), type, true, bundle);
          }
          if (types.get(1).getType() == Schema.Type.NULL) {
            return readNullable(types.get(0), type, false, bundle);
          }
        }
        return AvroField::readUnion;
      default:
        throw new IllegalStateException("Unknown Avro field type " + schema.getType());
    }
    throw new IllegalStateException(
        "Unknown Java field type " + type + " + Avro field type " + schema.getType());
  }

  @FunctionalInterface
  interface FieldWriter {
    void write(Encoder encoder, Object value) throws IOException;
  }

  public void write(Encoder encoder, Object value) throws IOException {
    fieldWriter.write(encoder, value);
  }

  static void writeNull(Encoder encoder, Object value) throws IOException {
    encoder.writeNull();
  }

  static void writeBoolean(Encoder encoder, Object value) throws IOException {
    encoder.writeBoolean((Boolean) value);
  }

  static void writeInt(Encoder encoder, Object value) throws IOException {
    encoder.writeInt((Integer) value);
  }

  static void writeIntEnum(Encoder encoder, Object value) throws IOException {
    Enum<?> enumValue = (Enum<?>) value;
    encoder.writeInt(enumValue.ordinal());
  }

  static void writeLong(Encoder encoder, Object value) throws IOException {
    encoder.writeLong((Long) value);
  }

  static void writeFloat(Encoder encoder, Object value) throws IOException {
    encoder.writeFloat((Float) value);
  }

  static void writeDouble(Encoder encoder, Object value) throws IOException {
    encoder.writeDouble((Double) value);
  }

  static void writeString(Encoder encoder, Object value) throws IOException {
    encoder.writeString((String) value);
  }

  static void writeStringEnum(Encoder encoder, Object value) throws IOException {
    Enum<?> enumValue = (Enum<?>) value;
    encoder.writeString(enumValue.name().toLowerCase(Locale.ROOT));
  }

  static void writeByteBuffer(Encoder encoder, Object value) throws IOException {
    encoder.writeBytes((ByteBuffer) value);
  }

  static void writeByteArray(Encoder encoder, Object value) throws IOException {
    encoder.writeBytes((byte[]) value);
  }

  static void writeFixed(Encoder encoder, Object value) {
    throw new UnsupportedOperationException();
  }

  static void writeMap(Encoder encoder, Object value) {
    // TODO Iceberg encodes maps with key-type STRING as "native" Avro maps
    //  optional (nullable) values as a UNION(NULL, valueSchema)
    //
    //  Maps with a different key-type are encoded as Avro arrays containing
    //  RECORDS with a 'key' + 'value' fields, the value gets a the default
    //  Avro 'JsonProperties.NULL_VALUE', if it's optional.
    //
    //  Currently there are no `Map<String, ?>` types in the manifest types.
    throw new UnsupportedOperationException();
  }

  static FieldWriter writeList(FieldWriter elementWriter) {
    return (encoder, value) -> {
      @SuppressWarnings("rawtypes")
      List list = (List) value;
      writeListElements(elementWriter, encoder, list);
    };
  }

  static void writeListElements(FieldWriter elementWriter, Encoder encoder, Collection<?> elements)
      throws IOException {
    encoder.writeArrayStart();
    int count = elements.size();
    encoder.setItemCount(count);
    for (Object o : elements) {
      encoder.startItem();
      elementWriter.write(encoder, o);
    }
    encoder.writeArrayEnd();
  }

  static void writeUnion(Encoder encoder, Object value) {
    throw new UnsupportedOperationException();
  }

  static void writeEnum(Encoder encoder, Object value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("rawtypes")
  private static FieldWriter writeArrayMapRecord(
      Schema elementType, Type keyType, Type valueType, AvroBundle bundle) {
    FieldWriter keyWriter =
        fieldWriterFunction(elementType.getField("key").schema(), keyType, bundle);
    FieldWriter valueWriter =
        fieldWriterFunction(elementType.getField("value").schema(), valueType, bundle);

    FieldWriter elementWriter =
        (encoder, value) -> {
          Map.Entry entry = (Map.Entry) value;
          keyWriter.write(encoder, entry.getKey());
          valueWriter.write(encoder, entry.getValue());
        };

    return (encoder, value) -> {
      Map map = (Map) value;
      Set entries = map.entrySet();
      writeListElements(elementWriter, encoder, entries);
    };
  }

  private static FieldWriter writeNullable(
      Schema schema, Type type, boolean nullFirst, AvroBundle bundle) {
    FieldWriter writer = fieldWriterFunction(schema, type, bundle);

    int nullIndex = nullFirst ? 0 : 1;
    int valueIndex = nullFirst ? 1 : 0;

    return (encoder, value) -> {
      if (value == null) {
        encoder.writeIndex(nullIndex);
        encoder.writeNull();
      } else {
        encoder.writeIndex(valueIndex);
        writer.write(encoder, value);
      }
    };
  }

  static FieldWriter fieldWriterFunction(Schema schema, Type type, AvroBundle bundle) {
    switch (schema.getType()) {
      case NULL:
        return AvroField::writeNull;
      case BOOLEAN:
        if (type == Boolean.class || type == boolean.class) {
          return AvroField::writeBoolean;
        }
        break;
      case INT:
        if (type == Integer.class || type == int.class) {
          return AvroField::writeInt;
        }
        if (Enum.class.isAssignableFrom((Class<?>) type)) {
          return AvroField::writeIntEnum;
        }
        break;
      case LONG:
        if (type == Long.class || type == long.class) {
          return AvroField::writeLong;
        }
        break;
      case FLOAT:
        if (type == Float.class || type == float.class) {
          return AvroField::writeFloat;
        }
        break;
      case DOUBLE:
        if (type == Double.class || type == double.class) {
          return AvroField::writeDouble;
        }
        break;
      case STRING:
        if (type == String.class) {
          return AvroField::writeString;
        }
        if (Enum.class.isAssignableFrom((Class<?>) type)) {
          return AvroField::writeStringEnum;
        }
        break;
      case BYTES:
        if (type.equals(byte[].class)) {
          return AvroField::writeByteArray;
        }
        if (((Class<?>) type).isAssignableFrom(ByteBuffer.class)) {
          return AvroField::writeByteBuffer;
        }
        break;
      case FIXED:
        return AvroField::writeFixed;
      case MAP:
        return AvroField::writeMap;
      case ARRAY:
        Schema elementType = schema.getElementType();
        if (type instanceof ParameterizedType) {
          ParameterizedType parameterizedType = (ParameterizedType) type;
          Type rawType = parameterizedType.getRawType();
          Type[] args = parameterizedType.getActualTypeArguments();
          if (Map.class.isAssignableFrom((Class<?>) rawType)) {
            if (Objects.requireNonNull(elementType.getType()) == Schema.Type.RECORD) {
              return writeArrayMapRecord(elementType, args[0], args[1], bundle);
            }
          } else if (List.class.isAssignableFrom((Class<?>) rawType)) {
            FieldWriter elementWriter = fieldWriterFunction(elementType, args[0], bundle);
            return writeList(elementWriter);
          }
        }
        break;
      case ENUM:
        return AvroField::writeEnum;
      case RECORD:
        @SuppressWarnings({"unchecked", "rawtypes"})
        AvroTyped<Object> avroSchema = bundle.lookupSchema((Class) type);
        return (encoder, object) -> avroSchema.write(encoder, object, schema);
      case UNION:
        List<Schema> unionTypes = schema.getTypes();
        if (unionTypes.size() == 2) {
          Schema unionAt0 = unionTypes.get(0);
          Schema unionAt1 = unionTypes.get(1);
          if (unionAt0.getType() == Schema.Type.NULL) {
            return writeNullable(unionAt1, type, true, bundle);
          }
          if (unionAt1.getType() == Schema.Type.NULL) {
            return writeNullable(unionAt0, type, false, bundle);
          }
        }
        return AvroField::writeUnion;
      default:
        throw new IllegalStateException("Unknown Avro field type " + schema.getType());
    }
    throw new IllegalStateException(
        "Unknown Java field type " + type + " + Avro field type " + schema.getType());
  }
}
