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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.UNION;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;

public final class Avro {
  private Avro() {}

  public static AvroBundle bundleFor(int specVersion) {
    switch (specVersion) {
      case 1:
        return AvroLazyInit.FORMAT_V1;
      case 2:
        return AvroLazyInit.FORMAT_V2;
      default:
        throw new IllegalArgumentException("Unknown Iceberg spec version: " + specVersion);
    }
  }

  static final byte[] MAGIC_BYTES = new byte[] {(byte) 0xC2, (byte) 0x01};

  private static AvroTyped<GenericRecord> genericRecordHandler() {
    return new AvroTyped<GenericRecord>() {
      @SuppressWarnings({"rawtypes", "unchecked"})
      @Override
      public void write(Encoder encoder, GenericRecord object, Schema writeSchema)
          throws IOException {
        GenericDatumWriter writer = new GenericDatumWriter(object.getSchema());
        writer.write(object, encoder);
      }

      @SuppressWarnings({"rawtypes", "unchecked"})
      @Override
      public GenericRecord read(Decoder decoder, Schema schema) throws IOException {
        GenericDatumReader reader = new GenericDatumReader(schema, schema);
        Object record = reader.read(null, decoder);
        return (GenericRecord) record;
      }

      @Override
      public void prefix(OutputStream output) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Schema schema() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Schema schema(InputStream input) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Schema writeSchema(AvroReadWriteContext readWriteContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public AvroField field(int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean handles(Class<?> type) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static final class AvroLazyInit {

    private static final AvroBundle FORMAT_V1;
    private static final AvroBundle FORMAT_V2;

    static {
      FORMAT_V1 =
          new Bundle()
              .loadAvroSchema(
                  "v1/partition_summary",
                  IcebergPartitionFieldSummary.class,
                  IcebergPartitionFieldSummary.Builder.class,
                  IcebergPartitionFieldSummary::builder)
              .loadAvroSchema(
                  "v1/manifest_entry_data_file",
                  IcebergDataFile.class,
                  IcebergDataFile.Builder.class,
                  IcebergDataFile::builder)
              .loadAvroSchema(
                  "v1/manifest_entry",
                  IcebergManifestEntry.class,
                  IcebergManifestEntry.Builder.class,
                  IcebergManifestEntry::builder)
              .loadAvroSchema(
                  "v1/manifest_file",
                  IcebergManifestFile.class,
                  IcebergManifestFile.Builder.class,
                  IcebergManifestFile::builder);

      FORMAT_V2 =
          new Bundle()
              .loadAvroSchema(
                  "v2/partition_summary",
                  IcebergPartitionFieldSummary.class,
                  IcebergPartitionFieldSummary.Builder.class,
                  IcebergPartitionFieldSummary::builder)
              .loadAvroSchema(
                  "v2/manifest_entry_data_file",
                  IcebergDataFile.class,
                  IcebergDataFile.Builder.class,
                  IcebergDataFile::builder)
              .loadAvroSchema(
                  "v2/manifest_entry",
                  IcebergManifestEntry.class,
                  IcebergManifestEntry.Builder.class,
                  IcebergManifestEntry::builder)
              .loadAvroSchema(
                  "v2/manifest_file",
                  IcebergManifestFile.class,
                  IcebergManifestFile.Builder.class,
                  IcebergManifestFile::builder);
    }
  }

  private static class Bundle implements AvroBundle {

    private final List<AvroTyped<?>> knownSchema;

    private Bundle() {
      this.knownSchema = new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> AvroTyped<T> lookupSchema(Class<T> entityType) {
      for (AvroTyped<?> avroSchema : knownSchema) {
        if (avroSchema.handles(entityType)) {
          return (AvroTyped<T>) avroSchema;
        }
      }

      if (GenericRecord.class.isAssignableFrom(entityType)) {
        return (AvroTyped<T>) genericRecordHandler();
      }

      throw new IllegalArgumentException("No schema known for Java " + entityType);
    }

    @Override
    public AvroTyped<IcebergManifestFile> schemaManifestFile() {
      return lookupSchema(IcebergManifestFile.class);
    }

    @Override
    public AvroTyped<IcebergManifestEntry> schemaManifestEntry() {
      return lookupSchema(IcebergManifestEntry.class);
    }

    private static final Converter<String, String> GETTER_SETTER =
        CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL);

    <E, B> Bundle loadAvroSchema(
        String name,
        Class<? extends E> entityType,
        Class<? extends B> builderType,
        Supplier<? extends B> builderCreator) {
      for (AvroTyped<?> avroSchema : knownSchema) {
        checkState(!avroSchema.handles(entityType), "More than one schema handles " + entityType);
      }

      Schema schema = loadSchema(name);
      byte[] prefix = "manifest_file".equals(name) ? manifestFilePrefix(schema) : null;
      AvroSchema<E, B> avroSchema =
          buildAvroSchema(name, schema, entityType, builderType, prefix, builderCreator, this);
      knownSchema.add(avroSchema);
      return this;
    }

    private static byte[] manifestFilePrefix(Schema schema) {
      // See org.apache.iceberg.avro.AvroEncoderUtil.encode
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      try (DataOutputStream dataOutput = new DataOutputStream(output)) {
        dataOutput.write(MAGIC_BYTES);
        dataOutput.writeUTF(schema.toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return output.toByteArray();
    }

    private static <E, B> AvroSchema<E, B> buildAvroSchema(
        String name,
        Schema schema,
        Class<? extends E> entityType,
        Class<? extends B> builderType,
        byte[] prefix,
        Supplier<? extends B> builderCreator,
        AvroBundle bundle) {
      try {
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        List<Schema.Field> schemaFields = schema.getFields();
        AvroField[] fields = new AvroField[schemaFields.size()];
        for (int i = 0; i < fields.length; i++) {
          Schema.Field schemaField = schemaFields.get(i);

          String getterSetterName = requireNonNull(GETTER_SETTER.convert(schemaField.name()));
          Method getter = entityType.getMethod(getterSetterName);
          Type fieldType = getter.getGenericReturnType();

          Schema fieldSchema = schemaField.schema();
          Schema.Type fieldSchemType = fieldSchema.getType();
          if (fieldSchemType == UNION) {
            List<Schema> types = fieldSchema.getTypes();
            if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL) {
              fieldSchemType = types.get(1).getType();
            } else {
              throw new IllegalArgumentException(
                  "Unsupported UNION with in field " + schemaField.schema());
            }
          }

          Class<?> rawFieldType;
          if (fieldType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) fieldType;
            rawFieldType = (Class<?>) parameterizedType.getRawType();
          } else {
            rawFieldType = (Class<?>) fieldType;
          }

          boolean isJavaMap = Map.class.isAssignableFrom(rawFieldType);
          boolean isSchemaArray = fieldSchemType == Schema.Type.ARRAY;

          Class<?> setterType;
          if (isSchemaArray && !isJavaMap) {
            setterType = Iterable.class;
          } else {
            setterType = rawFieldType;
          }

          Method setter;
          // TODO adopt collection reading using the `add*()` / `put*()` methods on the builders
          //
          // if (Collection.class.isAssignableFrom(setterType)
          //    || Iterable.class.isAssignableFrom(setterType)) {
          //  Class<?> elementType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
          //
          //  elementType = asPrimitiveType(elementType);
          //
          //  setter =
          //      builderType.getMethod(
          //          "add"
          //              + Character.toUpperCase(getterSetterName.charAt(0))
          //              + getterSetterName.substring(1),
          //          elementType);
          // } else if (isJavaMap) {
          //  Class<?> keyType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
          //  Class<?> valueType = (Class<?>) parameterizedType.getActualTypeArguments()[1];
          //
          //  keyType = asPrimitiveType(keyType);
          //  valueType = asPrimitiveType(valueType);
          //
          //  setter =
          //      builderType.getMethod(
          //          "put"
          //              + Character.toUpperCase(getterSetterName.charAt(0))
          //              + getterSetterName.substring(1),
          //          keyType,
          //          valueType);
          // } else {
          //  setter = builderType.getMethod(getterSetterName, setterType);
          // }
          setter = builderType.getMethod(getterSetterName, setterType);

          MethodHandle getterMethod = lookup.unreflect(getter);
          MethodHandle setterMethod = lookup.unreflect(setter);

          fields[i] =
              new AvroField(
                  schemaField.name(), fieldSchema, fieldType, getterMethod, setterMethod, bundle);
        }

        MethodHandle builderBuild =
            lookup.findVirtual(builderType, "build", MethodType.methodType(entityType));
        MethodHandle builderClear =
            lookup.findVirtual(builderType, "clear", MethodType.methodType(builderType));

        return new AvroSchema<>(
            schema,
            fields,
            prefix,
            entityType,
            builderCreator,
            builder -> {
              try {
                builderClear.bindTo(builder).invoke();
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }
            },
            builder -> {
              try {
                @SuppressWarnings("unchecked")
                E entity = (E) builderBuild.bindTo(builder).invoke();
                return entity;
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }
            });
      } catch (Exception e) {
        throw new RuntimeException("Failed to setup Avro schema mapping for " + name, e);
      }
    }

    private static Class<?> asPrimitiveType(Class<?> elementType) {
      if (elementType == Boolean.class) {
        return boolean.class;
      }
      if (elementType == Character.class) {
        return char.class;
      }
      if (elementType == Byte.class) {
        return byte.class;
      }
      if (elementType == Short.class) {
        return short.class;
      }
      if (elementType == Integer.class) {
        return int.class;
      }
      if (elementType == Long.class) {
        return long.class;
      }
      if (elementType == Float.class) {
        return float.class;
      }
      if (elementType == Double.class) {
        return double.class;
      }
      return elementType;
    }

    private static Schema loadSchema(String name) {
      String resourceName =
          String.format("org/projectnessie/catalog/formats/iceberg/%s.avro.json", name);
      try {
        ClassLoader classLoader = AvroLazyInit.class.getClassLoader();
        URL url = classLoader.getResource(resourceName);
        if (url == null) {
          throw new RuntimeException("Avro schema JSON " + resourceName + " missing on class path");
        }
        URLConnection urlConnection = url.openConnection();
        try (InputStream input = new BufferedInputStream(urlConnection.getInputStream())) {
          // Cannot reuse the parser instance, because it keeps state
          Schema.Parser schemaParser = new Schema.Parser().setValidateDefaults(true);
          return schemaParser.parse(input);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to load Avro schema JSON " + resourceName, e);
      }
    }
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  public static Schema avroNullable(Schema schema) {
    if (schema.getType() == UNION) {
      Preconditions.checkArgument(
          isOptionSchema(schema), "Union schemas are not supported: %s", schema);
      return schema;
    } else {
      return Schema.createUnion(NULL_SCHEMA, schema);
    }
  }

  public static boolean isOptionSchema(Schema schema) {
    if (schema.getType() == UNION && schema.getTypes().size() == 2) {
      if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
        return true;
      } else {
        return schema.getTypes().get(1).getType() == Schema.Type.NULL;
      }
    }
    return false;
  }

  public static String makeCompatibleName(String name) {
    if (!validAvroName(name)) {
      return sanitize(name);
    }
    return name;
  }

  static boolean validAvroName(String name) {
    int length = name.length();
    Preconditions.checkArgument(length > 0, "Empty name");
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      return false;
    }

    for (int i = 1; i < length; i++) {
      char character = name.charAt(i);
      if (!(Character.isLetterOrDigit(character) || character == '_')) {
        return false;
      }
    }
    return true;
  }

  static String sanitize(String name) {
    int length = name.length();
    StringBuilder sb = new StringBuilder(name.length());
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      sb.append(sanitize(first));
    } else {
      sb.append(first);
    }

    for (int i = 1; i < length; i++) {
      char character = name.charAt(i);
      if (!(Character.isLetterOrDigit(character) || character == '_')) {
        sb.append(sanitize(character));
      } else {
        sb.append(character);
      }
    }
    return sb.toString();
  }

  private static String sanitize(char character) {
    if (Character.isDigit(character)) {
      return "_" + character;
    }
    return "_x" + Integer.toHexString(character).toUpperCase();
  }

  public static int decimalRequiredBytes(int precision) {
    Preconditions.checkArgument(
        precision >= 0 && precision < 40, "Unsupported decimal precision: %s", precision);
    return REQUIRED_LENGTH[precision];
  }

  private static final int[] MAX_PRECISION = new int[24];
  private static final int[] REQUIRED_LENGTH = new int[40];

  static {
    // for each length, calculate the max precision
    for (int len = 0; len < MAX_PRECISION.length; len += 1) {
      MAX_PRECISION[len] = (int) Math.floor(Math.log10(Math.pow(2, 8 * len - 1) - 1));
    }

    // for each precision, find the first length that can hold it
    for (int precision = 0; precision < REQUIRED_LENGTH.length; precision += 1) {
      REQUIRED_LENGTH[precision] = -1;
      for (int len = 0; len < MAX_PRECISION.length; len += 1) {
        // find the first length that can hold the precision
        if (precision <= MAX_PRECISION[len]) {
          REQUIRED_LENGTH[precision] = len;
          break;
        }
      }
      if (REQUIRED_LENGTH[precision] < 0) {
        throw new IllegalStateException(
            "Could not find required length for precision " + precision);
      }
    }
  }

  public static class LogicalMap extends LogicalType {
    static final String NAME = "map";
    private static final LogicalMap INSTANCE = new LogicalMap();

    public static LogicalMap logicalMap() {
      return INSTANCE;
    }

    private LogicalMap() {
      super(NAME);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      Preconditions.checkArgument(
          schema.getType() == Schema.Type.ARRAY,
          "Invalid type for map, must be an array: %s",
          schema);
      //      Preconditions.checkArgument(
      //        AvroSchemaUtil.isKeyValueSchema(schema.getElementType()),
      //        "Invalid key-value record: %s",
      //        schema.getElementType());
    }
  }
}
