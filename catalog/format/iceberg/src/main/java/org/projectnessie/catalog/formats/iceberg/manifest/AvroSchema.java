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
import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Function.identity;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

final class AvroSchema<E, B> implements AvroTyped<E> {
  private final Schema schema;
  private final AvroField[] fields;
  private final Map<String, AvroField> fieldsByName;
  private final byte[] prefix;
  private final Class<? extends E> entityType;
  private final Supplier<? extends B> builderCreator;
  private final Consumer<? extends B> builderClear;
  private final Function<B, E> builderBuild;

  // TODO Optimizations:
  //  - re-use the builder instance (some externally provided context)
  //  - use immutable's `add*()` + `put*()` functions to prevent unnecessary collection instances

  AvroSchema(
      Schema schema,
      AvroField[] fields,
      byte[] prefix,
      Class<? extends E> entityType,
      Supplier<? extends B> builderCreator,
      Consumer<? extends B> builderClear,
      Function<B, E> builderBuild) {
    this.schema = schema;
    this.fields = fields;
    this.fieldsByName =
        Arrays.stream(fields).collect(Collectors.toMap(AvroField::name, identity()));
    this.prefix = prefix;
    this.entityType = entityType;
    this.builderCreator = builderCreator;
    this.builderClear = builderClear;
    this.builderBuild = builderBuild;
  }

  @Override
  public boolean handles(Class<?> type) {
    return entityType.isAssignableFrom(type);
  }

  @Override
  public Schema writeSchema(AvroReadWriteContext readWriteContext) {
    if (readWriteContext == null) {
      return schema;
    }
    Map<String, Schema> overrides = readWriteContext.schemaOverrides();
    if (overrides.isEmpty()) {
      return schema;
    }

    Set<String> updatedPaths = new HashSet<>();

    for (Map.Entry<String, Schema> override : overrides.entrySet()) {
      String property = override.getKey();
      String[] propertyElements = property.split("[.]");

      Schema current = schema;
      StringBuilder path = new StringBuilder();
      for (String elem : propertyElements) {
        Schema.Field field = current.getField(elem);
        checkArgument(
            field != null,
            "Property '%s' element '%s' does not exist in the Avro schema.",
            property,
            elem);

        path.append(elem);
        updatedPaths.add(path.toString());

        path.append('.');
        current = field.schema();
      }
    }

    return clonePartly(schema, updatedPaths, overrides, "");
  }

  private Schema clonePartly(
      Schema schema, Set<String> updatedPaths, Map<String, Schema> overrides, String path) {
    List<Schema.Field> schemaFields = schema.getFields();
    List<Schema.Field> newFields = new ArrayList<>(schemaFields.size());

    for (Schema.Field field : schemaFields) {
      String fullName = path + field.name();
      Schema override = overrides.get(fullName);
      Schema.Field newField;
      if (override != null) {
        newField =
            new Schema.Field(
                field.name(), override, field.doc(), field.defaultVal(), field.order());
        field.getObjectProps().forEach(newField::addProp);
      } else if (updatedPaths.contains(fullName)) {
        Schema newSchema = clonePartly(field.schema(), updatedPaths, overrides, fullName + '.');
        newField = new Schema.Field(field, newSchema);
      } else {
        newField = new Schema.Field(field, field.schema());
      }
      newFields.add(newField);
    }

    Schema newSchema =
        Schema.createRecord(
            schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), newFields);
    schema.getObjectProps().forEach(newSchema::addProp);
    schema.getAliases().forEach(newSchema::addAlias);
    return newSchema;
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public Schema schema(InputStream input) {
    if (prefix == null) {
      return schema;
    }

    try {
      DataInputStream dataInput = new DataInputStream(input);
      byte header0 = dataInput.readByte();
      byte header1 = dataInput.readByte();
      checkState(
          header0 == Avro.MAGIC_BYTES[0] && header1 == Avro.MAGIC_BYTES[1],
          "Unrecognized header bytes: 0x%02X 0x%02X",
          header0,
          header1);

      // Read avro schema
      return new Schema.Parser().parse(dataInput.readUTF());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AvroField field(int index) {
    return fields[index];
  }

  @Override
  public void prefix(OutputStream output) throws IOException {
    if (prefix != null) {
      output.write(prefix);
    }
  }

  @Override
  public void write(Encoder encoder, E object, Schema schema) throws IOException {
    if (schema == null) {
      schema = this.schema;
    }

    for (Schema.Field schemaField : schema.getFields()) {
      AvroField field = fieldsByName.get(schemaField.name());
      try {
        field.write(encoder, field.valueFrom(object));
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed to write field '" + schemaField.name(), e);
      }
    }
  }

  @Override
  public E read(Decoder decoder, Schema schema) {
    B builderInstance = builderCreator.get();

    if (schema == null) {
      schema = this.schema;
    }

    for (Schema.Field schemaField : schema.getFields()) {
      AvroField field = fieldsByName.get(schemaField.name());
      try {
        Object value = field.read(decoder, schemaField.schema());
        field.valueTo(builderInstance, value);
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed to read field '" + field.name() + "'", e);
      }
    }

    return builderBuild.apply(builderInstance);
  }
}
