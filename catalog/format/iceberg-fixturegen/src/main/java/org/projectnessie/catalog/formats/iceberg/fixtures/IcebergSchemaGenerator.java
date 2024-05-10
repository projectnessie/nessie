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
package org.projectnessie.catalog.formats.iceberg.fixtures;

import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.MIN_PARTITION_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.WRITE_METRICS_MAX_INFERRED_COLUMNS;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessiePartitionField;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Generates a simple, random Iceberg schema and partition spec, using a limited set of data types
 * (fixed-length: bigint, variable-length: text).
 */
public final class IcebergSchemaGenerator {
  private final IcebergSchema icebergSchema;
  private final IcebergPartitionSpec icebergPartitionSpec;
  private final Map<Integer, NessieField> fieldMap;
  private final Map<Integer, NessiePartitionField> partitionFieldsMap;
  private final NessieSchema nessieSchema;
  private final NessiePartitionDefinition nessiePartitionDefinition;
  private final Map<Integer, Supplier<Object>> partitionGenerators;
  private final Map<Integer, Supplier<Object>> columnGenerators;
  private final Map<Integer, IcebergType> icebergTypes;
  private final Schema avroPartitionSchema;

  private IcebergSchemaGenerator(IcebergGeneratorSpec spec) {
    this.icebergSchema = spec.schema.build();
    this.icebergPartitionSpec = spec.part.build();
    this.icebergTypes =
        icebergSchema.fields().stream()
            .collect(Collectors.toMap(IcebergNestedField::id, IcebergNestedField::type));
    this.fieldMap = new HashMap<>();
    this.partitionFieldsMap = new HashMap<>();
    this.nessieSchema = NessieModelIceberg.icebergSchemaToNessieSchema(icebergSchema, fieldMap);
    this.nessiePartitionDefinition =
        NessieModelIceberg.icebergPartitionSpecToNessie(
            icebergPartitionSpec, partitionFieldsMap, fieldMap);
    this.partitionGenerators = new LinkedHashMap<>(spec.partitionGenerators);
    this.columnGenerators = new LinkedHashMap<>(spec.columnGenerators);
    this.avroPartitionSchema = icebergPartitionSpec.avroSchema(icebergSchema, "r102");
  }

  public void generateColumns(
      BiConsumer<Integer, byte[]> lower, BiConsumer<Integer, byte[]> upper) {
    int col = 0;
    for (Map.Entry<Integer, Supplier<Object>> e : columnGenerators.entrySet()) {
      if (col++ >= WRITE_METRICS_MAX_INFERRED_COLUMNS) {
        break;
      }

      Integer fieldId = e.getKey();
      Supplier<Object> gen = e.getValue();
      IcebergType type = icebergTypes.get(fieldId);

      Object value1 = gen.get();
      Object value2 = gen.get();
      byte[] ser1 = type.serializeSingleValue(value1);
      byte[] ser2 = type.serializeSingleValue(value2);
      boolean oneIsLower = type.compare(value1, value2) <= 0;
      lower.accept(fieldId, oneIsLower ? ser1 : ser2);
      upper.accept(fieldId, oneIsLower ? ser2 : ser1);
    }
  }

  public IcebergSchema getIcebergSchema() {
    return icebergSchema;
  }

  public IcebergPartitionSpec getIcebergPartitionSpec() {
    return icebergPartitionSpec;
  }

  public Schema getAvroPartitionSchema() {
    return avroPartitionSchema;
  }

  public Map<Integer, NessieField> getFieldMap() {
    return fieldMap;
  }

  public NessieSchema getNessieSchema() {
    return nessieSchema;
  }

  public NessiePartitionDefinition getNessiePartitionDefinition() {
    return nessiePartitionDefinition;
  }

  public static IcebergGeneratorSpec spec() {
    return new IcebergGeneratorSpec();
  }

  public GenericData.Record generatePartitionRecord() {
    GenericData.Record record = new GenericData.Record(avroPartitionSchema);
    int i = 0;
    for (Supplier<Object> gen : partitionGenerators.values()) {
      // Can skip the serialize-deserialize round-trip, because there'll be only
      // java.lang.Long/String types.
      Object value = gen.get();
      record.put(i++, value);
    }

    return record;
  }

  public static final class IcebergGeneratorSpec {

    private final IcebergSchema.Builder schema = IcebergSchema.builder().schemaId(42);
    private final IcebergPartitionSpec.Builder part = IcebergPartitionSpec.builder().specId(43);

    private int fieldId = 1;
    private int colNum = 1;
    private int partNum = 1;

    private final Map<Integer, Supplier<Object>> partitionGenerators = new LinkedHashMap<>();
    private final Map<Integer, Supplier<Object>> columnGenerators = new LinkedHashMap<>();

    @CanIgnoreReturnValue
    public IcebergGeneratorSpec numColumns(int numColumns) {
      for (int i = 0; i < numColumns; i++, colNum++, fieldId++) {
        IcebergNestedField field =
            nestedField(fieldId, "col_" + colNum, true, IcebergType.longType(), null);
        schema.addFields(field);
        columnGenerators.put(fieldId, () -> ThreadLocalRandom.current().nextLong(0, 1_000_000));
      }
      return this;
    }

    @CanIgnoreReturnValue
    public IcebergGeneratorSpec numTextColumns(int amount, int minLength, int maxLength) {
      for (int i = 0; i < amount; i++, colNum++, fieldId++) {
        IcebergNestedField field =
            nestedField(fieldId, "col_" + colNum, true, IcebergType.stringType(), null);
        schema.addFields(field);
        columnGenerators.put(fieldId, () -> generateString(minLength, maxLength));
      }
      return this;
    }

    @CanIgnoreReturnValue
    public IcebergGeneratorSpec numPartitionColumns(int numPartitionColumns) {
      for (int i = 0; i < numPartitionColumns; i++, partNum++, fieldId++) {
        IcebergNestedField field =
            nestedField(fieldId, "part_" + partNum, true, IcebergType.longType(), null);
        schema.addFields(field);
        part.addFields(
            partitionField("p_" + partNum, "identity", fieldId, fieldId + MIN_PARTITION_ID));
        partitionGenerators.put(fieldId, () -> ThreadLocalRandom.current().nextLong(0, 1_000_000));
      }
      return this;
    }

    @CanIgnoreReturnValue
    public IcebergGeneratorSpec numTextPartitionColumns(int amount, int minLength, int maxLength) {
      for (int i = 0; i < amount; i++, partNum++, fieldId++) {
        IcebergNestedField field =
            nestedField(fieldId, "part_" + partNum, true, IcebergType.stringType(), null);
        schema.addFields(field);
        part.addFields(
            partitionField("p_" + partNum, "identity", fieldId, fieldId + MIN_PARTITION_ID));
        partitionGenerators.put(fieldId, () -> generateString(minLength, maxLength));
      }
      return this;
    }

    private String generateString(int minLength, int maxLength) {
      ThreadLocalRandom rand = ThreadLocalRandom.current();
      int len = rand.nextInt(minLength, maxLength);
      StringBuilder sb = new StringBuilder(len);
      for (int i = 0; i < len; i++) {
        sb.append((char) (32 + ThreadLocalRandom.current().nextInt(95)));
      }
      return sb.toString();
    }

    public IcebergSchemaGenerator generate() {
      return new IcebergSchemaGenerator(this);
    }

    @NessieImmutable
    abstract static class IcebergGenTextCols {
      abstract int amount();

      abstract int minLength();

      abstract int maxLength();

      static IcebergGenTextCols textCols(int amount, int minLength, int maxLength) {
        return ImmutableIcebergGenTextCols.of(amount, minLength, maxLength);
      }
    }
  }
}
