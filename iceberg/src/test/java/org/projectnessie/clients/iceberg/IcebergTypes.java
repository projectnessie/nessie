/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.clients.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.view.BaseVersion;
import org.apache.iceberg.view.HistoryEntry;
import org.apache.iceberg.view.Version;
import org.apache.iceberg.view.VersionSummary;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.ViewVersionMetadata;

public class IcebergTypes {
  static final Type[] PARTITION_PRIMITIVE_TYPES = {
    Types.LongType.get(),
    Types.IntegerType.get(),
    Types.StringType.get(),
    Types.BinaryType.get(),
    Types.DateType.get(),
    Types.TimeType.get(),
    Types.UUIDType.get(),
    Types.TimestampType.withZone(),
    Types.TimestampType.withoutZone()
  };
  static final Type[] PRIMITIVE_TYPES = {
    Types.DoubleType.get(),
    Types.LongType.get(),
    Types.FloatType.get(),
    Types.IntegerType.get(),
    Types.StringType.get(),
    Types.BinaryType.get(),
    Types.BooleanType.get(),
    Types.DateType.get(),
    Types.TimeType.get(),
    Types.UUIDType.get(),
    Types.TimestampType.withZone(),
    Types.TimestampType.withoutZone()
  };

  public static ViewVersionMetadata randomViewMetadata(int numColumns) {
    Version version = randomVersion(numColumns);
    return new ViewVersionMetadata(
        randomColumnName(),
        version.viewDefinition(),
        randomProperties(),
        version.versionId(),
        Collections.singletonList(version),
        Collections.singletonList(HistoryEntry.of(version.timestampMillis(), version.versionId())));
  }

  private static Version randomVersion(int numColumns) {
    ViewDefinition definition = randomViewDefinition(numColumns);
    int versionId = ThreadLocalRandom.current().nextInt(42666);

    return new BaseVersion(
        versionId,
        null,
        ThreadLocalRandom.current().nextLong(),
        new VersionSummary(randomProperties()),
        definition);
  }

  private static ViewDefinition randomViewDefinition(int numColumns) {
    List<NestedField> fields = randomFields(numColumns);
    Schema schema = new Schema(StructType.of(fields).fields());
    return ViewDefinition.of(
        randomColumnName(),
        schema,
        randomColumnName(),
        Arrays.asList(randomColumnName(), randomColumnName(), randomColumnName()));
  }

  public static TableMetadata randomTableMetadata(int numColumns) {
    List<NestedField> fields = randomFields(numColumns);

    Schema schema = new Schema(StructType.of(fields).fields());

    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket(fields.get(0).name(), 42).build();

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .sortBy(fields.get(1).name(), SortDirection.ASC, NullOrder.NULLS_LAST)
            .sortBy(fields.get(2).name(), SortDirection.DESC, NullOrder.NULLS_FIRST)
            .build();

    return TableMetadata.newTableMetadata(schema, spec, sortOrder, "foo://bar", randomProperties());
  }

  private static Map<String, String> randomProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(randomColumnName(), randomColumnName());
    return properties;
  }

  private static List<NestedField> randomFields(int numColumns) {
    List<NestedField> fields = Lists.newArrayList();
    fields.add(required(1, randomColumnName(), randomType(PARTITION_PRIMITIVE_TYPES)));
    for (int i = 2; i <= numColumns; i++) {
      fields.add(required(i, randomColumnName(), randomType(PRIMITIVE_TYPES)));
    }
    return fields;
  }

  public static Type randomType(Type[] types) {
    int idx = ThreadLocalRandom.current().nextInt(types.length);
    return types[idx];
  }

  public static String randomColumnName() {
    StringBuilder sb = new StringBuilder(30);
    int columnNameLength = ThreadLocalRandom.current().nextInt(5, 30);
    for (int i = 0; i < columnNameLength; i++) {
      sb.append(randomChar());
    }
    return sb.toString();
  }

  private static char randomChar() {
    return (char) ThreadLocalRandom.current().nextInt('a', 'z' + 1);
  }
}
