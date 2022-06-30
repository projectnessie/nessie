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
package org.projectnessie.iceberg.metadata;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.iceberg.view.IcebergBride.historyEntry;
import static org.apache.iceberg.view.IcebergBride.parseJsonAsViewVersionMetadata;
import static org.apache.iceberg.view.IcebergBride.viewVersionMetadataToJson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.view.BaseVersion;
import org.apache.iceberg.view.Version;
import org.apache.iceberg.view.VersionSummary;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.ViewVersionMetadata;

public final class NessieIceberg {
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

  // Deterministic w/ constant seed
  static final Random RANDOM = new Random(42L);

  private NessieIceberg() {}

  public static ViewVersionMetadata randomViewMetadata(int numColumns) {
    Version version = randomVersion(numColumns);
    return new ViewVersionMetadata(
        randomColumnName(),
        version.viewDefinition(),
        randomProperties(1),
        version.versionId(),
        Collections.singletonList(version),
        Collections.singletonList(historyEntry(version.timestampMillis(), version.versionId())));
  }

  private static Version randomVersion(int numColumns) {
    ViewDefinition definition = randomViewDefinition(numColumns);
    int versionId = RANDOM.nextInt(42666);

    return new BaseVersion(
        versionId, null, RANDOM.nextLong(), new VersionSummary(randomProperties(0)), definition);
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

    TableMetadata metadata = icebergTableMetadata(fields, schema);

    Snapshot snapshot2 = randomSnapshot(metadata);
    metadata = TableMetadata.buildFrom(metadata).setCurrentSnapshot(snapshot2).build();

    Snapshot snapshot3 = randomSnapshot(metadata);
    metadata = TableMetadata.buildFrom(metadata).setCurrentSnapshot(snapshot3).build();

    return TableMetadataParser.fromJson(
        null, "bar://metadata/location.json", TableMetadataParser.toJson(metadata));
  }

  public static Snapshot randomSnapshot(TableMetadata metadata) {
    return ImmutableDummySnapshot.builder()
        .snapshotId(RANDOM.nextLong())
        .sequenceNumber(metadata.lastSequenceNumber() + 1)
        .timestampMillis(metadata.lastUpdatedMillis() + RANDOM.nextInt(1_000_000))
        .operation("snapshotOperation")
        .build();
  }

  public static Map<String, String> randomProperties(int formatVersion) {
    Map<String, String> properties = new HashMap<>();
    if (formatVersion > 0) {
      properties.put("format-version", Integer.toString(formatVersion));
    }
    properties.put(randomColumnName(), randomColumnName());
    properties.put(randomColumnName(), randomColumnName());
    return properties;
  }

  public static List<NestedField> randomFields(int numColumns) {
    List<NestedField> fields = new ArrayList<>();
    fields.add(required(1, randomColumnName(), randomType(PARTITION_PRIMITIVE_TYPES)));
    for (int i = 2; i <= numColumns; i++) {
      fields.add(required(i, randomColumnName(), randomType(PRIMITIVE_TYPES)));
    }
    return fields;
  }

  public static Type randomType(Type[] types) {
    int idx = RANDOM.nextInt(types.length);
    return types[idx];
  }

  public static String randomColumnName() {
    StringBuilder sb = new StringBuilder(30);
    int columnNameLength = 5 + RANDOM.nextInt(25);
    for (int i = 0; i < columnNameLength; i++) {
      sb.append(randomChar());
    }
    return sb.toString();
  }

  private static char randomChar() {
    return (char) (RANDOM.nextInt(26) + 'a');
  }

  public static JsonNode toNessie(ViewVersionMetadata view) {
    try {
      String viewJsonString = viewVersionMetadataToJson(view);
      return new ObjectMapper().readValue(viewJsonString, JsonNode.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static JsonNode toNessie(TableMetadata tableMetadata) {
    try {
      String tableJsonString = TableMetadataParser.toJson(tableMetadata);
      return new ObjectMapper().readValue(tableJsonString, JsonNode.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static TableMetadata toIceberg(FileIO io, JsonNode nessieTableMetadata) {
    return TableMetadataParser.fromJson(io, nessieTableMetadata.toString());
  }

  public static ViewVersionMetadata toIceberg(JsonNode nessieViewMetadata) {
    return parseJsonAsViewVersionMetadata(nessieViewMetadata);
  }

  public static JsonNode asJsonNode(String json) {
    try (JsonParser parser = new ObjectMapper().createParser(json)) {
      return parser.readValueAs(JsonNode.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static TableMetadata icebergTableMetadata(List<NestedField> fields, Schema schema) {
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket(fields.get(0).name(), 42).build();

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .sortBy(fields.get(1).name(), SortDirection.ASC, NullOrder.NULLS_LAST)
            .sortBy(fields.get(2).name(), SortDirection.DESC, NullOrder.NULLS_FIRST)
            .build();

    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, spec, sortOrder, "foo://bar", randomProperties(2));

    Snapshot snapshot1 = randomSnapshot(metadata);
    metadata = TableMetadata.buildFrom(metadata).setCurrentSnapshot(snapshot1).build();
    return metadata;
  }
}
