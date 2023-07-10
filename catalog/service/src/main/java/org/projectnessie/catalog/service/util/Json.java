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
package org.projectnessie.catalog.service.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.rest.RESTSerializers;

public final class Json {

  private Json() {}

  public static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);
    return mapper;
  }

  public static TableMetadata jsonToTableMetadata(
      String metadataLocation, JsonNode tableMetadataJson) {
    // TODO change to org.apache.iceberg.TableMetadataParser#fromJson(java.lang.String,
    // com.fasterxml.jackson.databind.JsonNode) once it is public
    return TableMetadataParser.fromJson(metadataLocation, tableMetadataJson.toString());
  }

  @FunctionalInterface
  interface JsonNodeParse<T> {
    void objectToJsonNode(T object, JsonGenerator jsonGenerator) throws IOException;
  }

  private static <T> JsonNode toJsonNode(T obj, JsonNodeParse<T> jsonNodeParse) {
    TokenBuffer tokenBuffer = new TokenBuffer(Json.OBJECT_MAPPER, false);
    try {
      jsonNodeParse.objectToJsonNode(obj, tokenBuffer);
      JsonParser parser = tokenBuffer.asParser();
      return parser.readValueAsTree();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static JsonNode tableMetadataToJsonNode(TableMetadata tableMetadata) {
    return toJsonNode(tableMetadata, TableMetadataParser::toJson);
  }

  private static <T> Map<String, Object> toJsonMap(T obj, JsonNodeParse<T> jsonNodeParse) {
    TokenBuffer tokenBuffer = new TokenBuffer(Json.OBJECT_MAPPER, false);
    try {
      jsonNodeParse.objectToJsonNode(obj, tokenBuffer);
      JsonParser parser = tokenBuffer.asParser();
      @SuppressWarnings("unchecked")
      Map<String, Object> r = parser.readValueAs(Map.class);
      return r;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Map<String, Object> snapshotUpdateSummaryToJsonMap(Snapshot snapshot) {
    Map<String, Object> node = new LinkedHashMap<>();
    node.put("sequence-number", snapshot.sequenceNumber());
    node.put("timestamp-ms", snapshot.timestampMillis());
    Map<String, Object> summary = new LinkedHashMap<>();
    node.put("summary", summary);
    summary.put("operation", snapshot.operation());
    summary.putAll(snapshot.summary());
    return node;
  }

  public static Map<String, Object> snapshotToJsonMap(Snapshot snapshot) {
    // TODO this would be easier, but it is not public :(
    //  return toJsonNode(snapshot, SnapshotParser::toJson);
    try {
      String jsonString = SnapshotParser.toJson(snapshot);
      @SuppressWarnings("unchecked")
      Map<String, Object> r = OBJECT_MAPPER.readValue(jsonString, Map.class);
      return r;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Map<String, Object> schemaToJsonMap(Schema schema) {
    return toJsonMap(schema, SchemaParser::toJson);
  }

  public static Map<String, Object> partitionSpecToJsonMap(PartitionSpec partitionSpec) {
    return toJsonMap(partitionSpec, PartitionSpecParser::toJson);
  }

  public static Map<String, Object> sortOrderToJsonMap(SortOrder sortOrder) {
    return toJsonMap(sortOrder, SortOrderParser::toJson);
  }
}
