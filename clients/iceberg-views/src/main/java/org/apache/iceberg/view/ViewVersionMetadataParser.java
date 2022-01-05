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
package org.apache.iceberg.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.JsonUtil;

public class ViewVersionMetadataParser {

  private ViewVersionMetadataParser() {}

  // visible for testing
  static final String FORMAT_VERSION = "format-version";
  static final String LOCATION = "location";
  static final String CURRENT_VERSION_ID = "current-version-id";
  static final String VERSIONS = "versions";
  static final String VERSION_ID = "version-id";
  static final String TIMESTAMP_MS = "timestamp-ms";
  static final String VERSION_LOG = "version-log";
  private static final String PROPERTIES = "properties";

  public static void overwrite(ViewVersionMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(ViewVersionMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  public static void internalWrite(
      ViewVersionMetadata metadata, OutputFile outputFile, boolean overwrite) {
    OutputStream stream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
    try (OutputStreamWriter writer = new OutputStreamWriter(stream, "UTF-8")) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json to file: %s", outputFile);
    }
  }

  public static String toJson(ViewVersionMetadata metadata) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(metadata, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json");
    }
  }

  public static void toJson(ViewVersionMetadata metadata, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(FORMAT_VERSION, ViewVersionMetadata.VIEW_FORMAT_VERSION);
    generator.writeStringField(LOCATION, metadata.location());
    generator.writeNumberField(CURRENT_VERSION_ID, metadata.currentVersionId());
    generator.writeObjectFieldStart(PROPERTIES);
    for (Map.Entry<String, String> keyValue : metadata.properties().entrySet()) {
      generator.writeStringField(keyValue.getKey(), keyValue.getValue());
    }
    generator.writeEndObject();

    generator.writeArrayFieldStart(VERSIONS);
    for (Version version : metadata.versions()) {
      VersionParser.toJson(version, generator);
    }
    generator.writeEndArray();

    generator.writeArrayFieldStart(VERSION_LOG);
    for (HistoryEntry logEntry : metadata.history()) {
      generator.writeStartObject();
      generator.writeNumberField(TIMESTAMP_MS, logEntry.timestampMillis());
      generator.writeNumberField(VERSION_ID, logEntry.versionId());
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  public static ViewVersionMetadata read(InputFile file) {
    try (InputStream is = file.newStream()) {
      return fromJson(file, JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read file: %s", file);
    }
  }

  public static ViewVersionMetadata fromJson(String json) {
    try (Reader is = new StringReader(json)) {
      return fromJson(null, JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to parse JSON");
    }
  }

  static ViewVersionMetadata fromJson(InputFile file, JsonNode node) {
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse metadata from a non-object: %s", node);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(
        formatVersion == ViewVersionMetadata.VIEW_FORMAT_VERSION,
        "Cannot read unsupported version %s",
        formatVersion);

    String location = JsonUtil.getString(LOCATION, node);

    int currentVersionId = JsonUtil.getInt(CURRENT_VERSION_ID, node);

    Map<String, String> properties = new HashMap<>();
    if (node.has(PROPERTIES)) {
      properties.putAll(JsonUtil.getStringMap(PROPERTIES, node));
    }

    JsonNode versionArray = node.get(VERSIONS);
    Preconditions.checkArgument(
        versionArray.isArray(), "Cannot parse versions from non-array: %s", versionArray);

    List<Version> versions = Lists.newArrayListWithExpectedSize(versionArray.size());
    Iterator<JsonNode> iterator = versionArray.elements();
    while (iterator.hasNext()) {
      versions.add(VersionParser.fromJson(iterator.next()));
    }

    SortedSet<VersionLogEntry> entries =
        Sets.newTreeSet(Comparator.comparingLong(VersionLogEntry::timestampMillis));
    if (node.has(VERSION_LOG)) {
      Iterator<JsonNode> logIterator = node.get(VERSION_LOG).elements();
      while (logIterator.hasNext()) {
        JsonNode entryNode = logIterator.next();
        entries.add(
            new VersionLogEntry(
                JsonUtil.getLong(TIMESTAMP_MS, entryNode), JsonUtil.getInt(VERSION_ID, entryNode)));
      }
    }

    if (versions.get(versions.size() - 1).versionId() != currentVersionId) {
      throw new RuntimeIOException(
          "Version history is corrupt. Latest version info with id: %s does not match current version id: %s",
          versions.get(versions.size() - 1).versionId(), currentVersionId);
    }
    ViewDefinition viewMetadata = versions.get(versions.size() - 1).viewDefinition();
    return new ViewVersionMetadata(
        location,
        viewMetadata,
        properties,
        currentVersionId,
        versions,
        ImmutableList.copyOf(entries.iterator()));
  }
}
