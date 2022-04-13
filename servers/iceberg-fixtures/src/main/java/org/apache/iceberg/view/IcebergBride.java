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
package org.apache.iceberg.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import org.apache.iceberg.util.JsonUtil;

public final class IcebergBride {

  private IcebergBride() {}

  public static ViewVersionMetadata parseJsonAsViewVersionMetadata(JsonNode nessieViewMetadata) {
    return ViewVersionMetadataParser.fromJson(null, nessieViewMetadata);
  }

  public static String viewVersionMetadataToJson(ViewVersionMetadata metadata) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      ViewVersionMetadataParser.toJson(metadata, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write json", e);
    }
  }

  public static HistoryEntry historyEntry(long timestampMillis, int versionId) {
    return new VersionLogEntry(timestampMillis, versionId);
  }
}
