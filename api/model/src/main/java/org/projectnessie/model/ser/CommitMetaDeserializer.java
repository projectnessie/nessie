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
package org.projectnessie.model.ser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Map;
import org.projectnessie.model.CommitMeta;

public class CommitMetaDeserializer extends StdDeserializer<CommitMeta> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public CommitMetaDeserializer() {
    super(CommitMeta.class);
  }

  private void toArray(ObjectNode node, String singleAttr, String arrayAttr) {
    if (!node.has(arrayAttr)) {
      ArrayNode array = node.withArray(arrayAttr);
      if (node.has(singleAttr)) {
        JsonNode value = node.get(singleAttr);
        if (!value.isNull()) {
          array.add(value);
        }
      }
    }
    node.remove(singleAttr);
  }

  private void toMapOfLists(ObjectNode node, String singleAttr, String arrayAttr) {
    if (!node.has(arrayAttr)) {
      ObjectNode mapOfLists = node.putObject(arrayAttr);
      if (node.has(singleAttr) && node.get(singleAttr).isObject()) {
        ObjectNode map = (ObjectNode) node.get(singleAttr);
        for (Map.Entry<String, JsonNode> entry : map.properties()) {
          mapOfLists.putArray(entry.getKey()).add(entry.getValue());
        }
      }
    }
    node.remove(singleAttr);
  }

  @Override
  public CommitMeta deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
    // First, parse the serialized data as a generic object
    ObjectNode node = p.readValueAs(ObjectNode.class);
    // Convert old properties to new, array-based variants
    toArray(node, "author", "authors");
    toArray(node, "signedOffBy", "allSignedOffBy");
    toMapOfLists(node, "properties", "allProperties");

    // Parse again using the standard a Bean deserializer to avoid infinite recursion.
    // This adds a bit extra runtime work, but keeps the code simpler.
    // Note: we do not use any Json views here. All attributes are processed. However,
    // attributes specific to the v1 serialized form have been removed by the `toArray`
    // methods above.
    CommitMetaSer value = MAPPER.convertValue(node, CommitMetaSer.class);
    return CommitMeta.builder().from(value).build();
  }
}
