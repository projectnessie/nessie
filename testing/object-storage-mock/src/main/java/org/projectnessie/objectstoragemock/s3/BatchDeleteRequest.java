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
package org.projectnessie.objectstoragemock.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import java.util.ArrayList;
import java.util.List;
import org.immutables.value.Value;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import tools.jackson.dataformat.xml.annotation.JacksonXmlProperty;

@JsonRootName("Delete")
@JsonSerialize(as = ImmutableBatchDeleteRequest.class)
@JsonDeserialize(using = BatchDeleteRequest.Deserializer.class)
@Value.Immutable
@Value.Style(jdkOnly = true)
public interface BatchDeleteRequest {

  @JsonProperty("Quiet")
  @Value.Default
  default boolean quiet() {
    return false;
  }

  @JsonProperty("Object")
  @JacksonXmlElementWrapper(useWrapping = false)
  @JacksonXmlProperty(localName = "Object")
  List<S3ObjectIdentifier> objectsToDelete();

  final class Deserializer extends ValueDeserializer<BatchDeleteRequest> {
    @Override
    public BatchDeleteRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws JacksonException {
      ImmutableBatchDeleteRequest.Builder builder = ImmutableBatchDeleteRequest.builder();
      if (p.currentToken() == null) {
        p.nextToken();
      }
      if (p.currentToken() != JsonToken.START_OBJECT) {
        return ctxt.reportInputMismatch(BatchDeleteRequest.class, "Expected S3 delete request");
      }

      for (JsonToken token = p.nextToken();
          token == JsonToken.PROPERTY_NAME;
          token = p.nextToken()) {
        String fieldName = p.currentName();
        JsonToken valueToken = p.nextToken();
        switch (fieldName) {
          case "Quiet" -> builder.quiet(valueToken == JsonToken.VALUE_TRUE);
          case "Object" -> builder.addAllObjectsToDelete(readObjectIdentifiers(p, ctxt));
          default -> p.skipChildren();
        }
      }

      return builder.build();
    }

    private static List<S3ObjectIdentifier> readObjectIdentifiers(
        JsonParser p, DeserializationContext ctxt) throws JacksonException {
      List<S3ObjectIdentifier> objects = new ArrayList<>();
      JsonToken token = p.currentToken();
      if (token == JsonToken.START_ARRAY) {
        while (p.nextToken() != JsonToken.END_ARRAY) {
          objects.add(readObjectIdentifier(p, ctxt));
        }
      } else {
        objects.add(readObjectIdentifier(p, ctxt));
      }
      return objects;
    }

    private static S3ObjectIdentifier readObjectIdentifier(
        JsonParser p, DeserializationContext ctxt) throws JacksonException {
      JsonToken token = p.currentToken();
      if (token == JsonToken.VALUE_STRING) {
        return S3ObjectIdentifier.of(p.getString());
      }
      if (token != JsonToken.START_OBJECT) {
        return ctxt.reportInputMismatch(S3ObjectIdentifier.class, "Invalid S3 object identifier");
      }

      String key = null;
      String versionId = null;
      for (token = p.nextToken(); token == JsonToken.PROPERTY_NAME; token = p.nextToken()) {
        String fieldName = p.currentName();
        JsonToken valueToken = p.nextToken();
        if ("Key".equals(fieldName)) {
          key = valueToken == JsonToken.VALUE_NULL ? null : p.getString();
        } else if ("VersionId".equals(fieldName)) {
          versionId = valueToken == JsonToken.VALUE_NULL ? null : p.getString();
        } else {
          p.skipChildren();
        }
      }

      if (key == null) {
        return ctxt.reportInputMismatch(S3ObjectIdentifier.class, "Missing S3 object key");
      }
      return S3ObjectIdentifier.of(key, versionId);
    }
  }
}
