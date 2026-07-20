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

import org.immutables.value.Value;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = ImmutableS3ObjectIdentifier.class)
@JsonDeserialize(using = S3ObjectIdentifier.Deserializer.class)
@Value.Immutable
public interface S3ObjectIdentifier extends ObjectIdentifier {
  static S3ObjectIdentifier of(String key) {
    return of(key, null);
  }

  static S3ObjectIdentifier of(String key, String versionId) {
    return ImmutableS3ObjectIdentifier.of(key, versionId);
  }

  final class Deserializer extends ValueDeserializer<S3ObjectIdentifier> {
    @Override
    public S3ObjectIdentifier deserialize(JsonParser p, DeserializationContext ctxt)
        throws JacksonException {
      JsonNode node = ctxt.readTree(p);
      if (node.isString()) {
        return S3ObjectIdentifier.of(node.stringValue());
      }
      if (node.isObject()) {
        JsonNode key = node.get("Key");
        if (key == null || !key.isString()) {
          return ctxt.reportInputMismatch(S3ObjectIdentifier.class, "Missing S3 object key");
        }
        JsonNode versionId = node.get("VersionId");
        return S3ObjectIdentifier.of(
            key.stringValue(),
            versionId != null && versionId.isString() ? versionId.stringValue() : null);
      }
      return ctxt.reportInputMismatch(S3ObjectIdentifier.class, "Invalid S3 object identifier");
    }
  }
}
