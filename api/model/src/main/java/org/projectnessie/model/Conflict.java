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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.Locale;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.immutables.value.Value;

@Schema(
    type = SchemaType.OBJECT,
    title = "Per Content Key conflict details",
    properties = {
      @SchemaProperty(name = "conflictType", description = "Conflict type (enum)."),
      @SchemaProperty(name = "key", description = "The conflicting Content Key."),
      @SchemaProperty(name = "message", description = "Conflict details.")
    })
@Value.Immutable
@JsonSerialize(as = ImmutableConflict.class)
@JsonDeserialize(as = ImmutableConflict.class)
@JsonIgnoreProperties(
    // need to ignore unknown properties as this type can be used in `ReferenceConflicts`
    ignoreUnknown = true)
public interface Conflict {
  @Value.Parameter(order = 1)
  @JsonDeserialize(using = ConflictType.Deserializer.class)
  ConflictType conflictType();

  @Value.Parameter(order = 2)
  @Nullable
  @jakarta.annotation.Nullable
  ContentKey key();

  @Value.Parameter(order = 3)
  String message();

  static Conflict conflict(
      ConflictType conflictType,
      @Nullable @jakarta.annotation.Nullable ContentKey key,
      String message) {
    return ImmutableConflict.of(conflictType, key, message);
  }

  enum ConflictType {
    /**
     * Unknown, for situations when the server returned a conflict type that is unknown to the
     * client.
     */
    UNKNOWN,

    /** The key exists, but is expected to not exist. */
    KEY_EXISTS,

    /** The key does not exist, but is expected to exist. */
    KEY_DOES_NOT_EXIST,

    /** Payload of existing and expected differ. */
    PAYLOAD_DIFFERS,

    /** Content IDs of existing and expected content differs. */
    CONTENT_ID_DIFFERS,

    /** Values of existing and expected content differs. */
    VALUE_DIFFERS,

    /** The mandatory parent namespace does not exist. */
    NAMESPACE_ABSENT,

    /** The key expected to be a namespace is not a namespace. */
    NOT_A_NAMESPACE,

    /** A namespace must be empty before it can be deleted. */
    NAMESPACE_NOT_EMPTY,

    /** Reference is not at the expected hash, reported for reference assignment and deletion. */
    UNEXPECTED_HASH,

    /** Generic key conflict, reported for merges and transplants. */
    KEY_CONFLICT,

    /** Values of existing and expected documentation differs. */
    DOCUMENTATION_DIFFERS;

    public static ConflictType parse(String conflictType) {
      try {
        if (conflictType != null) {
          return ConflictType.valueOf(conflictType.toUpperCase(Locale.ROOT));
        }
        return null;
      } catch (IllegalArgumentException e) {
        return UNKNOWN;
      }
    }

    public static final class Deserializer extends JsonDeserializer<ConflictType> {

      @Override
      public ConflictType getNullValue(DeserializationContext ctxt) {
        return UNKNOWN;
      }

      @Override
      public ConflictType deserialize(JsonParser p, DeserializationContext ctxt)
          throws IOException {
        String name = p.readValueAs(String.class);
        return ConflictType.parse(name);
      }
    }
  }
}
