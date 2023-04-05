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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Locale;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.Util.ConflictTypeDeserializer;

@Value.Immutable
@JsonSerialize(as = ImmutableConflict.class)
@JsonDeserialize(as = ImmutableConflict.class)
public interface Conflict {
  @Value.Parameter(order = 1)
  @Nullable
  @jakarta.annotation.Nullable
  @JsonDeserialize(using = ConflictTypeDeserializer.class)
  ConflictType conflictType();

  @Value.Parameter(order = 2)
  @Nullable
  @jakarta.annotation.Nullable
  ContentKey key();

  @Value.Parameter(order = 3)
  String message();

  static Conflict conflict(
      @Nullable @jakarta.annotation.Nullable ConflictType conflictType,
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

    /** Reference is not at the expected hash. */
    UNEXPECTED_HASH,

    /** Generic key conflict, reported for merges and transplants. */
    KEY_CONFLICT;

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
  }
}
