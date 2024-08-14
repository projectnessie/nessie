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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

@Schema(
    type = SchemaType.OBJECT,
    title = "Operation",
    description =
        "Describes an operation to be performed against one content object.\n"
            + "\n"
            + "The Nessie backend will validate the correctness of the operations.",
    oneOf = {Operation.Put.class, Operation.Unchanged.class, Operation.Delete.class},
    discriminatorMapping = {
      @DiscriminatorMapping(value = "PUT", schema = Operation.Put.class),
      @DiscriminatorMapping(value = "UNCHANGED", schema = Operation.Unchanged.class),
      @DiscriminatorMapping(value = "DELETE", schema = Operation.Delete.class)
    },
    discriminatorProperty = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(Operation.Put.class),
  @JsonSubTypes.Type(Operation.Delete.class),
  @JsonSubTypes.Type(Operation.Unchanged.class)
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface Operation {

  @NotNull
  @jakarta.validation.constraints.NotNull
  ContentKey getKey();

  @Schema(
      type = SchemaType.OBJECT,
      title = "Put-'Content'-operation for a 'ContentKey'.",
      description =
          "Used to add new content or to update existing content.\n"
              + "\n"
              + "A new content object is created by populating the `value` field, the "
              + "content-id in the content object must not be present (null).\n"
              + "\n"
              + "A content object is updated by populating the `value` containing the correct "
              + "content-id.\n"
              + "\n"
              + "If the key for a content shall change (aka a rename), then use a `Delete` "
              + "operation using the current (old) key and a `Put` operation using the new key "
              + "with the `value` having the correct content-id. Both operations must happen "
              + "in the same commit.\n"
              + "\n"
              + "A content object can be replaced (think: `DROP TABLE xyz` + `CREATE TABLE xyz`) "
              + "with a `Delete` operation and a `Put` operation for a content using a `value`"
              + "representing a new content object, so without a content-id, in the same commit.")
  @Value.Immutable
  @JsonSerialize(as = ImmutablePut.class)
  @JsonDeserialize(as = ImmutablePut.class)
  @JsonTypeName("PUT")
  interface Put extends Operation {
    @NotNull
    @jakarta.validation.constraints.NotNull
    Content getContent();

    @Nullable
    @jakarta.annotation.Nullable
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    @JsonView(Views.V1.class)
    Content getExpectedContent();

    /**
     * Additional information about the operation and/or content object. If and how a Nessie server
     * uses and handles the information depends on the server version and type of metadata (called
     * variant).
     */
    @JsonInclude(Include.NON_EMPTY)
    @JsonView(Views.V2.class)
    List<ContentMetadata> getMetadata();

    @Nullable
    @jakarta.annotation.Nullable
    @JsonView(Views.V2.class)
    Documentation getDocumentation();

    static Put of(ContentKey key, Content content) {
      return ImmutablePut.builder().key(key).content(content).build();
    }

    static Put of(ContentKey key, Content content, Documentation documentation) {
      return ImmutablePut.builder().key(key).content(content).documentation(documentation).build();
    }

    @Deprecated // for removal
    static Put of(ContentKey key, Content content, Content expectedContent) {
      return ImmutablePut.builder()
          .key(key)
          .content(content)
          .expectedContent(expectedContent)
          .build();
    }
  }

  @Schema(
      type = SchemaType.OBJECT,
      title = "Delete-'Content'-operation for a 'ContentKey'.",
      description =
          "Used to delete an existing content key.\n"
              + "\n"
              + "If the key for a content shall change (aka a rename), then use a `Delete` "
              + "operation using the current (old) key and a `Put` operation using the new key "
              + "with the current `Content` in the the `value` field. See `Put` operation.")
  @Value.Immutable
  @JsonSerialize(as = ImmutableDelete.class)
  @JsonDeserialize(as = ImmutableDelete.class)
  @JsonTypeName("DELETE")
  interface Delete extends Operation {

    static Delete of(ContentKey key) {
      return ImmutableDelete.builder().key(key).build();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableUnchanged.class)
  @JsonDeserialize(as = ImmutableUnchanged.class)
  @JsonTypeName("UNCHANGED")
  interface Unchanged extends Operation {

    static Unchanged of(ContentKey key) {
      return ImmutableUnchanged.builder().key(key).build();
    }
  }

  @Value.Check
  default void check() {
    if (getKey().getElementCount() == 0) {
      throw new IllegalStateException("Content key must not be empty");
    }
  }
}
