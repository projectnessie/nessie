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

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

@Value.Immutable
@JsonSerialize(as = ImmutableEntriesResponse.class)
@JsonDeserialize(as = ImmutableEntriesResponse.class)
public interface EntriesResponse extends PaginatedResponse {

  static ImmutableEntriesResponse.Builder builder() {
    return ImmutableEntriesResponse.builder();
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
  List<Entry> getEntries();

  /**
   * The effective reference (for example a branch or tag) including the commit ID from which the
   * entries were fetched. Never null when using REST API v2.
   */
  @Nullable // Only nullable in V1
  @jakarta.annotation.Nullable
  @JsonView(Views.V2.class)
  Reference getEffectiveReference();

  @Value.Immutable
  @JsonSerialize(as = ImmutableEntry.class)
  @JsonDeserialize(as = ImmutableEntry.class)
  interface Entry {

    static ImmutableEntry.Builder builder() {
      return ImmutableEntry.builder();
    }

    @NotNull
    @jakarta.validation.constraints.NotNull
    @Value.Parameter(order = 2)
    @Schema(ref = "#/components/schemas/Type") // workaround self-referencing 'ref'
    Content.Type getType();

    @NotNull
    @jakarta.validation.constraints.NotNull
    @Value.Parameter(order = 1)
    ContentKey getName();

    @JsonView(Views.V2.class)
    @Value.Parameter(order = 3)
    @Nullable
    @jakarta.annotation.Nullable // for V1 backwards compatibility
    String getContentId();

    @JsonView(Views.V2.class)
    @Value.Parameter(order = 4)
    @Nullable
    @jakarta.annotation.Nullable // for V1 backwards compatibility
    Content getContent();

    @Value.Check
    default void verify() {
      Content content = getContent();
      if (content == null) {
        return;
      }
      if (!content.getType().equals(getType())) {
        throw new IllegalArgumentException(
            "Content type '" + getType() + "' does not match actual type of content: " + content);
      }
      if (!content.getId().equals(getContentId())) {
        throw new IllegalArgumentException(
            "Content id '" + getContentId() + "' does not match actual id of content: " + content);
      }
    }

    static Entry entry(ContentKey name, Content.Type type) {
      return entry(name, type, (String) null);
    }

    static Entry entry(ContentKey name, Content.Type type, String contentId) {
      return ImmutableEntry.of(name, type, contentId, null);
    }

    static Entry entry(ContentKey name, Content.Type type, Content content) {
      return ImmutableEntry.of(name, type, content.getId(), content);
    }
  }
}
