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
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

@Schema(type = SchemaType.OBJECT, title = "GetMultipleContentsResponse")
@Value.Immutable
@JsonSerialize(as = ImmutableGetMultipleContentsResponse.class)
@JsonDeserialize(as = ImmutableGetMultipleContentsResponse.class)
public interface GetMultipleContentsResponse {

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Value.Parameter(order = 1)
  List<ContentWithKey> getContents();

  /**
   * The effective reference (for example a branch or tag) including the commit ID from which the
   * contents were fetched. Never null when using REST API v2.
   */
  @Nullable // Only nullable in V1
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 2)
  @JsonView(Views.V2.class)
  Reference getEffectiveReference();

  @Value.NonAttribute
  default Map<ContentKey, Content> toContentsMap() {
    return getContents().stream()
        .collect(Collectors.toMap(ContentWithKey::getKey, ContentWithKey::getContent));
  }

  static GetMultipleContentsResponse of(List<ContentWithKey> items, Reference effectiveReference) {
    return ImmutableGetMultipleContentsResponse.of(items, effectiveReference);
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableContentWithKey.class)
  @JsonDeserialize(as = ImmutableContentWithKey.class)
  interface ContentWithKey {

    @NotNull
    @jakarta.validation.constraints.NotNull
    @Value.Parameter(order = 1)
    ContentKey getKey();

    @NotNull
    @jakarta.validation.constraints.NotNull
    @Value.Parameter(order = 2)
    Content getContent();

    @Nullable
    @jakarta.annotation.Nullable
    @JsonView(Views.V2.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Value.Parameter(order = 3)
    Documentation getDocumentation();

    static ContentWithKey of(ContentKey key, Content content) {
      return of(key, content, null);
    }

    static ContentWithKey of(ContentKey key, Content content, Documentation documentation) {
      return ImmutableContentWithKey.of(key, content, documentation);
    }
  }
}
