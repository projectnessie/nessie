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
package org.projectnessie.model;

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

@Schema(type = SchemaType.OBJECT, title = "Commit Response")
@Value.Immutable
@JsonSerialize(as = ImmutableCommitResponse.class)
@JsonDeserialize(as = ImmutableCommitResponse.class)
public interface CommitResponse {

  static ImmutableCommitResponse.Builder builder() {
    return ImmutableCommitResponse.builder();
  }

  /**
   * Returns updated information about the branch where the commit was applied.
   *
   * <p>Specifically, the hash of the {@link Branch} will be the hash of the applied commit.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  Branch getTargetBranch();

  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable // for V1 backwards compatibility
  List<AddedContent> getAddedContents();

  @Value.NonAttribute
  default Map<ContentKey, String> toAddedContentsMap() {
    List<AddedContent> added = getAddedContents();
    if (added == null) {
      throw new UnsupportedOperationException("toAddedContentsMap is not available in API v1");
    }
    return added.stream().collect(Collectors.toMap(AddedContent::getKey, AddedContent::contentId));
  }

  /**
   * If new content has been added to Nessie for the given {@link ContentKey key}, updates the
   * {@link Content#getId() content ID} of the given {@link Content content} with the content ID
   * returned by Nessie.
   *
   * <p>Returns the {@code content} parameter value, if no content for the given {@code key} has
   * been added.
   *
   * <p>Note: This convenience function only works for REST API v2.
   */
  @SuppressWarnings("unchecked")
  default <T extends Content> T contentWithAssignedId(ContentKey key, T content) {
    List<AddedContent> addedContents = getAddedContents();
    if (addedContents == null) {
      throw new UnsupportedOperationException("toAddedContentsMap is not available in API v1");
    }
    return (T)
        getAddedContents().stream()
            .filter(added -> added.getKey().equals(key))
            .findFirst()
            .map(added -> content.withId(added.contentId()))
            .orElse(content);
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableAddedContent.class)
  @JsonDeserialize(as = ImmutableAddedContent.class)
  interface AddedContent {
    @NotNull
    @jakarta.validation.constraints.NotNull
    @Value.Parameter(order = 1)
    ContentKey getKey();

    @NotNull
    @jakarta.validation.constraints.NotNull
    @Value.Parameter(order = 2)
    String contentId();

    static AddedContent addedContent(ContentKey key, String contentId) {
      return ImmutableAddedContent.of(key, contentId);
    }
  }
}
