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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.ser.Views;

@Schema(type = SchemaType.OBJECT, title = "Merge Response")
@Value.Immutable
@JsonSerialize(as = ImmutableMergeResponse.class)
@JsonDeserialize(as = ImmutableMergeResponse.class)
public interface MergeResponse {
  /** Indicates whether the merge or transplant operation has been applied. */
  @Value.Default
  default boolean wasApplied() {
    return false;
  }

  /** Indicates whether the merge or transplant operation was successful without any conflicts. */
  @Value.Default
  default boolean wasSuccessful() {
    return false;
  }

  /** Commit-ID of the target branch after the merge/transplant operation. */
  @Nullable
  @jakarta.annotation.Nullable
  String getResultantTargetHash();

  /**
   * Commit-ID of the identified merge base, only returned for a merge operation.
   *
   * <p>Note: earlier Nessie versions only supported merging using the common ancestor, so only
   * considering the direct commit parents (predecessors). Nessie identifies the "nearest"
   * merge-base singe version 0.61.0 (with the new storage model), and allows "incremental merges".
   * Since renaming a public API fields is not good practice, this field represents the merge-base.
   */
  @Nullable
  @jakarta.annotation.Nullable
  String getCommonAncestor();

  /** Name of the target branch. */
  String getTargetBranch();

  /** Head commit-ID of the target branch identified by the merge or transplant operation. */
  String getEffectiveTargetHash();

  /** The expected commit-ID of the target branch, as specified by the caller. */
  @Nullable
  @jakarta.annotation.Nullable
  String getExpectedHash();

  /**
   * This field is deprecated and will be removed in a future version. It always returns an empty
   * list with the current Nessie version.
   *
   * @deprecated for removal and replaced with something else.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated // for removal and replaced with something else
  @Schema(deprecated = true, hidden = true)
  @JsonView(Views.V1.class)
  List<LogEntry> getSourceCommits();

  /**
   * This field is deprecated and will be removed in a future version. It always returns null with
   * the current Nessie version.
   *
   * @deprecated for removal and replaced with something else.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Nullable
  @jakarta.annotation.Nullable
  @Deprecated // for removal and replaced with something else
  @Schema(deprecated = true, hidden = true)
  @JsonView(Views.V1.class)
  List<LogEntry> getTargetCommits();

  /** Details of all keys encountered during the merge or transplant operation. */
  List<ContentKeyDetails> getDetails();

  @Schema(type = SchemaType.OBJECT, title = "Merge Per-Content-Key details")
  @Value.Immutable
  @JsonSerialize(as = ImmutableContentKeyDetails.class)
  @JsonDeserialize(as = ImmutableContentKeyDetails.class)
  interface ContentKeyDetails {
    ContentKey getKey();

    MergeBehavior getMergeBehavior();

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated // for removal, #getConflict() is a proper replacement
    @Value.Default
    @JsonDeserialize(using = ContentKeyConflict.Deserializer.class)
    @Schema(deprecated = true, hidden = true)
    @JsonView(Views.V1.class)
    default ContentKeyConflict getConflictType() {
      return ContentKeyConflict.NONE;
    }

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated // for removal and replaced with something else
    @Schema(deprecated = true, hidden = true)
    @JsonView(Views.V1.class)
    List<String> getSourceCommits();

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated // for removal and replaced with something else
    @Schema(deprecated = true, hidden = true)
    @JsonView(Views.V1.class)
    List<String> getTargetCommits();

    /** {@link Conflict} details, if available. */
    @JsonInclude(Include.NON_NULL)
    @JsonView(Views.V2.class)
    @Nullable
    @jakarta.annotation.Nullable
    Conflict getConflict();
  }

  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated // for removal
  enum ContentKeyConflict {
    NONE,
    UNRESOLVABLE;

    public static ContentKeyConflict parse(String mergeBehavior) {
      try {
        if (mergeBehavior != null) {
          return ContentKeyConflict.valueOf(mergeBehavior.toUpperCase(Locale.ROOT));
        }
        return null;
      } catch (IllegalArgumentException e) {
        return UNRESOLVABLE;
      }
    }

    static final class Deserializer extends JsonDeserializer<ContentKeyConflict> {
      @Override
      public ContentKeyConflict deserialize(JsonParser p, DeserializationContext ctxt)
          throws IOException {
        String name = p.readValueAs(String.class);
        return name != null ? ContentKeyConflict.parse(name) : null;
      }
    }
  }
}
