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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.BaseMergeTransplant.MergeBehavior;
import org.projectnessie.model.LogResponse.LogEntry;

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
  String getResultantTargetHash();

  /** Commit-ID of the identified common ancestor, only returned for a merge operation. */
  @Nullable
  String getCommonAncestor();

  /** Name of the target branch. */
  String getTargetBranch();

  /** Head commit-ID of the target branch identified by the merge or transplant operation. */
  String getEffectiveTargetHash();

  /** The expected commit-ID of the target branch, as specified by the caller. */
  @Nullable
  String getExpectedHash();

  /** List of commit-IDs to be merged or transplanted. */
  List<LogEntry> getSourceCommits();

  /**
   * List of commit-IDs between {@link #getExpectedHash()} and {@link #getEffectiveTargetHash()}, if
   * the expected hash was provided.
   */
  @Nullable
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

    @Value.Default
    default ContentKeyConflict getConflictType() {
      return ContentKeyConflict.NONE;
    }

    List<String> getSourceCommits();

    List<String> getTargetCommits();
  }

  enum ContentKeyConflict {
    NONE,
    UNRESOLVABLE
  }
}
