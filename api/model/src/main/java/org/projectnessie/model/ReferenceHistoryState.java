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
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(
    type = SchemaType.OBJECT,
    title = "ReferenceHistoryState",
    description =
        "Describes the consistency status of a commit within a `ReferenceHistoryResponse` object.\n"
            + "\n"
            + "Possible values of the `CommitConsistency` enum:\n"
            + "- `NOT_CHECKED` means: Consistency was not checked.\n"
            + "- `COMMIT_CONSISTENT` means: The commit object, its index information and all reachable content is present.\n"
            + "- `COMMIT_CONTENT_INCONSISTENT` means: The commit object is present and its index is accessible, but "
            + "some content reachable from the commit is not present.\n"
            + "- `COMMIT_INCONSISTENT` means: The commit is inconsistent in a way that makes it impossible to access "
            + "the commit, for example if the commit object itself or its index information is missing.")
@Value.Immutable
@JsonSerialize(as = ImmutableReferenceHistoryState.class)
@JsonDeserialize(as = ImmutableReferenceHistoryState.class)
public interface ReferenceHistoryState {
  @Schema(description = "Nessie commit ID.")
  @Value.Parameter(order = 1)
  String commitHash();

  @Schema(description = "Consistency status of the commit.")
  @Value.Parameter(order = 2)
  CommitConsistency commitConsistency();

  @Schema(description = "Meta information from the commit, if available.")
  @Value.Parameter(order = 3)
  @Nullable
  @jakarta.annotation.Nullable
  CommitMeta meta();

  static ReferenceHistoryState referenceHistoryElement(
      String commitHash, CommitConsistency commitConsistency, CommitMeta meta) {
    return ImmutableReferenceHistoryState.of(commitHash, commitConsistency, meta);
  }
}
