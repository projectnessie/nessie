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
package org.projectnessie.client.api;

import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;

/**
 * Allows inspection of merge results, to resolve {@link ConflictType#VALUE_DIFFERS content related}
 * merge {@link Conflict conflicts} indicated in the {@link #getResponse() merge response}.
 */
public interface MergeResponseInspector {
  /** The merge request sent to Nessie. */
  Merge getRequest();

  /** The merge response received from Nessie. */
  MergeResponse getResponse();

  /**
   * Provides details about the conflicts that happened during a merge operation.
   *
   * <p>The returned stream contains one {@link MergeConflictDetails element} for each conflict.
   * Non-conflicting contents are not included.
   *
   * <p>Each {@link MergeConflictDetails conflict details object} allows callers to resolve
   * conflicts based on that information, also known as "content aware merge".
   *
   * <p>Once conflicts have been either resolved, or alternatively specific keys declared to "use
   * the {@link MergeBehavior#DROP left/from} or {@link MergeBehavior#FORCE right/target} side",
   * another {@link MergeReferenceBuilder merge operation} cna be performed, providing a {@link
   * MergeKeyBehavior#getResolvedContent() resolved content} for a content key.
   *
   * <p>Keep in mind that calling this function triggers API calls against nessie to retrieve the
   * relevant content objects on the merge-base and and the content keys and content objects on the
   * merge-from (source) and merge-target.
   */
  Stream<MergeConflictDetails> collectMergeConflictDetails() throws NessieNotFoundException;

  @Value.Immutable
  interface MergeConflictDetails {
    /** The content ID of the conflicting content. */
    default String getContentId() {
      return contentOnMergeBase().getId();
    }

    /** Key of the content on the {@link MergeResponse#getCommonAncestor() merge-base commit}. */
    ContentKey keyOnMergeBase();

    /** Key of the content on the {@link MergeReferenceBuilder#fromRef merge-from reference}. */
    @Nullable
    @jakarta.annotation.Nullable
    ContentKey keyOnSource();

    /**
     * Key of the content on the {@link MergeResponse#getEffectiveTargetHash() merge-target
     * reference}.
     */
    @Nullable
    @jakarta.annotation.Nullable
    ContentKey keyOnTarget();

    /** Content object on the {@link MergeResponse#getCommonAncestor() merge-base commit}. */
    // TODO this can also be null, if the same key was added on source + target but is not present
    //  on merge-base.
    Content contentOnMergeBase();

    /**
     * Content on the {@link MergeReferenceBuilder#fromRef merge-from reference}, or {@code null} if
     * not present on the merge-from.
     */
    @Nullable
    @jakarta.annotation.Nullable
    Content contentOnSource();

    /**
     * Content on the {@link MergeResponse#getEffectiveTargetHash() merge-target reference}, or
     * {@code null} if not present on the merge-target.
     */
    @Nullable
    @jakarta.annotation.Nullable
    Content contentOnTarget();

    /**
     * Contains {@link Conflict#conflictType() machine interpretable} and {@link Conflict#message()
     * human.readable information} about the conflict.
     */
    // TODO this can also be null, if the same key was added on source + target but is not present
    //  on merge-base.
    Conflict conflict();
  }
}
