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
package org.projectnessie.events.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.model.Reference;

/**
 * Event that is emitted when a merge is performed. This event is emitted after the merge has been
 * persisted.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableMergeEvent.class)
@JsonDeserialize(as = ImmutableMergeEvent.class)
public interface MergeEvent extends MultiReferenceEvent, WithHashBeforeEvent, WithHashAfterEvent {

  @Override
  @Value.Default
  default EventType getType() {
    return EventType.MERGE;
  }

  /**
   * The source reference where the committed operations come from. This is usually a branch, but
   * not always (e.g. it could be a tag or a detached reference).
   */
  Reference getSourceReference();

  /** The hash of the source reference that was merged into the target reference. */
  String getSourceHash();

  /**
   * The hash of the common ancestor of the two merged branches.
   *
   * <p>If merging unrelated histories is allowed, and the two branches share no common history,
   * this will be the so-called "no-ancestor" hash.
   */
  String getCommonAncestorHash();
}
