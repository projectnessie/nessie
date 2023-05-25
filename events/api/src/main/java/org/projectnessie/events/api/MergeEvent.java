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

import org.immutables.value.Value;

/**
 * Event that is emitted when a merge is performed. This event is emitted after the merge has been
 * persisted.
 */
@Value.Immutable
public interface MergeEvent extends CommittingEvent {

  @Override
  @Value.Default
  default EventType getType() {
    return EventType.MERGE;
  }

  /**
   * The hash of the common ancestor of the two merged branches.
   *
   * <p>If merging unrelated histories is allowed, and the two branches share no common history,
   * this will be the so-called "no-ancestor" hash.
   */
  String getCommonAncestorHash();
}
