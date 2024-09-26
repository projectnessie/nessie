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

/**
 * Event that is emitted when a transplant is performed. This event is emitted after the transplant
 * has been persisted.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTransplantEvent.class)
@JsonDeserialize(as = ImmutableTransplantEvent.class)
public interface TransplantEvent
    extends MultiReferenceEvent, WithHashBeforeEvent, WithHashAfterEvent {

  @Override
  @Value.Default
  default EventType getType() {
    return EventType.TRANSPLANT;
  }

  /** The number of commits that were transplanted into the target reference. */
  int getCommitCount();
}
