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

/**
 * An enum of all possible event types.
 *
 * <p>Please note that this enum may evolve in the future, and more enum values may be added. It is
 * important that SPI implementers be prepared to handle unknown enum values.
 */
public enum EventType {

  /**
   * The content is a reference created event.
   *
   * @see ReferenceCreatedEvent
   */
  REFERENCE_CREATED(ReferenceCreatedEvent.class),

  /**
   * The content is a reference updated event.
   *
   * @see ReferenceUpdatedEvent
   */
  REFERENCE_UPDATED(ReferenceUpdatedEvent.class),

  /**
   * The content is a reference deleted event.
   *
   * @see ReferenceDeletedEvent
   */
  REFERENCE_DELETED(ReferenceDeletedEvent.class),

  /**
   * The content is a commit event.
   *
   * @see CommitEvent
   */
  COMMIT(CommitEvent.class),

  /**
   * The content is a merge event.
   *
   * @see MergeEvent
   */
  MERGE(MergeEvent.class),

  /**
   * The content is a transplant event.
   *
   * @see TransplantEvent
   */
  TRANSPLANT(TransplantEvent.class),

  /**
   * The content is a content stored event.
   *
   * @see ContentStoredEvent
   */
  CONTENT_STORED(ContentStoredEvent.class),

  /**
   * The content is a content removed event.
   *
   * @see ContentRemovedEvent
   */
  CONTENT_REMOVED(ContentRemovedEvent.class),

  /**
   * The content is a custom (unknown) event. This type is a catch-all type for all other runtime
   * event types that do not match any of the well-known types above.
   *
   * @see CustomEvent
   */
  CUSTOM(CustomEvent.class),
  ;

  private final Class<? extends Event> subtype;

  EventType(Class<? extends Event> subtype) {
    this.subtype = subtype;
  }

  public Class<? extends Event> getSubtype() {
    return subtype;
  }
}
