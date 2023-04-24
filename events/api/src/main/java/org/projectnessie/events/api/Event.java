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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.immutables.value.Value;

/**
 * Base interface for all events produced by Nessie.
 *
 * @see CommitEvent
 * @see MergeEvent
 * @see TransplantEvent
 * @see ReferenceCreatedEvent
 * @see ReferenceUpdatedEvent
 * @see ReferenceDeletedEvent
 * @see ContentStoredEvent
 * @see ContentRemovedEvent
 */
@JsonSubTypes({
  @JsonSubTypes.Type(name = "COMMIT", value = ImmutableCommitEvent.class),
  @JsonSubTypes.Type(name = "MERGE", value = ImmutableMergeEvent.class),
  @JsonSubTypes.Type(name = "TRANSPLANT", value = ImmutableTransplantEvent.class),
  @JsonSubTypes.Type(name = "REFERENCE_CREATED", value = ImmutableReferenceCreatedEvent.class),
  @JsonSubTypes.Type(name = "REFERENCE_UPDATED", value = ImmutableReferenceUpdatedEvent.class),
  @JsonSubTypes.Type(name = "REFERENCE_DELETED", value = ImmutableReferenceDeletedEvent.class),
  @JsonSubTypes.Type(name = "CONTENT_STORED", value = ImmutableContentStoredEvent.class),
  @JsonSubTypes.Type(name = "CONTENT_REMOVED", value = ImmutableContentRemovedEvent.class)
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface Event {

  /** The type of the event. */
  EventType getType();

  /**
   * The id of the event. The UUID is generated by the event emitter. No assumptions should be made
   * about the version of the UUID.
   */
  UUID getId();

  /**
   * The id of the repository. This is configured on a per-instance basis.
   *
   * <p>See configuration option: {@code nessie.version.store.advanced.repository-id}.
   */
  String getRepositoryId();

  /**
   * The time the event was created. The time is generated by the event emitter and is based on its
   * internal wall clock time.
   */
  Instant getCreatedAt();

  /**
   * The user that created the event. The user is set by the event emitter and is based on the
   * authenticated user that performed the action that caused the event to be emitted.
   *
   * <p>If authentication is disabled, this will be empty.
   */
  Optional<String> getCreatedBy();

  /**
   * A distinctive identifier for the entity that emitted the event. This is configured on a per-
   * instance basis and usually contains the host name and the deployed Nessie version.
   */
  String getSentBy();

  /** A map of properties that can be used to add additional information to the event. */
  @Value.Default
  default Map<String, String> getProperties() {
    return Collections.emptyMap();
  }
}
