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

import org.projectnessie.model.Reference;

/**
 * Event that affects only one reference.
 *
 * <p>This type has many child interfaces:
 *
 * <ul>
 *   <li>{@link CommitEvent}: for commits;
 *   <li>{@link ContentEvent}: for content events (PUT / DELETE operations);
 *   <li>{@link ReferenceCreatedEvent}: for reference creations;
 *   <li>{@link ReferenceUpdatedEvent}: for reference updates (reassignments);
 *   <li>{@link ReferenceDeletedEvent}: for reference deletions.
 * </ul>
 */
public interface ReferenceEvent extends Event {

  /** The reference affected by the event. */
  Reference getReference();
}
