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
package org.projectnessie.versioned.persist.adapter.events;

import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;

public interface ReferenceEvent extends AdapterEvent {
  NamedRef getRef();

  /**
   * Hash of the reference, specific meaning for each event type.
   *
   * <ul>
   *   <li>For {@link ReferenceCreatedEvent}: the current/initial commit ID of the reference when it
   *       was created
   *   <li>For {@link ReferenceDeletedEvent}: the current commit ID of the reference when it was
   *       deleted
   *   <li>For {@link ReferenceAssignedEvent}): the <em>new</em> commit ID of the named reference
   * </ul>
   */
  Hash getCurrentHash();

  interface Builder<B extends Builder<B, E>, E extends ReferenceEvent>
      extends AdapterEvent.Builder<B, E> {
    B ref(NamedRef ref);

    B currentHash(Hash currentHash);
  }
}
