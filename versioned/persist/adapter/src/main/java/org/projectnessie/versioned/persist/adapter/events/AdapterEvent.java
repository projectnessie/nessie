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

/**
 * Base class for all events emitted by {@link
 * org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter} implementations.
 *
 * <p>Database adapter events and infrastructure are "Nessie internal" and may change, even
 * fundamentally, w/o prior notice.
 */
public interface AdapterEvent {
  OperationType getOperationType();

  long getEventTimeInMicros();

  interface Builder<B extends Builder<B, E>, E extends AdapterEvent> {
    B operationType(OperationType operationType);

    B eventTimeInMicros(long eventTimeInMicros);

    E build();
  }
}
