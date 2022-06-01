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

import com.google.common.annotations.Beta;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

/**
 * Base class for all events emitted by {@link
 * org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter} implementations.
 *
 * <p>Database adapter events and infrastructure are "Nessie internal" and may change, even
 * fundamentally, w/o prior notice.
 */
@Beta
public interface AdapterEvent {
  OperationType getOperationType();

  /**
   * Time when the event was created, source is {@link DatabaseAdapterConfig#getClock()} ({@link
   * java.time.Clock#systemUTC()} in production) using {@link java.time.Instant#getEpochSecond} plus
   * {@link java.time.Instant#getNano()}.
   */
  long getEventTimeMicros();

  interface Builder<B extends Builder<B, E>, E extends AdapterEvent> {
    B operationType(OperationType operationType);

    B eventTimeMicros(long eventTimeMicros);

    E build();
  }
}
