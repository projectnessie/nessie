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
package org.projectnessie.events;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import org.projectnessie.model.CommitMeta;

/** Marker interface for all events. */
public interface Event {

  /**
   * The event time. This is the time at which the event was "fired" and not necessarily the time at
   * which the event happened. Although the idea is for those times to be equal, GC pauses and other
   * scheduling "weirdness" prevent us to make that promise.
   */
  @JsonSerialize(using = CommitMeta.InstantSerializer.class)
  @JsonDeserialize(using = CommitMeta.InstantDeserializer.class)
  Instant getEventTime();
}
