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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.events.api.json.CustomEventDeserializer;
import org.projectnessie.events.api.json.CustomEventSerializer;

/**
 * A custom {@link Event} whose runtime type is not known.
 *
 * <p>This special event subtype is only used when deserializing unknown event types. This situation
 * can happen when a newer event subtype is present server-side, but the client deserializing the
 * event has an older version of the Event API where that event subtype is not available.
 *
 * <p>All properties of the original event object will be deserialized in the {@link
 * #getProperties()} map.
 */
@Value.Immutable
@JsonTypeName("CUSTOM")
@JsonSerialize(using = CustomEventSerializer.class)
@JsonDeserialize(using = CustomEventDeserializer.class)
public interface CustomEvent extends Event {

  @Override
  @Value.Default
  default EventType getType() {
    return EventType.CUSTOM;
  }

  /** The actual runtime type name of this event object. */
  String getCustomType();
}
