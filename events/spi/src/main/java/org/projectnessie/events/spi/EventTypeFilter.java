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
package org.projectnessie.events.spi;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.function.Predicate;
import org.projectnessie.events.api.EventType;

/** A filter for {@link EventType}s. */
public interface EventTypeFilter extends Predicate<EventType> {

  /** Returns a filter that matches all {@link EventType}s. */
  static EventTypeFilter all() {
    return t -> true;
  }

  /** Returns a filter that matches no {@link EventType}s. */
  static EventTypeFilter none() {
    return t -> false;
  }

  /** Returns a filter that matches only the given {@link EventType}. */
  static EventTypeFilter of(EventType eventType) {
    return eventType::equals;
  }

  /** Returns a filter that matches any of the given {@link EventType}s. */
  static EventTypeFilter of(EventType... eventTypes) {
    return of(Arrays.asList(eventTypes));
  }

  /** Returns a filter that matches any of the given {@link EventType}s. */
  static EventTypeFilter of(Collection<EventType> eventTypes) {
    if (eventTypes.isEmpty()) {
      return none();
    }
    if (eventTypes.size() == 1) {
      return of(eventTypes.iterator().next());
    }
    return EnumSet.copyOf(eventTypes)::contains;
  }
}
