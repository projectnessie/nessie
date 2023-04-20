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

import java.time.Instant;
import java.util.function.Predicate;
import org.projectnessie.events.api.Event;

/**
 * A filter for events.
 *
 * <p>Filters can be combined using {@link #and(EventFilter...)}, {@link #or(EventFilter...)} and
 * {@link #not(EventFilter)}.
 */
@FunctionalInterface
public interface EventFilter extends Predicate<Event> {

  /** Returns a filter that matches all events. */
  static EventFilter all() {
    return e -> true;
  }

  /** Returns a filter that matches no events. */
  static EventFilter none() {
    return e -> false;
  }

  /** Returns a filter that matches all events that match all the given filters. */
  static EventFilter and(EventFilter... filters) {
    return e -> {
      for (EventFilter f : filters) {
        if (!f.test(e)) {
          return false;
        }
      }
      return true;
    };
  }

  /** Returns a filter that matches all events that match at least one of the given filters. */
  static EventFilter or(EventFilter... filters) {
    return e -> {
      for (EventFilter f : filters) {
        if (f.test(e)) {
          return true;
        }
      }
      return false;
    };
  }

  /** Returns a filter that matches all events that do not match the given filter. */
  static EventFilter not(EventFilter filter) {
    return e -> !filter.test(e);
  }

  /** Returns a filter that matches all events that have the given repository id. */
  static EventFilter repositoryId(String id) {
    return e -> e.getRepositoryId().equals(id);
  }

  /** Returns a filter that matches all events created after the given instant. */
  static EventFilter createdAfter(Instant i) {
    return e -> e.getEventCreationTimestamp().isAfter(i);
  }

  /** Returns a filter that matches all events created before the given instant. */
  static EventFilter createdBefore(Instant i) {
    return e -> e.getEventCreationTimestamp().isBefore(i);
  }

  /** Returns a filter that matches all events initiated by the given username. */
  static EventFilter initiatedBy(String username) {
    return e -> e.getEventInitiator().filter(s -> s.equals(username)).isPresent();
  }

  /** Returns a filter that matches all events that have a property with the given key. */
  static EventFilter hasProperty(String key) {
    return e -> e.getProperties().containsKey(key);
  }

  /** Returns a filter that matches all events that have a property with the given key and value. */
  static EventFilter hasProperty(String key, String value) {
    return e -> e.getProperties().containsKey(key) && e.getProperties().get(key).equals(value);
  }
}
