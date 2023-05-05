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
package org.projectnessie.events.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.versioned.ResultType;

/**
 * Loads and holds all {@link EventSubscriber}s.
 *
 * <p>The main purpose of this class is to provide a single point of access to all subscribers, and
 * also to provide a bit-mask of all event types that subscribers are subscribed to, for a fast and
 * efficient type-based event filtering.
 *
 * <p>This class is meant to be used as a singleton, or in CDI Dependent pseudo-scope.
 */
public class EventSubscribers {

  /** Load all {@link EventSubscriber}s via {@link ServiceLoader}. */
  public static List<EventSubscriber> loadSubscribers() {
    List<EventSubscriber> subscribers = new ArrayList<>();
    for (EventSubscriber subscriber : ServiceLoader.load(EventSubscriber.class)) {
      subscribers.add(subscriber);
    }
    return subscribers;
  }

  private final List<EventSubscriber> subscribers;

  private final BitSet eventTypeMask;
  private final BitSet resultTypeMask;

  public EventSubscribers() {
    this(loadSubscribers());
  }

  public EventSubscribers(EventSubscriber... subscribers) {
    this(Arrays.asList(subscribers));
  }

  public EventSubscribers(Iterable<EventSubscriber> subscribers) {
    this.subscribers =
        Collections.unmodifiableList(
            StreamSupport.stream(subscribers.spliterator(), false).collect(Collectors.toList()));
    eventTypeMask = new BitSet(EventType.values().length);
    for (EventType eventType : EventType.values()) {
      this.subscribers.stream()
          .filter(s -> s.accepts(eventType))
          .findAny()
          .ifPresent(s -> eventTypeMask.set(eventType.ordinal()));
    }
    resultTypeMask = new BitSet(ResultType.values().length);
    for (ResultType resultType : ResultType.values()) {
      this.subscribers.stream()
          .filter(s -> map(resultType).anyMatch(s::accepts))
          .findAny()
          .ifPresent(s -> resultTypeMask.set(resultType.ordinal()));
    }
  }

  /** Returns an unmodifiable list of all {@link EventSubscriber}s. */
  public List<EventSubscriber> getSubscribers() {
    return subscribers;
  }

  /** Returns {@code true} if there are any subscribers for the given {@link EventType}. */
  public boolean hasSubscribersFor(EventType type) {
    return eventTypeMask.get(type.ordinal());
  }

  /** Returns {@code true} if there are any subscribers for the given {@link ResultType}. */
  public boolean hasSubscribersFor(ResultType resultType) {
    return resultTypeMask.get(resultType.ordinal());
  }

  /**
   * Maps a {@link ResultType} to all {@link EventType}s that could be emitted for a result of that
   * type.
   */
  private static Stream<EventType> map(ResultType resultType) {
    switch (resultType) {
      case REFERENCE_CREATED:
        return Stream.of(EventType.REFERENCE_CREATED);
      case REFERENCE_ASSIGNED:
        return Stream.of(EventType.REFERENCE_UPDATED);
      case REFERENCE_DELETED:
        return Stream.of(EventType.REFERENCE_DELETED);
      case COMMIT:
        return Stream.of(EventType.COMMIT, EventType.CONTENT_STORED, EventType.CONTENT_REMOVED);
      case MERGE:
        return Stream.of(
            EventType.MERGE, EventType.COMMIT, EventType.CONTENT_STORED, EventType.CONTENT_REMOVED);
      case TRANSPLANT:
        return Stream.of(
            EventType.TRANSPLANT,
            EventType.COMMIT,
            EventType.CONTENT_STORED,
            EventType.CONTENT_REMOVED);
      default:
        throw new IllegalArgumentException("Unknown result type: " + resultType);
    }
  }
}
