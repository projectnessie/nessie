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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.versioned.ResultType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and holds all {@link EventSubscriber}s.
 *
 * <p>The main purpose of this class is to provide a single point of access to all subscribers, and
 * also to provide a bit-mask of all event types that subscribers are subscribed to, for a fast and
 * efficient type-based event filtering.
 *
 * <p>This class is meant to be used as a singleton, or in CDI Dependent pseudo-scope.
 */
public class EventSubscribers implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventSubscribers.class);

  // Visible for testing
  final List<EventSubscriber> subscribers;

  private final EnumSet<EventType> acceptedEventTypes;
  private final EnumSet<ResultType> acceptedResultTypes;

  // guarded by this
  private boolean started;
  // guarded by this
  private boolean closed;

  private Map<EventSubscription, EventSubscriber> subscriptions;

  public EventSubscribers(EventSubscriber... subscribers) {
    this(Arrays.asList(subscribers));
  }

  public EventSubscribers(Collection<EventSubscriber> subscribers) {
    this.subscribers = List.copyOf(subscribers);
    acceptedEventTypes =
        Arrays.stream(EventType.values())
            .filter(t -> this.subscribers.stream().anyMatch(s -> s.accepts(t)))
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(EventType.class)));
    acceptedResultTypes =
        acceptedEventTypes.stream()
            .flatMap(EventSubscribers::map)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(ResultType.class)));
  }

  /**
   * Starts all subscribers.
   *
   * @param subscriptionFactory a function that creates a subscription for a subscriber
   */
  public synchronized void start(Function<EventSubscriber, EventSubscription> subscriptionFactory) {
    if (!started) {
      if (subscribers.isEmpty()) {
        LOGGER.debug("No subscribers to start.");
        this.subscriptions = Collections.emptyMap();
      } else {
        LOGGER.info("Starting subscribers...");
        Map<EventSubscription, EventSubscriber> subscriptions = new HashMap<>();
        for (EventSubscriber subscriber : subscribers) {
          try {
            EventSubscription subscription = subscriptionFactory.apply(subscriber);
            subscriber.onSubscribe(subscription);
            subscriptions.put(subscription, subscriber);
          } catch (Exception e) {
            throw new RuntimeException("Error starting subscriber", e);
          }
        }
        this.subscriptions = Collections.unmodifiableMap(subscriptions);
        LOGGER.info("Done starting subscribers.");
      }
      started = true;
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      if (!subscribers.isEmpty()) {
        LOGGER.info("Closing subscribers...");
        List<Exception> errors = new ArrayList<>();
        for (EventSubscriber subscriber : subscribers) {
          try {
            subscriber.close();
          } catch (Exception e) {
            errors.add(e);
          }
        }
        if (!errors.isEmpty()) {
          RuntimeException e = new RuntimeException("Error closing at least one subscriber");
          errors.forEach(e::addSuppressed);
          LOGGER.error(e.getMessage(), e);
          throw e;
        }
        LOGGER.info("Done closing subscribers.");
      }
      closed = true;
    }
  }

  /** Returns an unmodifiable list of all subscribers. */
  public List<EventSubscriber> getSubscribers() {
    return subscribers;
  }

  /**
   * Returns an unmodifiable map of all active subscriptions with their subscribers. Returns null if
   * {@link #start(Function)} has not been called yet.
   */
  public Map<EventSubscription, EventSubscriber> getSubscriptions() {
    return subscriptions;
  }

  /** Returns {@code true} if there are any subscribers for the given {@link EventType}. */
  public boolean hasSubscribersFor(EventType type) {
    return acceptedEventTypes.contains(type);
  }

  /** Returns {@code true} if there are any subscribers for the given {@link ResultType}. */
  public boolean hasSubscribersFor(ResultType resultType) {
    return acceptedResultTypes.contains(resultType);
  }

  /**
   * Maps a {@link ResultType} to all {@link EventType}s that could be emitted for a result of that
   * type.
   */
  private static Stream<ResultType> map(EventType resultType) {
    switch (resultType) {
      case COMMIT:
      case CONTENT_STORED:
      case CONTENT_REMOVED:
        return Stream.of(ResultType.COMMIT, ResultType.MERGE, ResultType.TRANSPLANT);
      case MERGE:
        return Stream.of(ResultType.MERGE);
      case TRANSPLANT:
        return Stream.of(ResultType.TRANSPLANT);
      case REFERENCE_CREATED:
        return Stream.of(ResultType.REFERENCE_CREATED);
      case REFERENCE_UPDATED:
        return Stream.of(ResultType.REFERENCE_ASSIGNED);
      case REFERENCE_DELETED:
        return Stream.of(ResultType.REFERENCE_DELETED);
      default:
        throw new IllegalArgumentException("Unknown result type: " + resultType);
    }
  }
}
