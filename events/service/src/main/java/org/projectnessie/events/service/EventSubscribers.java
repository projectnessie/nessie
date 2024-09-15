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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
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

  /** Load all {@link EventSubscriber}s via {@link ServiceLoader}. */
  public static List<EventSubscriber> loadSubscribers() {
    List<EventSubscriber> subscribers = new ArrayList<>();
    for (EventSubscriber subscriber : ServiceLoader.load(EventSubscriber.class)) {
      subscribers.add(subscriber);
    }
    return subscribers;
  }

  // Visible for testing
  final List<EventSubscriber> subscribers;

  private final EnumSet<EventType> acceptedEventTypes;
  private final EnumSet<ResultType> acceptedResultTypes;
  private final Set<Content.Type> acceptedContentTypes;

  // guarded by this
  private boolean started;
  // guarded by this
  private boolean closed;

  private Map<EventSubscription, EventSubscriber> subscriptions;

  public EventSubscribers(EventSubscriber... subscribers) {
    this(Arrays.asList(subscribers));
  }

  public EventSubscribers(Iterable<EventSubscriber> subscribers) {
    this.subscribers =
        StreamSupport.stream(subscribers.spliterator(), false)
            .collect(Collectors.toUnmodifiableList());
    acceptedEventTypes =
        Arrays.stream(EventType.values())
            .filter(t -> this.subscribers.stream().anyMatch(s -> s.accepts(t)))
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(EventType.class)));
    acceptedResultTypes =
        acceptedEventTypes.stream()
            .flatMap(EventSubscribers::mapToResultType)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(ResultType.class)));
    acceptedContentTypes =
        acceptedEventTypes.stream()
            .flatMap(EventSubscribers::mapToContentType)
            .collect(Collectors.toUnmodifiableSet());
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
        throw e;
      }
      LOGGER.info("Done closing subscribers.");
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

  /**
   * Returns {@code true} if there are any subscribers for the given {@link ResultType}. This is
   * used to filter version store events.
   */
  public boolean hasSubscribersFor(ResultType resultType) {
    return acceptedResultTypes.contains(resultType);
  }

  /**
   * Returns {@code true} if there are any subscribers for the given {@link Content.Type}. This is
   * used to filter catalog events.
   */
  public boolean hasSubscribersFor(Content.Type contentType) {
    return acceptedContentTypes.contains(contentType);
  }

  /** Maps an {@link EventType} to all corresponding {@link ResultType}s. */
  private static Stream<ResultType> mapToResultType(EventType eventType) {
    switch (eventType) {
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
      case TABLE_CREATED:
      case TABLE_ALTERED:
      case TABLE_DROPPED:
      case VIEW_CREATED:
      case VIEW_ALTERED:
      case VIEW_DROPPED:
      case NAMESPACE_CREATED:
      case NAMESPACE_ALTERED:
      case NAMESPACE_DROPPED:
      case UDF_CREATED:
      case UDF_ALTERED:
      case UDF_DROPPED:
      case GENERIC_CONTENT_CREATED:
      case GENERIC_CONTENT_ALTERED:
      case GENERIC_CONTENT_DROPPED:
        return Stream.of();
      default:
        throw new IllegalArgumentException("Unknown event type: " + eventType);
    }
  }

  /** Maps an {@link EventType} to all corresponding {@link Content.Type}s. */
  private static Stream<Content.Type> mapToContentType(EventType eventType) {
    switch (eventType) {
      case TABLE_CREATED:
      case TABLE_ALTERED:
      case TABLE_DROPPED:
        return Stream.of(Type.ICEBERG_TABLE, Type.DELTA_LAKE_TABLE);
      case VIEW_CREATED:
      case VIEW_ALTERED:
      case VIEW_DROPPED:
        return Stream.of(Type.ICEBERG_VIEW);
      case NAMESPACE_CREATED:
      case NAMESPACE_ALTERED:
      case NAMESPACE_DROPPED:
        return Stream.of(Type.NAMESPACE);
      case UDF_CREATED:
      case UDF_ALTERED:
      case UDF_DROPPED:
        return Stream.of(Type.UDF);
      case GENERIC_CONTENT_CREATED:
      case GENERIC_CONTENT_ALTERED:
      case GENERIC_CONTENT_DROPPED:
        return Stream.of(Type.UNKNOWN);
      case COMMIT:
      case CONTENT_STORED:
      case CONTENT_REMOVED:
      case MERGE:
      case TRANSPLANT:
      case REFERENCE_CREATED:
      case REFERENCE_UPDATED:
      case REFERENCE_DELETED:
        return Stream.of();
      default:
        throw new IllegalArgumentException("Unknown event type: " + eventType);
    }
  }
}
