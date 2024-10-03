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

import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;

/**
 * A subscriber for events.
 *
 * <p>This is the main SPI interface that must be implemented in order to receive events from
 * Nessie.
 *
 * <p>How subscribers are registered with Nessie is implementation-specific. For example, if Nessie
 * is running on Quarkus, subscribers are typically discovered by CDI and classpath scanning.
 *
 * <p>Implementations of this interface must not block. If blocking I/O is required, then the {@link
 * #isBlocking()} method should return {@code true}.
 *
 * <p>Please note that this interface may evolve in the future, and more methods may be added. These
 * will always be default methods, so that existing implementations will continue to work. But it is
 * important that SPI implementers be prepared to cope with such evolutions.
 */
public interface EventSubscriber extends AutoCloseable {

  /**
   * Called when the subscriber is registered by Nessie.
   *
   * <p>Any initialization work, such as reading configuration, opening remote connections, etc.,
   * should be done here, and not in the constructor.
   */
  void onSubscribe(EventSubscription subscription);

  /**
   * Returns whether this subscriber is blocking, that is, whether it is expected to perform
   * blocking I/O operations when processing events.
   *
   * <p>Event delivery to blocking subscribers is done in a separate thread pool, so that they do
   * not block Nessie's internal event queue.
   */
  default boolean isBlocking() {
    return false;
  }

  /**
   * Returns a filter that determines which event types are accepted by this subscriber.
   *
   * <p>By default, all event types are accepted.
   */
  default EventTypeFilter getEventTypeFilter() {
    return EventTypeFilter.all();
  }

  /**
   * Returns a filter that determines which events are accepted by this subscriber.
   *
   * <p>By default, all events are accepted.
   */
  default EventFilter getEventFilter() {
    return EventFilter.all();
  }

  /**
   * Returns whether this subscriber accepts the given event type.
   *
   * <p>If this method returns {@code false}, no events of this type will be delivered to the
   * subscriber.
   */
  default boolean accepts(EventType eventType) {
    return getEventTypeFilter().test(eventType);
  }

  /**
   * Returns whether this subscriber accepts the given event.
   *
   * <p>If this method returns {@code false}, this specific event will not be delivered to the
   * subscriber.
   */
  default boolean accepts(Event event) {
    return accepts(event.getType()) && getEventFilter().test(event);
  }

  /** Called when a reference is created. */
  default void onReferenceCreated(ReferenceCreatedEvent event) {}

  /** Called when a reference is updated (re-assigned). */
  default void onReferenceUpdated(ReferenceUpdatedEvent event) {}

  /** Called when a reference is deleted. */
  default void onReferenceDeleted(ReferenceDeletedEvent event) {}

  /** Called when a commit is performed. */
  default void onCommit(CommitEvent event) {}

  /** Called when a merge is performed. */
  default void onMerge(MergeEvent event) {}

  /** Called when a transplant is performed. */
  default void onTransplant(TransplantEvent event) {}

  /** Called when a content is stored (PUT operation). */
  default void onContentStored(ContentStoredEvent event) {}

  /** Called when a content is removed (DELETE operation). */
  default void onContentRemoved(ContentRemovedEvent event) {}

  /**
   * Called when any event is received from Nessie. The default implementation simply dispatches to
   * the more specific methods.
   */
  default void onEvent(Event event) {
    switch (event.getType()) {
      case REFERENCE_CREATED:
        onReferenceCreated((ReferenceCreatedEvent) event);
        break;
      case REFERENCE_UPDATED:
        onReferenceUpdated((ReferenceUpdatedEvent) event);
        break;
      case REFERENCE_DELETED:
        onReferenceDeleted((ReferenceDeletedEvent) event);
        break;
      case COMMIT:
        onCommit((CommitEvent) event);
        break;
      case MERGE:
        onMerge((MergeEvent) event);
        break;
      case TRANSPLANT:
        onTransplant((TransplantEvent) event);
        break;
      case CONTENT_STORED:
        onContentStored((ContentStoredEvent) event);
        break;
      case CONTENT_REMOVED:
        onContentRemoved((ContentRemovedEvent) event);
        break;
      default:
        throw new IllegalArgumentException("Unknown event type: " + event.getType());
    }
  }

  /**
   * Called when the Nessie server is stopped. Subscribers should release any resources they hold in
   * this method.
   */
  @Override
  void close() throws Exception;
}
