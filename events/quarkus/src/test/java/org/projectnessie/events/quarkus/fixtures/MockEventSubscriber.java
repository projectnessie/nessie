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
package org.projectnessie.events.quarkus.fixtures;

import static org.projectnessie.events.api.EventType.CONTENT_REMOVED;
import static org.projectnessie.events.api.EventType.CONTENT_STORED;
import static org.projectnessie.events.api.EventType.REFERENCE_CREATED;
import static org.projectnessie.events.api.EventType.REFERENCE_DELETED;
import static org.projectnessie.events.api.EventType.REFERENCE_UPDATED;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.projectnessie.events.spi.EventFilter;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.EventTypeFilter;

/**
 * Mock event subscriber for testing. Instances of this class are loaded via ServiceLoader, just as
 * they would in production.
 *
 * <p>Two child classes are used to test different scenarios: {@link MockEventSubscriber1} and
 * {@link MockEventSubscriber2}.
 *
 * <p>The two subscribers behave in the following way:
 *
 * <ul>
 *   <li>MockEventSubscriber1: non-blocking, receives all events without any issues;
 *   <li>MockEventSubscriber2: reports {@link EventSubscriber#isBlocking()} as {@code true} and:
 *       <ul>
 *         <li>does not receive COMMIT, MERGE, TRANSPLANT events (event type filter);
 *         <li>rejects the first CONTENT_STORED event (event filter);
 *         <li>throws 2 exceptions on CONTENT_REMOVED (delivery succeeds on the 3rd attempt);
 *         <li>throws 3 exceptions on REFERENCE_DELETED (delivery fails after 3 attempts).
 *       </ul>
 * </ul>
 */
public class MockEventSubscriber implements EventSubscriber {

  private List<Event> events = new ArrayList<>();
  private EnumMap<EventType, AtomicInteger> rejects;
  private EnumMap<EventType, AtomicInteger> failures;
  private EventTypeFilter eventTypeFilter;
  private EventFilter eventFilter;
  private boolean blocking = false;
  private EventSubscription subscription;

  public MockEventSubscriber() {
    setUp();
  }

  public synchronized List<Event> getEvents() {
    return ImmutableList.copyOf(events);
  }

  public EventSubscription getSubscription() {
    return subscription;
  }

  public synchronized void setUp() {
    events = new ArrayList<>();
    rejects = new EnumMap<>(EventType.class);
    failures = new EnumMap<>(EventType.class);
    eventTypeFilter = EventTypeFilter.all();
    eventFilter = EventFilter.all();
  }

  @SuppressWarnings("SameParameterValue")
  protected void emulateRejects(EventType eventType, int numRejects) {
    rejects.put(eventType, new AtomicInteger(numRejects));
    setEventFilter(this::maybeReject);
  }

  private boolean maybeReject(Event event) {
    if (rejects.containsKey(event.getType())) {
      int numRejects = rejects.get(event.getType()).getAndDecrement();
      return numRejects <= 0;
    }
    return true;
  }

  protected void emulateFailures(EventType eventType, int numFailures) {
    failures.put(eventType, new AtomicInteger(numFailures));
  }

  private synchronized void recordEvent(Event event) {
    if (failures.containsKey(event.getType())) {
      // Emulate failed delivery
      int numFailures = failures.get(event.getType()).getAndDecrement();
      if (numFailures > 0) {
        throw new RuntimeException("Emulated failure");
      }
    }
    events.add(event);
  }

  @Override
  public EventFilter getEventFilter() {
    return eventFilter;
  }

  public void setEventFilter(EventFilter eventFilter) {
    this.eventFilter = eventFilter;
  }

  @Override
  public EventTypeFilter getEventTypeFilter() {
    return eventTypeFilter;
  }

  public void setEventTypeFilter(EventTypeFilter eventTypeFilter) {
    this.eventTypeFilter = eventTypeFilter;
  }

  @Override
  public boolean isBlocking() {
    return blocking;
  }

  public void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  @Override
  public void onSubscribe(EventSubscription subscription) {
    this.subscription = subscription;
  }

  @Override
  public void onReferenceCreated(ReferenceCreatedEvent event) {
    recordEvent(event);
  }

  @Override
  public void onReferenceUpdated(ReferenceUpdatedEvent event) {
    recordEvent(event);
  }

  @Override
  public void onReferenceDeleted(ReferenceDeletedEvent event) {
    recordEvent(event);
  }

  @Override
  public void onCommit(CommitEvent event) {
    recordEvent(event);
  }

  @Override
  public void onMerge(MergeEvent event) {
    recordEvent(event);
  }

  @Override
  public void onTransplant(TransplantEvent event) {
    recordEvent(event);
  }

  @Override
  public void onContentStored(ContentStoredEvent event) {
    recordEvent(event);
  }

  @Override
  public void onContentRemoved(ContentRemovedEvent event) {
    recordEvent(event);
  }

  @Override
  public void close() {}

  public static class MockEventSubscriber1 extends MockEventSubscriber {}

  public static class MockEventSubscriber2 extends MockEventSubscriber {

    @Override
    public synchronized void setUp() {
      super.setUp();
      setBlocking(true);
      setEventTypeFilter(
          EventTypeFilter.of(
              CONTENT_REMOVED,
              CONTENT_STORED,
              REFERENCE_CREATED,
              REFERENCE_DELETED,
              REFERENCE_UPDATED));
      emulateFailures(CONTENT_REMOVED, 2);
      emulateFailures(REFERENCE_DELETED, 3);
      emulateRejects(CONTENT_STORED, 1);
    }
  }
}
