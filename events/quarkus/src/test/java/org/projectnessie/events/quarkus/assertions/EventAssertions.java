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
package org.projectnessie.events.quarkus.assertions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.projectnessie.events.api.EventType.COMMIT;
import static org.projectnessie.events.api.EventType.CONTENT_REMOVED;
import static org.projectnessie.events.api.EventType.CONTENT_STORED;
import static org.projectnessie.events.api.EventType.MERGE;
import static org.projectnessie.events.api.EventType.TRANSPLANT;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.quarkus.fixtures.MockEventSubscriber;

@Singleton
public class EventAssertions {

  static final Duration TIMEOUT = Duration.ofSeconds(5);

  @Inject MockEventSubscriber.MockEventSubscriber1 subscriber1;

  @Inject MockEventSubscriber.MockEventSubscriber2 subscriber2;

  public void reset() {
    subscriber1.setUp();
    subscriber2.setUp();
  }

  // Note: the assertions below rely on the predefined subscriber behavior, see MockEventSubscriber
  // for details

  public void awaitAndAssertCommitEvents(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertCommitEvents(authEnabled));
  }

  public void awaitAndAssertMergeEvents(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertMergeEvents(authEnabled));
  }

  public void awaitAndAssertTransplantEvents(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertTransplantEvents(authEnabled));
  }

  public void awaitAndAssertReferenceCreatedEvents(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertReferenceCreatedEvents(authEnabled));
  }

  public void awaitAndAssertReferenceDeletedEvents(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertReferenceDeletedEvents(authEnabled));
  }

  public void awaitAndAssertReferenceUpdatedEvents(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertReferenceUpdatedEvents(authEnabled));
  }

  private void assertCommitEvents(boolean authEnabled) {
    assertEventsReceived(subscriber1, COMMIT, CONTENT_STORED, CONTENT_REMOVED);
    assertEventsReceived(subscriber2, CONTENT_REMOVED);
    assertEventInitiator(authEnabled);
    assertRepositoryId();
    assertStaticProperties();
  }

  private void assertMergeEvents(boolean authEnabled) {
    assertEventsReceived(subscriber1, MERGE, COMMIT, CONTENT_STORED, CONTENT_REMOVED);
    assertEventsReceived(subscriber2, CONTENT_REMOVED);
    assertEventInitiator(authEnabled);
    assertRepositoryId();
    assertStaticProperties();
  }

  private void assertTransplantEvents(boolean authEnabled) {
    assertEventsReceived(subscriber1, TRANSPLANT, COMMIT, CONTENT_STORED, CONTENT_REMOVED);
    assertEventsReceived(subscriber2, CONTENT_REMOVED);
    assertEventInitiator(authEnabled);
    assertRepositoryId();
    assertStaticProperties();
  }

  private void assertReferenceCreatedEvents(boolean authEnabled) {
    assertEventsReceived(subscriber1, EventType.REFERENCE_CREATED);
    assertEventsReceived(subscriber2, EventType.REFERENCE_CREATED);
    assertEventInitiator(authEnabled);
    assertRepositoryId();
    assertStaticProperties();
  }

  private void assertReferenceDeletedEvents(boolean authEnabled) {
    assertEventsReceived(subscriber1, EventType.REFERENCE_DELETED);
    assertThat(subscriber2.getEvents()).isEmpty(); // delivery failed after 3 attempts
    assertEventInitiator(authEnabled);
    assertRepositoryId();
    assertStaticProperties();
  }

  private void assertReferenceUpdatedEvents(boolean authEnabled) {
    assertEventsReceived(subscriber1, EventType.REFERENCE_UPDATED);
    assertEventsReceived(subscriber2, EventType.REFERENCE_UPDATED);
    assertEventInitiator(authEnabled);
    assertRepositoryId();
    assertStaticProperties();
  }

  private void assertEventInitiator(boolean authEnabled) {
    if (authEnabled) {
      assertEventInitiatorPresent(subscriber1);
      assertEventInitiatorPresent(subscriber2);
    } else {
      assertEventInitiatorNotPresent(subscriber1);
      assertEventInitiatorNotPresent(subscriber2);
    }
  }

  private void assertRepositoryId() {
    assertRepositoryId(subscriber1);
    assertRepositoryId(subscriber2);
  }

  private void assertStaticProperties() {
    assertStaticProperties(subscriber1);
    assertStaticProperties(subscriber2);
  }

  private void assertEventsReceived(MockEventSubscriber subscriber, EventType... eventTypes) {
    // No need to test the exact contents of each event, this has been tested already
    assertThat(subscriber.getEvents())
        .extracting(Event::getType)
        .containsExactlyInAnyOrder(eventTypes);
  }

  private void assertEventInitiatorPresent(MockEventSubscriber subscriber) {
    for (Event event : subscriber.getEvents()) {
      assertThat(event.getEventInitiator()).isPresent().contains("Alice");
    }
  }

  private void assertEventInitiatorNotPresent(MockEventSubscriber subscriber) {
    for (Event event : subscriber.getEvents()) {
      assertThat(event.getEventInitiator()).isNotPresent();
    }
  }

  private void assertRepositoryId(MockEventSubscriber subscriber) {
    for (Event event : subscriber.getEvents()) {
      assertThat(event.getRepositoryId()).isEqualTo("repo1");
    }
  }

  private void assertStaticProperties(MockEventSubscriber subscriber) {
    for (Event event : subscriber.getEvents()) {
      assertThat(event.getProperties()).containsEntry("foo", "bar");
    }
  }
}
