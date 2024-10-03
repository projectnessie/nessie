/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.server.events.fixtures.MockEventSubscriber;

@ApplicationScoped
public class MockEventSubscriberImpl implements MockEventSubscriber {

  private final List<Event> events = new CopyOnWriteArrayList<>();

  private volatile boolean recording = false;

  @Override
  public List<Event> getEvents() {
    return ImmutableList.copyOf(events);
  }

  @Override
  public List<Event> awaitEvents(int numEvents) {
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(events).hasSize(numEvents));
    return getEvents();
  }

  @Override
  public void reset() {
    events.clear();
  }

  @Override
  public void startRecording() {
    recording = true;
  }

  @Override
  public void stopRecording() {
    recording = false;
  }

  @Override
  public void onSubscribe(EventSubscription subscription) {}

  private void recordEvent(Event event) {
    if (recording) {
      events.add(event);
    }
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
}
