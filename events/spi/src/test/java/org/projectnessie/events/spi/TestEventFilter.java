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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.CommittingEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestEventFilter {

  @Mock CommitEvent commitEvent;
  @Mock MergeEvent mergeEvent;
  @Mock TransplantEvent transplantEvent;
  @Mock ReferenceCreatedEvent referenceCreatedEvent;
  @Mock ReferenceUpdatedEvent referenceUpdatedEvent;
  @Mock ReferenceDeletedEvent referenceDeletedEvent;
  @Mock ContentStoredEvent contentStoredEvent;
  @Mock ContentRemovedEvent contentRemovedEvent;

  public Stream<Event> allEvents() {
    return Stream.of(
        commitEvent,
        mergeEvent,
        transplantEvent,
        referenceCreatedEvent,
        referenceUpdatedEvent,
        referenceDeletedEvent,
        contentStoredEvent,
        contentRemovedEvent);
  }

  @ParameterizedTest
  @MethodSource("allEvents")
  void all(Event e) {
    assertThat(EventFilter.all().test(e)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("allEvents")
  void none(Event e) {
    assertThat(EventFilter.none().test(e)).isFalse();
  }

  @Test
  void and() {
    EventFilter isCommit = e -> e instanceof CommitEvent;
    EventFilter isCommitting = e -> e instanceof CommittingEvent;
    EventFilter filter = EventFilter.and(isCommit, isCommitting);
    assertThat(filter.test(commitEvent)).isTrue();
    assertThat(filter.test(mergeEvent)).isFalse();
    assertThat(filter.test(transplantEvent)).isFalse();
  }

  @Test
  void or() {
    EventFilter isCommit = e -> e instanceof CommitEvent;
    EventFilter isMerge = e -> e instanceof MergeEvent;
    EventFilter filter = EventFilter.or(isCommit, isMerge);
    assertThat(filter.test(commitEvent)).isTrue();
    assertThat(filter.test(mergeEvent)).isTrue();
    assertThat(filter.test(transplantEvent)).isFalse();
  }

  @Test
  void not() {
    EventFilter isCommit = e -> e instanceof CommitEvent;
    EventFilter filter = EventFilter.not(isCommit);
    assertThat(filter.test(commitEvent)).isFalse();
    assertThat(filter.test(mergeEvent)).isTrue();
    assertThat(filter.test(transplantEvent)).isTrue();
  }

  @Test
  void repositoryId() {
    when(commitEvent.getRepositoryId()).thenReturn("repo1");
    when(mergeEvent.getRepositoryId()).thenReturn("repo2");
    when(transplantEvent.getRepositoryId()).thenReturn("repo3");
    EventFilter filter = EventFilter.repositoryId("repo1");
    assertThat(filter.test(commitEvent)).isTrue();
    assertThat(filter.test(mergeEvent)).isFalse();
    assertThat(filter.test(transplantEvent)).isFalse();
  }

  @Test
  void createdAfter() {
    Instant i1 = Instant.ofEpochMilli(1L);
    Instant i2 = Instant.ofEpochMilli(2L);
    Instant i3 = Instant.ofEpochMilli(3L);
    when(commitEvent.getEventCreationTimestamp()).thenReturn(i1);
    when(mergeEvent.getEventCreationTimestamp()).thenReturn(i2);
    when(transplantEvent.getEventCreationTimestamp()).thenReturn(i3);
    EventFilter filter = EventFilter.createdAfter(i2);
    assertThat(filter.test(commitEvent)).isFalse();
    assertThat(filter.test(mergeEvent)).isFalse();
    assertThat(filter.test(transplantEvent)).isTrue();
  }

  @Test
  void createdBefore() {
    Instant i1 = Instant.ofEpochMilli(1L);
    Instant i2 = Instant.ofEpochMilli(2L);
    Instant i3 = Instant.ofEpochMilli(3L);
    when(commitEvent.getEventCreationTimestamp()).thenReturn(i1);
    when(mergeEvent.getEventCreationTimestamp()).thenReturn(i2);
    when(transplantEvent.getEventCreationTimestamp()).thenReturn(i3);
    EventFilter filter = EventFilter.createdBefore(i2);
    assertThat(filter.test(commitEvent)).isTrue();
    assertThat(filter.test(mergeEvent)).isFalse();
    assertThat(filter.test(transplantEvent)).isFalse();
  }

  @Test
  void initiator() {
    when(commitEvent.getEventInitiator()).thenReturn(Optional.of("user1"));
    when(mergeEvent.getEventInitiator()).thenReturn(Optional.of("user2"));
    when(transplantEvent.getEventInitiator()).thenReturn(Optional.empty());
    EventFilter filter = EventFilter.initiatedBy("user1");
    assertThat(filter.test(commitEvent)).isTrue();
    assertThat(filter.test(mergeEvent)).isFalse();
    assertThat(filter.test(transplantEvent)).isFalse();
  }

  @Test
  void hasProperty() {
    Map<String, Object> props1 = new HashMap<>();
    Map<String, Object> props2 = new HashMap<>();
    Map<String, Object> props3 = new HashMap<>();
    props1.put("prop1", "value1");
    props2.put("prop2", "value2");
    props3.put("prop1", "value1a");
    props3.put("prop3", "value3");
    when(commitEvent.getProperties()).thenReturn(props1);
    when(mergeEvent.getProperties()).thenReturn(props2);
    when(transplantEvent.getProperties()).thenReturn(props3);
    EventFilter filter = EventFilter.hasProperty("prop1");
    assertThat(filter.test(commitEvent)).isTrue();
    assertThat(filter.test(mergeEvent)).isFalse();
    assertThat(filter.test(transplantEvent)).isTrue();
  }

  @Test
  void hasPropertyWithValue() {
    Map<String, Object> props1 = new HashMap<>();
    Map<String, Object> props2 = new HashMap<>();
    Map<String, Object> props3 = new HashMap<>();
    props1.put("prop1", "value1");
    props2.put("prop2", "value2");
    props3.put("prop1", "value1a");
    props3.put("prop3", "value3");
    when(commitEvent.getProperties()).thenReturn(props1);
    when(mergeEvent.getProperties()).thenReturn(props2);
    when(transplantEvent.getProperties()).thenReturn(props3);
    EventFilter filter = EventFilter.hasProperty("prop1", "value1");
    assertThat(filter.test(commitEvent)).isTrue();
    assertThat(filter.test(mergeEvent)).isFalse();
    assertThat(filter.test(transplantEvent)).isFalse();
  }
}
