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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.versioned.ResultType;

@ExtendWith(MockitoExtension.class)
class TestEventSubscribers {

  @Mock private EventSubscriber subscriber1;
  @Mock private EventSubscriber subscriber2;

  @Test
  void loadSubscribers() {
    EventSubscribers subscribers = new EventSubscribers();
    assertThat(subscribers.getSubscribers())
        .hasSize(1)
        .singleElement()
        .isInstanceOf(MockEventSubscriber.class);
  }

  @Test
  void getSubscribers() {
    EventSubscribers subscribers = new EventSubscribers(Collections.emptyList());
    assertThat(subscribers.getSubscribers()).isEmpty();
    subscribers = new EventSubscribers(subscriber1);
    assertThat(subscribers.getSubscribers()).containsExactly(subscriber1);
    subscribers = new EventSubscribers(subscriber1, subscriber2);
    assertThat(subscribers.getSubscribers()).containsExactly(subscriber1, subscriber2);
  }

  @Test
  void hasSubscribersForEventType() {
    when(subscriber1.accepts(any(EventType.class)))
        .thenAnswer(
            invocation -> {
              EventType eventType = invocation.getArgument(0);
              return eventType == EventType.COMMIT;
            });
    when(subscriber2.accepts(any(EventType.class)))
        .thenAnswer(
            invocation -> {
              EventType eventType = invocation.getArgument(0);
              return eventType == EventType.MERGE;
            });
    EventSubscribers subscribers = new EventSubscribers(Collections.emptyList());
    for (EventType eventType : EventType.values()) {
      assertThat(subscribers.hasSubscribersFor(eventType)).isFalse();
    }
    subscribers = new EventSubscribers(subscriber1);
    for (EventType eventType : EventType.values()) {
      assertThat(subscribers.hasSubscribersFor(eventType)).isEqualTo(eventType == EventType.COMMIT);
    }
    subscribers = new EventSubscribers(subscriber1, subscriber2);
    for (EventType eventType : EventType.values()) {
      assertThat(subscribers.hasSubscribersFor(eventType))
          .isEqualTo(eventType == EventType.COMMIT || eventType == EventType.MERGE);
    }
  }

  @Test
  void hasSubscribersForResultType() {
    when(subscriber1.accepts(any(EventType.class)))
        .thenAnswer(
            invocation -> {
              EventType eventType = invocation.getArgument(0);
              return eventType == EventType.MERGE;
            });
    when(subscriber2.accepts(any(EventType.class)))
        .thenAnswer(
            invocation -> {
              EventType eventType = invocation.getArgument(0);
              return eventType == EventType.CONTENT_STORED;
            });
    EventSubscribers subscribers = new EventSubscribers(Collections.emptyList());
    for (ResultType resultType : ResultType.values()) {
      assertThat(subscribers.hasSubscribersFor(resultType)).isFalse();
    }
    subscribers = new EventSubscribers(subscriber1);
    for (ResultType resultType : ResultType.values()) {
      assertThat(subscribers.hasSubscribersFor(resultType))
          .isEqualTo(resultType == ResultType.MERGE);
    }
    subscribers = new EventSubscribers(subscriber1, subscriber2);
    for (ResultType resultType : ResultType.values()) {
      assertThat(subscribers.hasSubscribersFor(resultType))
          .isEqualTo(
              resultType == ResultType.COMMIT
                  || resultType == ResultType.MERGE
                  || resultType == ResultType.TRANSPLANT);
    }
  }

  /** Expected to be loaded by ServiceLoader. */
  public static class MockEventSubscriber implements EventSubscriber {
    @Override
    public void close() {}
  }
}
