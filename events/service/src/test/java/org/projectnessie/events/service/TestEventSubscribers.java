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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.versioned.ResultType;

@ExtendWith(MockitoExtension.class)
class TestEventSubscribers {

  @Mock private EventSubscriber subscriber1;
  @Mock private EventSubscriber subscriber2;
  @Mock private EventSubscriber subscriber3;

  @Test
  void startSuccess() {
    EventSubscribers subscribers = new EventSubscribers(subscriber1, subscriber2);
    subscribers.start(s -> mock(EventSubscription.class));
    assertThat(subscribers.getSubscriptions().values())
        .containsExactlyInAnyOrder(subscriber1, subscriber2);
    verify(subscriber1).onSubscribe(any());
    verify(subscriber1).onSubscribe(any());
  }

  @Test
  void startFailure() {
    EventSubscribers subscribers = new EventSubscribers(subscriber1, subscriber2);
    doThrow(new RuntimeException("subscriber1")).when(subscriber1).onSubscribe(any());
    assertThatThrownBy(() -> subscribers.start(s -> mock(EventSubscription.class)))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Error starting subscriber")
        .hasCause(new RuntimeException("subscriber1"));
    verify(subscriber1).onSubscribe(any());
    verify(subscriber2, never()).onSubscribe(any());
  }

  @Test
  void close() throws Exception {
    EventSubscribers subscribers = new EventSubscribers(subscriber1, subscriber2, subscriber3);
    doThrow(new RuntimeException("subscriber1")).when(subscriber1).close();
    doThrow(new RuntimeException("subscriber2")).when(subscriber2).close();
    assertThatThrownBy(subscribers::close)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Error closing at least one subscriber")
        .hasSuppressedException(new RuntimeException("subscriber2"))
        .hasSuppressedException(new RuntimeException("subscriber1"));
    verify(subscriber1).close();
    verify(subscriber2).close();
    verify(subscriber3).close();
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
    @SuppressWarnings("resource")
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
    @SuppressWarnings("resource")
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
}
