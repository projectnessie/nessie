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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.MergeEvent;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("resource")
class TestEventSubscriber {

  @Mock CommitEvent commit1;
  @Mock CommitEvent commit2;
  @Mock MergeEvent merge;

  @Test
  void acceptsEventType() {
    EventSubscriber subscriber =
        new EventSubscriber() {

          @Override
          public void onSubscribe(EventSubscription subscription) {}

          @Override
          public EventTypeFilter getEventTypeFilter() {
            return EventTypeFilter.of(EventType.COMMIT);
          }

          @Override
          public void close() {}
        };
    assertThat(subscriber.accepts(EventType.COMMIT)).isTrue();
    assertThat(subscriber.accepts(EventType.MERGE)).isFalse();
    assertThat(subscriber.accepts(EventType.TRANSPLANT)).isFalse();
  }

  @Test
  void acceptsEvent() {
    when(commit1.getType()).thenReturn(EventType.COMMIT);
    when(commit2.getType()).thenReturn(EventType.COMMIT);
    when(merge.getType()).thenReturn(EventType.MERGE);
    when(commit1.getRepositoryId()).thenReturn("repo1");
    when(commit2.getRepositoryId()).thenReturn("repo2");
    EventSubscriber subscriber =
        new EventSubscriber() {
          @Override
          public void onSubscribe(EventSubscription subscription) {}

          @Override
          public EventTypeFilter getEventTypeFilter() {
            return EventTypeFilter.of(EventType.COMMIT);
          }

          @Override
          public EventFilter getEventFilter() {
            return e -> e.getRepositoryId().equals("repo1");
          }

          @Override
          public void close() {}
        };
    assertThat(subscriber.accepts(commit1)).isTrue();
    assertThat(subscriber.accepts(commit2)).isFalse();
    assertThat(subscriber.accepts(merge)).isFalse();
  }
}
