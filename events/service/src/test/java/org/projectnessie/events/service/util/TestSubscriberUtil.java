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
package org.projectnessie.events.service.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.events.spi.EventSubscriber;

@ExtendWith(MockitoExtension.class)
class TestSubscriberUtil {

  @Mock EventSubscriber subscriber;

  @ParameterizedTest
  @MethodSource
  void notifySubscriber(Class<? extends Event> eventClass, Consumer<EventSubscriber> verification) {
    Event event = mock(eventClass, InvocationOnMock::callRealMethod);
    SubscriberUtil.notifySubscriber(subscriber, event);
    verification.accept(verify(subscriber));
  }

  public static Stream<Arguments> notifySubscriber() {
    return Stream.of(
        Arguments.of(CommitEvent.class, (Consumer<EventSubscriber>) s -> s.onCommit(any())),
        Arguments.of(MergeEvent.class, (Consumer<EventSubscriber>) s -> s.onMerge(any())),
        Arguments.of(TransplantEvent.class, (Consumer<EventSubscriber>) s -> s.onTransplant(any())),
        Arguments.of(
            ReferenceCreatedEvent.class,
            (Consumer<EventSubscriber>) s -> s.onReferenceCreated(any())),
        Arguments.of(
            ReferenceUpdatedEvent.class,
            (Consumer<EventSubscriber>) s -> s.onReferenceUpdated(any())),
        Arguments.of(
            ReferenceDeletedEvent.class,
            (Consumer<EventSubscriber>) s -> s.onReferenceDeleted(any())),
        Arguments.of(
            ContentStoredEvent.class, (Consumer<EventSubscriber>) s -> s.onContentStored(any())),
        Arguments.of(
            ContentRemovedEvent.class, (Consumer<EventSubscriber>) s -> s.onContentRemoved(any())));
  }
}
