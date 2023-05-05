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
package org.projectnessie.events.quarkus.collector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.projectnessie.events.quarkus.QuarkusEventService.NESSIE_EVENTS_SERVICE_ADDR;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import java.security.Principal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.service.EventSubscribers;
import org.projectnessie.events.service.VersionStoreEvent;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.versioned.Result;
import org.projectnessie.versioned.ResultType;

@ExtendWith(MockitoExtension.class)
class TestQuarkusResultCollector {

  @Mock Result result;
  @Mock EventSubscriber subscriber;
  @Mock EventBus bus;
  @Mock DeliveryOptions options;

  QuarkusResultCollector createCollector(EventSubscribers subscribers) {
    return new QuarkusResultCollector(subscribers, "repo1", () -> "alice", bus, options);
  }

  @Test
  void testAcceptedBySubscribers() {
    when(result.getResultType()).thenReturn(ResultType.COMMIT);
    when(subscriber.accepts(any(EventType.class))).thenReturn(true);
    EventSubscribers subscribers = new EventSubscribers(subscriber);
    QuarkusResultCollector collector = createCollector(subscribers);
    collector.accept(result);
    ArgumentCaptor<VersionStoreEvent> captor = ArgumentCaptor.forClass(VersionStoreEvent.class);
    verify(bus).publish(eq(NESSIE_EVENTS_SERVICE_ADDR), captor.capture(), any());
    assertThat(captor.getValue())
        .extracting(
            VersionStoreEvent::getResult,
            versionStoreEvent -> versionStoreEvent.getUser().map(Principal::getName).orElse(null),
            VersionStoreEvent::getRepositoryId)
        .containsExactly(result, "alice", "repo1");
  }

  @Test
  void testRejectedBySubscribers() {
    when(result.getResultType()).thenReturn(ResultType.COMMIT);
    when(subscriber.accepts(any(EventType.class))).thenReturn(false);
    EventSubscribers subscribers = new EventSubscribers(subscriber);
    QuarkusResultCollector collector = createCollector(subscribers);
    collector.accept(result);
    verify(bus, never()).publish(any(), any(), any());
  }
}
