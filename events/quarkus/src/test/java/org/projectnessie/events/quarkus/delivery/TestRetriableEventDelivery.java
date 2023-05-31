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
package org.projectnessie.events.quarkus.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.quarkus.config.QuarkusEventConfig;
import org.projectnessie.events.quarkus.config.TestQuarkusEventConfig;
import org.projectnessie.events.spi.EventSubscriber;

@ExtendWith(MockitoExtension.class)
abstract class TestRetriableEventDelivery<D extends RetriableEventDelivery> {

  @Mock CommitEvent event;
  @Mock EventSubscriber subscriber;
  @Mock Vertx vertx;

  QuarkusEventConfig.RetryConfig retryConfig = new TestQuarkusEventConfig.MockRetryConfig();

  D delivery;

  abstract D newDelivery();

  @BeforeEach
  void setUp() {
    delivery = newDelivery();
  }

  @Test
  void testDeliverySuccessNoRetry() {
    when(subscriber.accepts(event)).thenReturn(true);
    delivery.start();
    verify(subscriber).onEvent(event);
  }

  @Test
  void testDeliverySuccessWithRetry() {
    when(subscriber.accepts(event)).thenReturn(true);
    setUpVertxTimer();
    AtomicReference<Throwable> errorHolder = mockSubscriberFailures(2);
    delivery.start();
    Throwable fail2 = errorHolder.get();
    assertThat(fail2).hasMessage("fail2");
    Throwable fail1 = fail2.getSuppressed()[0];
    assertThat(fail1).hasMessage("fail1");
    verify(subscriber, times(3)).onEvent(event);
  }

  @Test
  void testDeliveryFailureWithRetry() {
    when(subscriber.accepts(event)).thenReturn(true);
    setUpVertxTimer();
    AtomicReference<Throwable> errorHolder = mockSubscriberFailures(3);
    delivery.start();
    Throwable fail3 = errorHolder.get();
    assertThat(fail3).hasMessage("fail3");
    Throwable fail2 = fail3.getSuppressed()[0];
    assertThat(fail2).hasMessage("fail2");
    Throwable fail1 = fail2.getSuppressed()[0];
    assertThat(fail1).hasMessage("fail1");
    verify(subscriber, times(3)).onEvent(event);
  }

  @Test
  void testDeliveryRejected() {
    when(subscriber.accepts(event)).thenReturn(false);
    delivery.start();
    verify(subscriber, never()).onEvent(event);
  }

  AtomicReference<Throwable> mockSubscriberFailures(int numFailures) {
    AtomicInteger attempt = new AtomicInteger();
    AtomicReference<Throwable> errorHolder = new AtomicReference<>();
    doAnswer(
            invocation -> {
              int i = attempt.incrementAndGet();
              if (i <= numFailures) {
                RuntimeException exception = new RuntimeException("fail" + i);
                errorHolder.set(exception);
                throw exception;
              }
              return null;
            })
        .when(subscriber)
        .onEvent(event);
    return errorHolder;
  }

  void setUpVertxTimer() {
    when(vertx.setTimer(anyLong(), any()))
        .thenAnswer(
            invocation -> {
              Handler<Long> argument = invocation.getArgument(1);
              argument.handle(1L);
              return 1L;
            });
  }
}
