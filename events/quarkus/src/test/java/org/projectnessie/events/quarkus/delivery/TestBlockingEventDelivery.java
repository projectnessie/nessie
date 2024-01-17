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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestBlockingEventDelivery extends TestStandardEventDelivery {

  @Override
  StandardEventDelivery newDelivery() {
    BlockingEventDelivery spy =
        spy(new BlockingEventDelivery(event, subscriber, retryConfig, vertx));
    spy.setSelf(spy);
    return spy;
  }

  @Test
  @Override
  void testDeliverySuccessNoRetry() {
    setUpVertxExecuteBlocking();
    super.testDeliverySuccessNoRetry();
  }

  @Test
  @Override
  void testDeliverySuccessWithRetry() {
    setUpVertxExecuteBlocking();
    super.testDeliverySuccessWithRetry();
  }

  @Test
  @Override
  void testDeliveryFailureWithRetry() {
    setUpVertxExecuteBlocking();
    super.testDeliveryFailureWithRetry();
  }

  @SuppressWarnings("unchecked")
  void setUpVertxExecuteBlocking() {
    doAnswer(
            invocation -> {
              Callable<Void> task = invocation.getArgument(0);
              task.call();
              return null;
            })
        .when(vertx)
        .executeBlocking(any(Callable.class), eq(false));
  }
}
