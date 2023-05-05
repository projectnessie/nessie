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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestBlockingEventDelivery extends TestStandardEventDelivery {

  @Override
  StandardEventDelivery newDelivery() {
    return new BlockingEventDelivery(event, subscriber, retryConfig, vertx);
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

  void setUpVertxExecuteBlocking() {
    doAnswer(
            invocation -> {
              Handler<Promise<Object>> blockingHandler = invocation.getArgument(0);
              Promise<Object> promise = Promise.promise();
              blockingHandler.handle(promise);
              Handler<AsyncResult<Object>> resultHandler = invocation.getArgument(2);
              resultHandler.handle(promise.future());
              return null;
            })
        .when(vertx)
        .executeBlocking(any(), eq(false), any());
  }
}
