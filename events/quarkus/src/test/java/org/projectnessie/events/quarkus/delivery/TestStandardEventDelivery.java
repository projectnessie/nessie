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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

abstract class TestStandardEventDelivery extends TestRetriableEventDelivery<StandardEventDelivery> {

  @Override
  StandardEventDelivery newDelivery() {
    StandardEventDelivery spy =
        spy(new StandardEventDelivery(event, subscriber, retryConfig, vertx));
    spy.setSelf(spy);
    return spy;
  }

  @Override
  @Test
  void testDeliverySuccessNoRetry() {
    super.testDeliverySuccessNoRetry();
    verify(delivery).deliverySuccessful(1);
  }

  @Override
  @Test
  void testDeliverySuccessWithRetry() {
    super.testDeliverySuccessWithRetry();
    verify(delivery).deliverySuccessful(3);
  }

  @Override
  @Test
  void testDeliveryFailureWithRetry() {
    super.testDeliveryFailureWithRetry();
    verify(delivery).deliveryFailed(eq(3), any());
  }

  @Override
  @Test
  void testDeliveryRejected() {
    super.testDeliveryRejected();
    verify(delivery).deliveryRejected();
  }
}
