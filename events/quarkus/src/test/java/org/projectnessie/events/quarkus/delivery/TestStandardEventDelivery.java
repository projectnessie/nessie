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
import static org.projectnessie.events.quarkus.delivery.StandardEventDelivery.STATE_FAILURE;
import static org.projectnessie.events.quarkus.delivery.StandardEventDelivery.STATE_REJECTED;
import static org.projectnessie.events.quarkus.delivery.StandardEventDelivery.STATE_SUCCESS;

import org.junit.jupiter.api.Test;

abstract class TestStandardEventDelivery extends TestRetriableEventDelivery<StandardEventDelivery> {

  @Override
  StandardEventDelivery newDelivery() {
    return new StandardEventDelivery(event, subscriber, retryConfig, vertx);
  }

  @Override
  @Test
  void testDeliverySuccessNoRetry() {
    super.testDeliverySuccessNoRetry();
    assertThat(delivery.state).isEqualTo(STATE_SUCCESS);
  }

  @Override
  @Test
  void testDeliverySuccessWithRetry() {
    super.testDeliverySuccessWithRetry();
    assertThat(delivery.state).isEqualTo(STATE_SUCCESS);
  }

  @Override
  @Test
  void testDeliveryFailureWithRetry() {
    super.testDeliveryFailureWithRetry();
    assertThat(delivery.state).isEqualTo(STATE_FAILURE);
  }

  @Override
  @Test
  void testDeliveryRejected() {
    super.testDeliveryRejected();
    assertThat(delivery.state).isEqualTo(STATE_REJECTED);
  }
}
