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
import static org.projectnessie.events.quarkus.collector.QuarkusMetricsResultCollector.NESSIE_RESULTS_REJECTED;
import static org.projectnessie.events.quarkus.collector.QuarkusMetricsResultCollector.NESSIE_RESULTS_TOTAL;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.projectnessie.events.service.EventSubscribers;

class TestQuarkusMetricsResultCollector extends TestQuarkusResultCollector {

  MeterRegistry registry = new SimpleMeterRegistry();

  @Override
  QuarkusResultCollector createCollector(EventSubscribers subscribers) {
    return new QuarkusMetricsResultCollector(
        subscribers, "repo1", () -> "alice", bus, options, registry);
  }

  @Override
  @Test
  void testAcceptedBySubscribers() {
    super.testAcceptedBySubscribers();
    assertThat(registry.get(NESSIE_RESULTS_TOTAL).counter().count()).isEqualTo(1);
    assertThat(registry.find(NESSIE_RESULTS_REJECTED).counter()).isNull();
  }

  @Override
  @Test
  void testRejectedBySubscribers() {
    super.testRejectedBySubscribers();
    assertThat(registry.get(NESSIE_RESULTS_TOTAL).counter().count()).isEqualTo(1);
    assertThat(registry.get(NESSIE_RESULTS_REJECTED).counter().count()).isEqualTo(1);
  }
}
