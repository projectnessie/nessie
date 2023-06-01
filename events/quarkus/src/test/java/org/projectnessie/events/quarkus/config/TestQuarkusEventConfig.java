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
package org.projectnessie.events.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class TestQuarkusEventConfig {

  @Test
  void retryConfigNextDelay() {
    QuarkusEventConfig.RetryConfig rc = new MockRetryConfig();
    assertThat(rc.getNextDelay(Duration.ofSeconds(1))).isEqualTo(Duration.ofSeconds(2));
    assertThat(rc.getNextDelay(Duration.ofSeconds(2))).isEqualTo(Duration.ofSeconds(4));
    assertThat(rc.getNextDelay(Duration.ofSeconds(4))).isEqualTo(Duration.ofSeconds(5));
    assertThat(rc.getNextDelay(Duration.ofSeconds(8))).isEqualTo(Duration.ofSeconds(5));
  }

  public static class MockRetryConfig implements QuarkusEventConfig.RetryConfig {

    @Override
    public int getMaxAttempts() {
      return 3;
    }

    @Override
    public Duration getInitialDelay() {
      return Duration.ofSeconds(1);
    }

    @Override
    public Duration getMaxDelay() {
      return Duration.ofSeconds(5);
    }
  }
}
