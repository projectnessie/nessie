/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.operator.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.operator.events.EventReason.AutoscalingNotAllowed;
import static org.projectnessie.operator.events.EventReason.ReconcileError;
import static org.projectnessie.operator.events.EventReason.ReconcileSuccess;

import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import org.projectnessie.operator.exception.InvalidSpecException;
import org.projectnessie.operator.reconciler.nessie.resource.NessieBuilder;

class TestEventUtils {

  @Test
  void formatTime() {
    assertThat(EventUtils.formatTime(ZonedDateTime.parse("2006-01-02T15:04:05Z")))
        .isEqualTo("2006-01-02T15:04:05Z");
    assertThat(EventUtils.formatTime(ZonedDateTime.parse("2006-01-02T15:04:05+07:00")))
        .isEqualTo("2006-01-02T15:04:05+07:00");
    assertThat(EventUtils.formatTime(ZonedDateTime.parse("2006-01-02T15:04:05.999999Z")))
        .isEqualTo("2006-01-02T15:04:05Z");
    assertThat(EventUtils.formatTime(ZonedDateTime.parse("2006-01-02T15:04:05.999999+07:00")))
        .isEqualTo("2006-01-02T15:04:05+07:00");
  }

  @Test
  void formatMicroTime() {
    assertThat(EventUtils.formatMicroTime(ZonedDateTime.parse("2006-01-02T15:04:05Z")))
        .isEqualTo("2006-01-02T15:04:05.000000Z");
    assertThat(EventUtils.formatMicroTime(ZonedDateTime.parse("2006-01-02T15:04:05+07:00")))
        .isEqualTo("2006-01-02T15:04:05.000000+07:00");
    assertThat(EventUtils.formatMicroTime(ZonedDateTime.parse("2006-01-02T15:04:05.999999Z")))
        .isEqualTo("2006-01-02T15:04:05.999999Z");
    assertThat(EventUtils.formatMicroTime(ZonedDateTime.parse("2006-01-02T15:04:05.999999+07:00")))
        .isEqualTo("2006-01-02T15:04:05.999999+07:00");
  }

  @Test
  void eventName() {
    assertThat(
            EventUtils.eventName(
                new NessieBuilder().withNewMetadata().withUid("1234").endMetadata().build(),
                ReconcileSuccess))
        .isEqualTo("nessie-1234-ReconcileSuccess");
  }

  @Test
  void reasonFromEventName() {
    assertThat(EventUtils.reasonFromEventName("nessie-1234-ReconcileSuccess"))
        .isEqualTo(ReconcileSuccess);
  }

  @Test
  void errorReason() {
    assertThat(
            EventUtils.errorReason(new InvalidSpecException(AutoscalingNotAllowed, "irrelevant")))
        .isEqualTo(AutoscalingNotAllowed);
    assertThat(EventUtils.errorReason(new RuntimeException("test"))).isEqualTo(ReconcileError);
  }

  @Test
  void formatMessage() {
    assertThat(EventUtils.formatMessage("test")).isEqualTo("test");
    assertThat(EventUtils.formatMessage("test %s %d", "123", 456)).isEqualTo("test 123 456");
    assertThat(EventUtils.formatMessage("test %s %d", null, null)).isEqualTo("test null null");
    assertThat(EventUtils.formatMessage("x".repeat(1024))).isEqualTo("x".repeat(1024));
    assertThat(EventUtils.formatMessage("x".repeat(1025)))
        .isEqualTo("x".repeat(1009) + "... [truncated]")
        .hasSize(1024);
  }
}
