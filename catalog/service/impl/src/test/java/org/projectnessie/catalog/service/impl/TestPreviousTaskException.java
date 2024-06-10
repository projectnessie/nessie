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
package org.projectnessie.catalog.service.impl;

import static org.projectnessie.catalog.files.api.BackendErrorCode.UNKNOWN;
import static org.projectnessie.catalog.service.impl.PreviousTaskException.fromTaskState;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.catalog.files.api.BackendErrorCode;
import org.projectnessie.nessie.tasks.api.TaskState;

class TestPreviousTaskException {

  @Test
  void testFromTaskStateMissingData() {
    Assertions.assertThat(fromTaskState(null))
        .isInstanceOf(PreviousTaskException.class)
        .hasMessage("[unknown]")
        .extracting(PreviousTaskException::getErrorCode)
        .isEqualTo(UNKNOWN);

    Assertions.assertThat(fromTaskState(TaskState.failureState("msg123", null)))
        .isInstanceOf(PreviousTaskException.class)
        .hasMessage("msg123")
        .extracting(PreviousTaskException::getErrorCode)
        .isEqualTo(UNKNOWN);
  }

  @Test
  void testFromTaskStateInvalidErrorCode() {
    Assertions.assertThat(fromTaskState(TaskState.failureState("msg123", "invalid")))
        .isInstanceOf(PreviousTaskException.class)
        .hasMessage("msg123")
        .extracting(PreviousTaskException::getErrorCode)
        .isEqualTo(UNKNOWN);
  }

  @ParameterizedTest
  @EnumSource
  void testFromTaskState(BackendErrorCode errorCode) {
    Assertions.assertThat(fromTaskState(TaskState.failureState("msg123", errorCode.name())))
        .isInstanceOf(PreviousTaskException.class)
        .hasMessage("msg123")
        .extracting(PreviousTaskException::getErrorCode)
        .isEqualTo(errorCode);
  }
}
