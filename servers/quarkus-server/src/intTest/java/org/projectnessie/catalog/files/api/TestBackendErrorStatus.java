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
package org.projectnessie.catalog.files.api;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestBackendErrorStatus {

  private static final Throwable CAUSE = new Throwable("test");

  @ParameterizedTest
  @CsvSource({
    "42, UNKNOWN",
    "400, BAD_REQUEST",
    "401, UNAUTHORIZED",
    "403, FORBIDDEN",
    "404, NOT_FOUND",
    "429, THROTTLED",
  })
  void testFromHttpStatusCode(int inputCode, BackendErrorCode expected) {
    Assertions.assertThat(BackendErrorStatus.fromHttpStatusCode(inputCode, CAUSE))
        .extracting(BackendErrorStatus::statusCode, BackendErrorStatus::cause)
        .containsExactly(expected, CAUSE);
  }
}
