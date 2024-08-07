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
package org.projectnessie.client.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestStatus {
  @ParameterizedTest
  @MethodSource
  public void statusCodes(int code) {
    assertThat(Status.fromCode(code)).isNotNull().extracting(Status::getCode).isEqualTo(code);
    assertThat(Status.fromCode(code)).isNotNull().extracting(Status::getReason).isNotNull();
  }

  static Stream<Integer> statusCodes() {
    return IntStream.rangeClosed(0, 999).boxed();
  }

  @ParameterizedTest
  @MethodSource
  public void invalidStatusCodes(int code) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Status.fromCode(code))
        .withMessage("Illegal HTTP status code %d", code);
  }

  static Stream<Integer> invalidStatusCodes() {
    return IntStream.of(-1, -100, 1000, Integer.MAX_VALUE, Integer.MIN_VALUE).boxed();
  }
}
