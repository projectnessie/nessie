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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.projectnessie.operator.exception.InvalidSpecException;

class TestResourceUtils {

  @Test
  void validateName() {
    assertThatCode(() -> ResourceUtils.validateName("a")).doesNotThrowAnyException();
    assertThatCode(() -> ResourceUtils.validateName("a1")).doesNotThrowAnyException();
    assertThatCode(() -> ResourceUtils.validateName("a1-b")).doesNotThrowAnyException();
    assertThatCode(() -> ResourceUtils.validateName("a1-b2")).doesNotThrowAnyException();
    // wrong chars
    assertThatThrownBy(() -> ResourceUtils.validateName("-"))
        .isInstanceOf(InvalidSpecException.class)
        .hasMessage(
            "Resource name must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character");
    assertThatThrownBy(() -> ResourceUtils.validateName("a-"))
        .isInstanceOf(InvalidSpecException.class)
        .hasMessage(
            "Resource name must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character");
    assertThatThrownBy(() -> ResourceUtils.validateName("-a"))
        .isInstanceOf(InvalidSpecException.class)
        .hasMessage(
            "Resource name must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character");
    assertThatThrownBy(() -> ResourceUtils.validateName("1a"))
        .isInstanceOf(InvalidSpecException.class)
        .hasMessage(
            "Resource name must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character");
    assertThatThrownBy(() -> ResourceUtils.validateName("a_b"))
        .isInstanceOf(InvalidSpecException.class)
        .hasMessage(
            "Resource name must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character");
    // lengths
    assertThatCode(() -> ResourceUtils.validateName("a".repeat(63))).doesNotThrowAnyException();
    assertThatThrownBy(() -> ResourceUtils.validateName("a".repeat(64)))
        .isInstanceOf(InvalidSpecException.class)
        .hasMessage("Resource name cannot be longer than 63 characters");
    assertThatThrownBy(() -> ResourceUtils.validateName("a".repeat(11), 10))
        .isInstanceOf(InvalidSpecException.class)
        .hasMessage("Resource name cannot be longer than 10 characters");
  }
}
