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
package org.projectnessie.client.http;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestNessieApiCompatibilityException {

  @Test
  void testMessages() {
    NessieApiCompatibilityException e = new NessieApiCompatibilityException(1, 2, 3);
    assertThat(e.getMessage())
        .isEqualTo("API version 1 is too old for server (minimum supported version is 2)");
    e = new NessieApiCompatibilityException(5, 3, 4);
    assertThat(e.getMessage())
        .isEqualTo("API version 5 is too new for server (maximum supported version is 4)");
    e = new NessieApiCompatibilityException(3, 2, 4, 2);
    assertThat(e.getMessage())
        .isEqualTo("API version mismatch, check URI prefix (expected: 3, actual: 2)");
  }

  @Test
  void testGetters() {
    NessieApiCompatibilityException e = new NessieApiCompatibilityException(1, 2, 4, 3);
    assertThat(e.getClientApiVersion()).isEqualTo(1);
    assertThat(e.getMinServerApiVersion()).isEqualTo(2);
    assertThat(e.getMaxServerApiVersion()).isEqualTo(4);
    assertThat(e.getActualServerApiVersion()).isEqualTo(3);
  }
}
