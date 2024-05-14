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

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStatus {
  @Test
  void lookupByStatusCode() {
    assertThat(Status.fromCode(200)).isEqualTo(Status.OK);
    assertThat(Status.fromCode(401)).isEqualTo(Status.UNAUTHORIZED);
    assertThat(Status.fromCode(404)).isEqualTo(Status.NOT_FOUND);
    assertThat(Status.fromCode(504)).isEqualTo(Status.GATEWAY_TIMEOUT);
    assertThatThrownBy(() -> Status.fromCode(0))
      .hasMessage("Unknown status code 0")
      .isInstanceOf(UnsupportedOperationException.class);
  }
}
