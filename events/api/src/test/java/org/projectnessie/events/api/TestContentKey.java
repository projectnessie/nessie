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
package org.projectnessie.events.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestContentKey {

  @Test
  void getName() {
    assertThat(ContentKey.of("name").getName()).isEqualTo("name");
    assertThat(ContentKey.of("parent", "name").getName()).isEqualTo("name");
  }

  @Test
  void getParent() {
    assertThat(ContentKey.of("name").getParent()).isNotPresent();
    assertThat(ContentKey.of("parent", "name").getParent()).contains(ContentKey.of("parent"));
  }
}
