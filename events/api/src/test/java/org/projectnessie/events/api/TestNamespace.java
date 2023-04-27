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

class TestNamespace {

  @Test
  void getName() {
    assertThat(ns("name").getName()).isEqualTo("name");
    assertThat(ns("name1", "name2").getName()).isEqualTo("name1.name2");
    assertThat(ns("name1", "name2", "name3").getName()).isEqualTo("name1.name2.name3");
    assertThat(ns("na.me1", "na\u0000me2").getName()).isEqualTo("na\u001dme1.na\u001dme2");
  }

  @Test
  void getSimpleName() {
    assertThat(ns("name").getSimpleName()).isEqualTo("name");
    assertThat(ns("name1", "name2").getSimpleName()).isEqualTo("name2");
    assertThat(ns("name1", "name2", "name3").getSimpleName()).isEqualTo("name3");
    assertThat(ns("na.me1", "na\u0000me2").getSimpleName()).isEqualTo("na\u0000me2");
  }

  private static Namespace ns(String... elements) {
    return ImmutableNamespace.builder().id("id").addElements(elements).build();
  }
}
