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
    assertThat(ContentKey.of("name1", "name2").getName()).isEqualTo("name1.name2");
    assertThat(ContentKey.of("name1", "name2", "name3").getName()).isEqualTo("name1.name2.name3");
    assertThat(ContentKey.of("na.me1", "na\u0000me2").getName())
        .isEqualTo("na\u001dme1.na\u001dme2");
  }

  @Test
  void getSimpleName() {
    assertThat(ContentKey.of("name").getSimpleName()).isEqualTo("name");
    assertThat(ContentKey.of("name1", "name2").getSimpleName()).isEqualTo("name2");
    assertThat(ContentKey.of("name1", "name2", "name3").getSimpleName()).isEqualTo("name3");
    assertThat(ContentKey.of("na.me1", "na\u0000me2").getSimpleName()).isEqualTo("na\u0000me2");
  }

  @Test
  void getParent() {
    assertThat(ContentKey.of("name").getParent()).isNotPresent();
    assertThat(ContentKey.of("parent", "name").getParent()).contains(ContentKey.of("parent"));
  }

  @Test
  void compareTo() {
    assertThat(ContentKey.of("name").compareTo(ContentKey.of("name"))).isEqualTo(0);
    assertThat(ContentKey.of("name").compareTo(ContentKey.of("name2"))).isLessThan(0);
    assertThat(ContentKey.of("name2").compareTo(ContentKey.of("name"))).isGreaterThan(0);
    assertThat(ContentKey.of("name").compareTo(ContentKey.of("parent", "name"))).isLessThan(0);
    assertThat(ContentKey.of("parent", "name").compareTo(ContentKey.of("name"))).isGreaterThan(0);
    assertThat(ContentKey.of("parent", "name").compareTo(ContentKey.of("parent", "name")))
        .isEqualTo(0);
    assertThat(ContentKey.of("parent", "name").compareTo(ContentKey.of("parent", "name2")))
        .isLessThan(0);
    assertThat(ContentKey.of("parent", "name2").compareTo(ContentKey.of("parent", "name")))
        .isGreaterThan(0);
    assertThat(ContentKey.of("parent", "name").compareTo(ContentKey.of("parent2", "name")))
        .isLessThan(0);
    assertThat(ContentKey.of("parent2", "name").compareTo(ContentKey.of("parent", "name")))
        .isGreaterThan(0);
  }
}
