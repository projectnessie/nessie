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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestReference {

  @ParameterizedTest
  @ValueSource(strings = {"branch", "BRANCH"})
  void branch(String refType) {
    Reference ref =
        ImmutableReference.builder()
            .simpleName("branch1")
            .fullName("refs/heads/branch1")
            .type(refType)
            .build();
    assertThat(ref.isBranch()).isTrue();
    assertThat(ref.isTag()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"tag", "TAG"})
  void tag(String refType) {
    Reference ref =
        ImmutableReference.builder()
            .simpleName("tag1")
            .fullName("refs/tags/tag1")
            .type(refType)
            .build();
    assertThat(ref.isBranch()).isFalse();
    assertThat(ref.isTag()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"other", "OTHER"})
  void other(String refType) {
    Reference ref =
        ImmutableReference.builder()
            .simpleName("other1")
            .fullName("refs/other/other1")
            .type(refType)
            .build();
    assertThat(ref.isBranch()).isFalse();
    assertThat(ref.isTag()).isFalse();
  }
}
