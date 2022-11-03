/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.model.Reference.ReferenceType;

class TestReference {

  @ParameterizedTest
  @CsvSource({
    "a,11223344,a@11223344",
    "a,,a",
    "a/b,11223344,a/b@11223344",
    "a/b,,a/b@",
    ",11223344,@11223344",
  })
  void toPathString(String name, String hash, String expectedResult) {
    assertThat(Reference.toPathString(name, hash)).isEqualTo(expectedResult);
  }

  @ParameterizedTest
  @CsvSource({
    "a@11223344,a,11223344",
    "a,a,",
    "a/b,a/b,",
    "a/b@11223344,a/b,11223344",
    "a@,a,",
    "a/b@,a/b,",
  })
  void fromPathString(String pathParameter, String expectedName, String expectedHash) {
    for (ReferenceType type : ReferenceType.values()) {
      assertThat(Reference.fromPathString(pathParameter, type))
          .satisfies(r -> assertThat(r.getType()).isEqualTo(type))
          .satisfies(r -> assertThat(r.getName()).isEqualTo(expectedName))
          .satisfies(r -> assertThat(r.getHash()).isEqualTo(expectedHash));
    }
  }

  @Test
  void fromPathStringDetached() {
    assertThat(Reference.fromPathString("@11223344", ReferenceType.BRANCH))
        .satisfies(r -> assertThat(r).isInstanceOf(Detached.class))
        .satisfies(r -> assertThat(r.getHash()).isEqualTo("11223344"));
  }
}
