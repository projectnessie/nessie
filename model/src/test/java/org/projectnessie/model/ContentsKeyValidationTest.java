/*
 * Copyright (C) 2020 Dremio
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class ContentsKeyValidationTest {

  @Test
  public void testValidation() {
    assertThatThrownBy(() -> ContentsKey.of("a", "b", "\u0000", "c", "d"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("An object key cannot contain a zero byte.");
  }

  @Test
  public void testConstruction() {
    String[] elements = {"a", "b", "c", "d"};
    ContentsKey key = ContentsKey.of(elements);
    assertThat(key.getElements()).containsExactlyElementsOf(Arrays.asList(elements));
    assertThat(key.toPathString()).isEqualTo(String.join(".", elements));
    assertThat(ContentsKey.fromPathString("a.b.c.d")).isEqualTo(key);
  }
}
