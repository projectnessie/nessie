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
package com.dremio.nessie.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TestContentsKeyEncoding {

  @Test
  void singleByte() {
    assertRoundTrip("a.b","c.d");
  }

  @Test
  void strangeCharacters() {
    assertRoundTrip("/%","#&&");
  }

  @Test
  void doubleByte() {
    assertRoundTrip("/%国","国.国");
  }

  @Test
  void blockZeroByteUsage() {
    assertThrows(IllegalArgumentException.class, () -> ContentsKey.of("\u0000"));
  }

  private void assertRoundTrip(String... elements) {
    ContentsKey k = ContentsKey.of(elements);
    ContentsKey k2 = ContentsKey.fromEncoded(k.toPathString());
    assertEquals(k, k2);
  }
}
