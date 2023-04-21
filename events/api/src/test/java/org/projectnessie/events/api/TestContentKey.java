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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TestContentKey {

  @Test
  void getName() {
    ContentKey contentKey = ContentKey.of("name");
    assertEquals("name", contentKey.getName());
    contentKey = ContentKey.of("parent", "name");
    assertEquals("name", contentKey.getName());
  }

  @Test
  void getParent() {
    ContentKey contentKey = ContentKey.of("name");
    assertFalse(contentKey.getParent().isPresent());
    contentKey = ContentKey.of("parent", "name");
    assertTrue(contentKey.getParent().isPresent());
    assertEquals(ContentKey.of("parent"), contentKey.getParent().get());
  }
}
