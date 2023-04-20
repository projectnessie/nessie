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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TestCommitMeta {

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void getAuthor() {
    CommitMeta commitMeta = getBuilder().addAuthors("author1", "author2").build();
    assertEquals(2, commitMeta.getAuthors().size());
    assertEquals("author1", commitMeta.getAuthors().get(0));
    assertEquals("author2", commitMeta.getAuthors().get(1));
    assertEquals("author1", commitMeta.getAuthor().get());
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void getSignOff() {
    CommitMeta commitMeta = getBuilder().addSignOffs("signOff1", "signOff2").build();
    assertEquals(2, commitMeta.getSignOffs().size());
    assertEquals("signOff1", commitMeta.getSignOffs().get(0));
    assertEquals("signOff2", commitMeta.getSignOffs().get(1));
    assertEquals("signOff1", commitMeta.getSignOff().get());
  }

  @Test
  void getProperties() {
    CommitMeta commitMeta =
        getBuilder()
            .putMultiProperty("key1", Arrays.asList("value1a", "value1b"))
            .putMultiProperty("key2", Collections.singletonList("value2a"))
            .build();
    assertEquals(2, commitMeta.getMultiProperties().size());
    assertEquals(Arrays.asList("value1a", "value1b"), commitMeta.getMultiProperties().get("key1"));
    assertEquals(Collections.singletonList("value2a"), commitMeta.getMultiProperties().get("key2"));
    assertEquals(2, commitMeta.getProperties().size());
    assertEquals("value1a", commitMeta.getProperties().get("key1"));
    assertEquals("value2a", commitMeta.getProperties().get("key2"));
  }

  private static ImmutableCommitMeta.Builder getBuilder() {
    return ImmutableCommitMeta.builder()
        .committer("committer")
        .message("message")
        .commitTime(Instant.now())
        .authorTime(Instant.now());
  }
}
