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
import static org.assertj.core.api.Assertions.entry;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TestCommitMeta {

  @Test
  void getAuthor() {
    CommitMeta commitMeta = builder().addAuthors("author1", "author2").build();
    assertThat(commitMeta.getAuthors()).containsExactly("author1", "author2");
    assertThat(commitMeta.getAuthor()).contains("author1");
  }

  @Test
  void getSignOff() {
    CommitMeta commitMeta = builder().addSignOffs("signOff1", "signOff2").build();
    assertThat(commitMeta.getSignOffs()).containsExactly("signOff1", "signOff2");
    assertThat(commitMeta.getSignOff()).contains("signOff1");
  }

  @Test
  void getProperties() {
    CommitMeta commitMeta =
        builder()
            .putMultiProperty("key1", Arrays.asList("value1a", "value1b"))
            .putMultiProperty("key2", Collections.singletonList("value2a"))
            .build();
    assertThat(commitMeta.getProperties())
        .containsOnly(entry("key1", "value1a"), entry("key2", "value2a"));
  }

  private static ImmutableCommitMeta.Builder builder() {
    return ImmutableCommitMeta.builder()
        .committer("committer")
        .message("message")
        .commitTime(Instant.now())
        .authorTime(Instant.now());
  }
}
