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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachment.Compression;
import org.projectnessie.versioned.ContentAttachmentKey;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/**
 * Verifies that a big-ish number of keys, split across multiple commits works and the correct
 * results are returned for the commit-log, keys, global-states. This test is especially useful to
 * verify that the embedded and nested key-lists (think: full-key-lists in a commit-log-entry) work
 * correctly.
 */
public abstract class AbstractBinary {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractBinary(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  void consistentNotPresentSuccess() {
    ContentAttachment content =
        ContentAttachment.builder()
            .key(ContentAttachmentKey.of("cid-consistentNotPresentSuccess", "snapshot", "123"))
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("snapshot stuff"))
            .version("new-hash-1")
            .build();

    assertThat(databaseAdapter.consistentPutAttachment(content, Optional.empty())).isTrue();
    assertThat(databaseAdapter.getAttachments(Stream.of(content.getKey())))
        .containsExactly(content);
  }

  @Test
  void consistentNotPresentFail() {
    ContentAttachment content =
        ContentAttachment.builder()
            .key(ContentAttachmentKey.of("cid-consistentNotPresentFail", "schema", "42"))
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("schema stuff"))
            .version("new-hash-2")
            .build();

    assertThat(databaseAdapter.consistentPutAttachment(content, Optional.of("expected-not")))
        .isFalse();
    assertThat(databaseAdapter.getAttachments(Stream.of(content.getKey()))).isEmpty();
  }

  @Test
  void consistentUpdate() {
    ContentAttachment content =
        ContentAttachment.builder()
            .key(ContentAttachmentKey.of("cid-consistentUpdate", "snapshot", "777"))
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("snapshot stuff"))
            .version("new-hash-1")
            .build();

    assertThat(databaseAdapter.consistentPutAttachment(content, Optional.empty())).isTrue();
    assertThat(databaseAdapter.getAttachments(Stream.of(content.getKey())))
        .containsExactly(content);

    ContentAttachment contentUpdate =
        ContentAttachment.builder().from(content).version("update-hash").build();

    assertThat(databaseAdapter.consistentPutAttachment(contentUpdate, Optional.empty())).isFalse();
    assertThat(databaseAdapter.getAttachments(Stream.of(content.getKey())))
        .containsExactly(content);
    assertThat(databaseAdapter.consistentPutAttachment(contentUpdate, Optional.of("WRONG HASH")))
        .isFalse();
    assertThat(databaseAdapter.getAttachments(Stream.of(content.getKey())))
        .containsExactly(content);
    assertThat(
            databaseAdapter.consistentPutAttachment(
                contentUpdate, Optional.of(content.getVersion())))
        .isTrue();
    assertThat(databaseAdapter.getAttachments(Stream.of(content.getKey())))
        .containsExactly(contentUpdate);
  }

  @Test
  void emptyGet() {
    try (Stream<ContentAttachment> s = databaseAdapter.getAttachments(Stream.empty())) {
      assertThat(s).isEmpty();
    }
  }

  @Test
  void nonExistent() {
    try (Stream<ContentAttachment> s =
        databaseAdapter.getAttachments(Stream.of(ContentAttachmentKey.of("foo", "no", "666")))) {
      assertThat(s).isEmpty();
    }
  }

  @Test
  void mixed() {
    ContentAttachment content1 =
        ContentAttachment.builder()
            .key(ContentAttachmentKey.of("cid-mixed", "snapshot", "123"))
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("snapshot stuff"))
            .build();
    ContentAttachment content2 =
        ContentAttachment.builder()
            .key(ContentAttachmentKey.of("cid-mixed", "schema", "42"))
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("schema stuff"))
            .build();

    databaseAdapter.putAttachments(Stream.of(content1, content2));

    try (Stream<ContentAttachment> s =
        databaseAdapter.getAttachments(Stream.of(content1.getKey(), content2.getKey()))) {
      assertThat(s).containsExactlyInAnyOrder(content1, content2);
    }

    try (Stream<ContentAttachment> s =
        databaseAdapter.getAttachments(
            Stream.of(
                content1.getKey(),
                ContentAttachmentKey.of("cid-mixes", "666", "snapshot"),
                content2.getKey()))) {
      assertThat(s).containsExactlyInAnyOrder(content1, content2);
    }

    databaseAdapter.deleteAttachments(
        Stream.of(
            content1.getKey(),
            ContentAttachmentKey.of("cid-mixes", "666", "snapshot"),
            content2.getKey()));

    try (Stream<ContentAttachment> s =
        databaseAdapter.getAttachments(
            Stream.of(
                content1.getKey(),
                ContentAttachmentKey.of("cid-mixes", "666", "snapshot"),
                content2.getKey()))) {
      assertThat(s).isEmpty();
    }
  }
}
