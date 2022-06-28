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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachment.Compression;
import org.projectnessie.versioned.ContentAttachment.Format;
import org.projectnessie.versioned.ContentAttachmentKey;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/** Verifies behavior of content attachments. */
public abstract class AbstractContentAttachments {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractContentAttachments(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  void consistentAttachmentsUpdateNotPresentSuccess() {
    ContentAttachment content =
        ContentAttachment.builder()
            .key(ContentAttachmentKey.of("cid-consistentNotPresentSuccess", "snapshot", "123"))
            .format(Format.JSON)
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("snapshot stuff"))
            .version("new-hash-1")
            .build();

    assertThat(databaseAdapter.consistentPutAttachment(content, Optional.empty())).isTrue();
    assertThat(attachments(Stream.of(content.getKey()))).containsExactly(content);
  }

  @Test
  void consistentAttachmentsUpdateNotPresentFail() {
    String contentId = "cid-consistentNotPresentFail";
    ContentAttachment content =
        ContentAttachment.builder()
            .key(ContentAttachmentKey.of(contentId, "schema", "42"))
            .format(Format.JSON)
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("schema stuff"))
            .version("new-hash-2")
            .build();

    assertThat(attachmentKeys(contentId)).isEmpty();
    assertThat(databaseAdapter.consistentPutAttachment(content, Optional.of("expected-not")))
        .isFalse();
    assertThat(attachmentKeys(contentId)).isEmpty();
    assertThat(attachments(Stream.of(content.getKey()))).isEmpty();
  }

  @Test
  void consistentAttachmentsUpdate() {
    String contentId = "cid-consistentUpdate";
    ContentAttachmentKey attachmentKey = ContentAttachmentKey.of(contentId, "snapshot", "777");
    ContentAttachment content =
        ContentAttachment.builder()
            .key(attachmentKey)
            .format(Format.JSON)
            .compression(Compression.NONE)
            .data(ByteString.copyFromUtf8("snapshot stuff"))
            .version("new-hash-1")
            .build();

    assertThat(attachmentKeys(contentId)).isEmpty();
    assertThat(databaseAdapter.consistentPutAttachment(content, Optional.empty())).isTrue();
    assertThat(attachmentKeys(contentId)).containsExactly(attachmentKey);
    assertThat(attachments(Stream.of(content.getKey()))).containsExactly(content);

    ContentAttachment contentUpdate =
        ContentAttachment.builder().from(content).version("update-hash").build();

    assertThat(databaseAdapter.consistentPutAttachment(contentUpdate, Optional.empty())).isFalse();
    assertThat(attachmentKeys(contentId)).containsExactly(attachmentKey);
    assertThat(attachments(Stream.of(content.getKey()))).containsExactly(content);
    assertThat(databaseAdapter.consistentPutAttachment(contentUpdate, Optional.of("WRONG HASH")))
        .isFalse();
    assertThat(attachmentKeys(contentId)).containsExactly(attachmentKey);
    assertThat(attachments(Stream.of(content.getKey()))).containsExactly(content);
    assertThat(
            databaseAdapter.consistentPutAttachment(
                contentUpdate, Optional.of(content.getVersion())))
        .isTrue();
    assertThat(attachmentKeys(contentId)).containsExactly(attachmentKey);
    assertThat(attachments(Stream.of(content.getKey()))).containsExactly(contentUpdate);
  }

  List<ContentAttachment> attachments(Stream<ContentAttachmentKey> keys) {
    try (Stream<ContentAttachment> s = databaseAdapter.mapToAttachment(keys)) {
      return s.collect(Collectors.toList());
    }
  }

  List<ContentAttachmentKey> attachmentKeys(String contentId) {
    try (Stream<ContentAttachmentKey> s = databaseAdapter.getAttachmentKeys(contentId)) {
      return s.collect(Collectors.toList());
    }
  }

  @Test
  void emptyGetAttachments() {
    assertThat(attachments(Stream.empty())).isEmpty();
  }

  @Test
  void nonExistentAttachments() {
    assertThat(attachments(Stream.of(ContentAttachmentKey.of("foo", "no", "666")))).isEmpty();
    assertThat(attachmentKeys("foo")).isEmpty();
  }

  private static String randomString(int num) {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    StringBuilder sb = new StringBuilder(num);
    for (int i = 0; i < num; i++) {
      char c = (char) rand.nextInt(32, 126);
      // ':' character is illegal in attachment keys
      if (c == ':') {
        c = '_';
      }
      sb.append(c);
    }
    return sb.toString();
  }

  public static List<ContentAttachment> createAttachments(int numAttachments, int attachmentSize) {
    String contentId = randomString(15);
    return IntStream.range(0, numAttachments)
        .mapToObj(
            i ->
                ContentAttachment.builder()
                    .key(ContentAttachmentKey.of(contentId, "type-" + i, randomString(10)))
                    .format(Format.JSON)
                    .compression(Compression.NONE)
                    .objectId(ThreadLocalRandom.current().nextLong())
                    .data(ByteString.copyFromUtf8(randomString(attachmentSize)))
                    .build())
        .collect(Collectors.toList());
  }

  @Test
  void rewriteAttachments() {
    List<ContentAttachment> attachments1 = createAttachments(4, 20);
    List<ContentAttachmentKey> keys1 =
        attachments1.stream().map(ContentAttachment::getKey).collect(Collectors.toList());
    String contentId1 = attachments1.get(0).getKey().getContentId();

    List<ContentAttachment> attachments2 = createAttachments(4, 20);
    String contentId2 = attachments2.get(0).getKey().getContentId();
    List<ContentAttachmentKey> keys2 =
        attachments2.stream().map(ContentAttachment::getKey).collect(Collectors.toList());

    for (int i = 0; i < 3; i++) {
      databaseAdapter.putAttachments(Stream.concat(attachments1.stream(), attachments2.stream()));

      assertThat(attachments(Stream.concat(keys1.stream(), keys2.stream())))
          .containsExactlyElementsOf(
              Stream.concat(attachments1.stream(), attachments2.stream())
                  .collect(Collectors.toList()));
      assertThat(attachmentKeys(contentId1)).containsExactlyInAnyOrderElementsOf(keys1);
      assertThat(attachmentKeys(contentId2)).containsExactlyInAnyOrderElementsOf(keys2);
    }
  }

  @Test
  void mixedAttachments() {
    List<ContentAttachment> attachments = createAttachments(3, 20);
    String contentId = attachments.get(0).getKey().getContentId();

    IntStream.rangeClosed(1, 3)
        .forEach(
            i -> {
              databaseAdapter.putAttachments(attachments.stream().limit(i));

              Supplier<Stream<ContentAttachment>> attStream = () -> attachments.stream().limit(i);
              Supplier<Stream<ContentAttachmentKey>> keyStream =
                  () -> attStream.get().map(ContentAttachment::getKey);

              assertThat(attachments(keyStream.get()))
                  .containsExactlyElementsOf(attStream.get().collect(Collectors.toList()));
              assertThat(attachmentKeys(contentId))
                  .containsExactlyInAnyOrderElementsOf(
                      keyStream.get().collect(Collectors.toList()));
            });

    IntStream.rangeClosed(0, 3)
        .forEach(
            i -> {
              Supplier<Stream<ContentAttachmentKey>> deleteStream =
                  () -> attachments.stream().limit(i).map(ContentAttachment::getKey);
              Supplier<Stream<ContentAttachment>> remainAttStream =
                  () -> attachments.stream().skip(i);
              Supplier<Stream<ContentAttachmentKey>> remainKeyStream =
                  () -> remainAttStream.get().map(ContentAttachment::getKey);

              databaseAdapter.deleteAttachments(deleteStream.get());

              assertThat(attachments(remainKeyStream.get()))
                  .containsExactlyElementsOf(remainAttStream.get().collect(Collectors.toList()));
              assertThat(attachmentKeys(contentId))
                  .containsExactlyInAnyOrderElementsOf(
                      remainKeyStream.get().collect(Collectors.toList()));
            });
  }

  @ParameterizedTest
  @ValueSource(ints = {150, 250, 350, 1001})
  void manyAttachments(int numAttachments) {
    List<ContentAttachment> attachments = createAttachments(numAttachments, 10 * 1024);
    String contentId = attachments.get(0).getKey().getContentId();

    Supplier<Stream<ContentAttachment>> attStream = attachments::stream;
    Supplier<Stream<ContentAttachmentKey>> keyStream =
        () -> attStream.get().map(ContentAttachment::getKey);

    databaseAdapter.putAttachments(attStream.get());

    assertThat(attachments(keyStream.get()))
        .containsExactlyElementsOf(attStream.get().collect(Collectors.toList()));
    assertThat(attachmentKeys(contentId))
        .containsExactlyInAnyOrderElementsOf(keyStream.get().collect(Collectors.toList()));
  }

  @Test
  void manyAttachmentWriteReadCalls() {
    List<ContentAttachment> attachments = createAttachments(1, 10);
    String contentId = attachments.get(0).getKey().getContentId();

    Supplier<Stream<ContentAttachment>> attStream = attachments::stream;
    Supplier<Stream<ContentAttachmentKey>> keyStream =
        () -> attStream.get().map(ContentAttachment::getKey);

    for (int i = 0; i < 20; i++) {
      databaseAdapter.putAttachments(attStream.get());

      assertThat(attachments(keyStream.get()))
          .containsExactlyElementsOf(attStream.get().collect(Collectors.toList()));
      assertThat(attachmentKeys(contentId))
          .containsExactlyInAnyOrderElementsOf(keyStream.get().collect(Collectors.toList()));
    }
  }
}
