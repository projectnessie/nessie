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
package org.projectnessie.versioned.persist.adapter.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

public class TestKeyListBuildState {
  @ParameterizedTest
  @MethodSource("embedded")
  void embedded(int maxEmbeddedSize, int numEntries) {

    ImmutableCommitLogEntry.Builder commit = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(
            commit, maxEmbeddedSize, 0, e -> KeyListBuildState.MINIMUM_BUCKET_SIZE >> 3);

    List<KeyListEntry> entries = createEntries(numEntries);

    entries.forEach(keyListBuilder::add);

    List<KeyListEntity> entities = keyListBuilder.finish();

    assertThat(entities).isEmpty();
    assertThat(commit.build().getKeyList())
        .extracting(KeyList::getKeys)
        .asInstanceOf(list(KeyListEntry.class))
        .satisfies(this::assertSortedList)
        .containsExactlyElementsOf(entries);
  }

  static Stream<Arguments> embedded() {
    return Stream.of(
        arguments(4096, 0),
        arguments(512, 1),
        arguments(4096, 1),
        arguments(4096, 7),
        arguments(4096, 8));
  }

  @ParameterizedTest
  @MethodSource("nonEmbedded")
  void nonEmbedded(int numEntries, int expectedBuckets) {

    ImmutableCommitLogEntry.Builder commit = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(commit, 0, 0, e -> KeyListBuildState.MINIMUM_BUCKET_SIZE >> 3);

    List<KeyListEntry> entries = createEntries(numEntries);

    entries.forEach(keyListBuilder::add);

    List<KeyListEntity> entities = keyListBuilder.finish();

    List<List<KeyListEntry>> expectedEntityContents =
        IntStream.range(0, expectedBuckets)
            .mapToObj(
                bucket ->
                    entries.stream()
                        .filter(e -> keyListBuilder.bucket(e, expectedBuckets) == bucket)
                        .sorted(Comparator.comparing(KeyListEntry::getKey))
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());

    assertThat(commit.build().getKeyList()).isNull();

    assertThat(entities)
        .hasSize(expectedBuckets)
        .allSatisfy(this::assertSortedEntity)
        .map(entity -> entity.getKeys().getKeys())
        .isEqualTo(expectedEntityContents);
  }

  static Stream<Arguments> nonEmbedded() {
    // Can assume that 8 entries fit into one bucket
    return Stream.of(
        arguments(1, 1),
        arguments(8, 1),
        arguments(9, 2),
        arguments(10, 2),
        arguments(11, 2),
        arguments(100, 13),
        arguments(500, 63));
  }

  private List<KeyListEntry> createEntries(int numEntries) {
    return IntStream.range(0, numEntries)
        .mapToObj(i -> entry("meep-" + i))
        .collect(Collectors.toList());
  }

  private void assertSortedEntity(KeyListEntity entries) {
    assertSortedList(entries.getKeys().getKeys());
  }

  private void assertSortedList(List<? extends KeyListEntry> entries) {
    ArrayList<KeyListEntry> validate = new ArrayList<>(entries);
    validate.sort(Comparator.comparing(KeyListEntry::getKey));
    assertThat(entries).isEqualTo(validate);
  }

  private static KeyListEntry entry(String key) {
    return KeyListEntry.of(Key.of(key), ContentId.of(key), (byte) 0, randomHash());
  }

  private ImmutableCommitLogEntry.Builder newCommit() {
    return ImmutableCommitLogEntry.builder()
        .createdTime(1L)
        .hash(randomHash())
        .commitSeq(123L)
        .metadata(ByteString.EMPTY)
        .keyListDistance(42);
  }
}
