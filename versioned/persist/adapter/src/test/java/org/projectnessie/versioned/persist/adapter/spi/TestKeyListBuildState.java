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
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

public class TestKeyListBuildState {
  @Test
  void noKeysAtAll() {
    ImmutableCommitLogEntry.Builder commit = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(0, commit, 0, 0, e -> e.getKey().toString().length());

    List<KeyListEntity> entities = keyListBuilder.finish();

    assertThat(entities).isEmpty();
    assertThat(commit.build().getKeyList()).extracting(KeyList::getKeys).asList().isEmpty();
  }

  @Test
  void oneKey() {
    ImmutableCommitLogEntry.Builder commit = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(0, commit, 0, 0, e -> e.getKey().toString().length());

    KeyListEntry entry = entry("meep");
    keyListBuilder.add(entry);

    List<KeyListEntity> entities = keyListBuilder.finish();

    assertThat(entities).isEmpty();
    assertThat(commit.build().getKeyList())
        .extracting(KeyList::getKeys)
        .asList()
        .containsExactly(entry);
  }

  @Test
  void embeddedNoEntities() {
    ImmutableCommitLogEntry.Builder commit = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(0, commit, 0, 0, e -> KeyListBuildState.MINIMUM_BUCKET_SIZE / 10);

    List<KeyListEntry> entries =
        IntStream.range(0, 9).mapToObj(i -> entry("meep-" + i)).collect(Collectors.toList());

    entries.forEach(keyListBuilder::add);

    List<KeyListEntity> entities = keyListBuilder.finish();

    assertThat(entities).isEmpty();
    assertThat(commit.build().getKeyList())
        .extracting(KeyList::getKeys)
        .asInstanceOf(list(KeyListEntry.class))
        .satisfies(this::assertSortedList)
        .containsExactlyElementsOf(entries);
  }

  @Test
  void twoBuckets() {
    ImmutableCommitLogEntry.Builder commit = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(0, commit, 0, 0, e -> KeyListBuildState.MINIMUM_BUCKET_SIZE >> 3);

    List<KeyListEntry> entries =
        IntStream.range(0, 8).mapToObj(i -> entry("meep-" + i)).collect(Collectors.toList());

    entries.forEach(keyListBuilder::add);

    List<KeyListEntity> entities = keyListBuilder.finish();

    assertThat(entities)
        .hasSize(1)
        .allSatisfy(this::assertSortedEntity)
        .flatMap(entity -> entity.getKeys().getKeys())
        .isEqualTo(
            entries.stream()
                .filter(e -> keyListBuilder.bucket(e, 2) == 1)
                .collect(Collectors.toList()));

    assertThat(commit.build().getKeyList())
        .extracting(KeyList::getKeys)
        .asInstanceOf(list(KeyListEntry.class))
        .satisfies(this::assertSortedList)
        .containsExactlyElementsOf(
            entries.stream()
                .filter(e -> keyListBuilder.bucket(e, 2) == 0)
                .collect(Collectors.toList()));
  }

  @RepeatedTest(20)
  void manyBuckets() {
    ImmutableCommitLogEntry.Builder commit = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(0, commit, 0, 0, e -> KeyListBuildState.MINIMUM_BUCKET_SIZE >> 3);

    int entryCount = 500;
    int bucketCount = entryCount / 8 + 1;

    List<KeyListEntry> entries = new ArrayList<>();
    IntStream.range(0, entryCount).mapToObj(i -> entry("meep-" + i)).forEach(entries::add);

    // randomize the order of the keys
    Collections.shuffle(entries);

    entries.forEach(keyListBuilder::add);

    List<List<KeyListEntry>> expectedEntities = new ArrayList<>();
    for (int i = 1; i < bucketCount; i++) {
      int b = i;
      expectedEntities.add(
          entries.stream()
              .filter(e -> keyListBuilder.bucket(e, bucketCount) == b)
              .sorted(Comparator.comparing(KeyListEntry::getKey))
              .collect(Collectors.toList()));
    }
    List<KeyListEntry> expectedEmbedded =
        entries.stream()
            .filter(e -> keyListBuilder.bucket(e, bucketCount) == 0)
            .sorted(Comparator.comparing(KeyListEntry::getKey))
            .collect(Collectors.toList());

    List<KeyListEntity> entities = keyListBuilder.finish();

    assertThat(entities)
        .hasSize(bucketCount - 1)
        .allSatisfy(this::assertSortedEntity)
        .map(entity -> entity.getKeys().getKeys())
        .containsExactlyElementsOf(expectedEntities);

    assertThat(commit.build().getKeyList())
        .extracting(KeyList::getKeys)
        .asInstanceOf(list(KeyListEntry.class))
        .satisfies(this::assertSortedList)
        .containsExactlyElementsOf(expectedEmbedded);
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
