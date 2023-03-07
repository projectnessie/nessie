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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.adapter.spi.KeyListBuildState.MINIMUM_BUCKET_SIZE;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

public class TestKeyListBuildState {

  static Stream<Arguments> keyNotFoundScenarios() {
    return Stream.of(
        Arguments.of(
            singletonList(ContentKey.of("existing", "key")), ContentKey.of("non_existing.key"), 2),
        Arguments.of(
            asList(
                ContentKey.of("db", "from_spark"),
                ContentKey.of("db", "from_flink"),
                ContentKey.of("db", "from_presto")),
            ContentKey.of("db.from_spark"),
            1),
        Arguments.of(
            IntStream.range(0, 60)
                .mapToObj(i -> ContentKey.of("db", "table_" + i))
                .collect(Collectors.toList()),
            ContentKey.of("db.meep"),
            1),
        Arguments.of(
            IntStream.range(0, 100)
                .mapToObj(i -> ContentKey.of("db", "table_" + i))
                .collect(Collectors.toList()),
            ContentKey.of("db.meep"),
            1));
  }

  @ParameterizedTest
  @MethodSource("keyNotFoundScenarios")
  public void keyNotFoundScenario(
      List<ContentKey> keyList, ContentKey keyToLookup, int emptyOnRound) {
    ImmutableCommitLogEntry.Builder commitLogEntry = newCommit();
    KeyListBuildState buildState = new KeyListBuildState(commitLogEntry, 50, 50, 0.65f, e -> 1);
    keyList.stream()
        .map(k -> KeyListEntry.of(k, ContentId.of("id1"), (byte) 99, Hash.of("1234")))
        .forEach(buildState::add);

    Map<Hash, KeyListEntity> persisted =
        buildState.finish().stream().collect(Collectors.toMap(KeyListEntity::getId, e -> e));

    CommitLogEntry entry = commitLogEntry.build();

    FetchValuesUsingOpenAddressing helper = new FetchValuesUsingOpenAddressing(entry);

    List<KeyListEntry> keyListEntries = new ArrayList<>();

    Collection<ContentKey> remainingKeys = singletonList(keyToLookup);

    for (int round = 0; !remainingKeys.isEmpty(); round++) {
      List<Hash> entitiesToFetch = helper.entityIdsToFetch(round, 0, remainingKeys);

      // Fetch the key-list-entities for the identified segments, store those
      entitiesToFetch.stream().map(persisted::remove).forEach(helper::entityLoaded);

      // Try to extract the key-list-entries for the remainingKeys and add to the result list
      remainingKeys = helper.checkForKeys(round, remainingKeys, keyListEntries::add);

      if (emptyOnRound == round) {
        assertThat(remainingKeys).isEmpty();
      }
    }
  }

  /**
   * Exercises a bunch of combinations of number of {@link KeyListEntry}s with different
   * combinations of load-factor and segment sizes. Small segments and load factors above 0.65 are
   * bad, as those would lead to many database fetches. Looks like good configurations are:
   *
   * <ul>
   *   <li>segment size of 128kB or more with a load factor of 0.65
   *   <li>segment sizes of 64kB or more with a load factor of 0.45
   * </ul>
   */
  static Stream<Arguments> openAddressing() {
    return Stream.of(
        arguments(0, 0, 0, 0, 0, 0.65f, 0),
        arguments(0, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 0, 0, 0.65f, 0),
        // single key, fits into embedded key list
        arguments(1, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 1, 0, 0.45f, 0),
        // single key, does not fit
        arguments(1, 0, MINIMUM_BUCKET_SIZE, 0, 1, 0.0000001f, 0), // extremely low load factor
        arguments(1, 0, MINIMUM_BUCKET_SIZE, 0, 1, 0.45f, 0),
        arguments(1, (MINIMUM_BUCKET_SIZE >> 3) - 1, MINIMUM_BUCKET_SIZE, 0, 1, 0.45f, 0),
        // single key, does not fit
        arguments(1, 0, MINIMUM_BUCKET_SIZE, 0, 1, 0.45f, 0),
        // 16 keys fill up the embedded key-list and overflow to a single bucket
        arguments(16, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 8, 1, 0.45f, 1),
        // Just some more test cases with many entries
        arguments(500, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 8, 62, 0.65f, 30),
        arguments(2000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 8, 249, 0.65f, 30),
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 8, 2499, 0.65f, 30),
        // 16kB segments
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE * 4, 8, 625, 0.65f, 10),
        // 64kB segments
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE * 16, 8, 157, 0.65f, 2),
        // 128kB segments
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE * 32, 8, 79, 0.65f, 1),
        // different load-factor
        arguments(500, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 8, 62, 0.45f, 3),
        arguments(2000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 8, 249, 0.45f, 3),
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE, 8, 2499, 0.45f, 3),
        // 16kB segments
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE * 4, 8, 625, 0.45f, 1),
        // 64kB segments
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE * 16, 8, 157, 0.45f, 1),
        // 128kB segments
        arguments(20000, MINIMUM_BUCKET_SIZE, MINIMUM_BUCKET_SIZE * 32, 8, 79, 0.45f, 1));
  }

  @Test
  public void nextPowerOfTwoBucketCount() {
    // This case involves a computed shift distance of 32 on type int.  As described in JLS 11
    // section 15.19 Shift Operators, only the five lowest-order bits of the distance are actually
    // used for an int's shift distance, so the effective shift distance is 0.
    assertThat(KeyListBuildState.nextPowerOfTwo(0)).isEqualTo(1);

    assertThat(KeyListBuildState.nextPowerOfTwo(1)).isEqualTo(1);
    assertThat(KeyListBuildState.nextPowerOfTwo((1 << 29) + 1)).isEqualTo(1 << 30);
    assertThat(KeyListBuildState.nextPowerOfTwo((1 << 30) - 1)).isEqualTo(1 << 30);
    assertThat(KeyListBuildState.nextPowerOfTwo(1 << 30)).isEqualTo(1 << 30);
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -2, -1, (1 << 30) + 1, (1 << 30) + 2, Integer.MAX_VALUE})
  public void nextPowerOfTwoBucketCountException(int invalidParameter) {
    final String expectedMessageFragment = "must be between 0 and 2^30";
    assertThatThrownBy(() -> KeyListBuildState.nextPowerOfTwo(invalidParameter))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessageFragment);
  }

  @ParameterizedTest
  @MethodSource("openAddressing")
  void openAddressing(
      int numEntries,
      int maxEmbeddedSize,
      int maxEntitySize,
      int expectedEmbeddedKeys,
      int expectedBuckets,
      float loadFactor,
      // one "fetch" represents an _additional_ "query" against a key-list-entity
      int maxAllowedFetches) {
    ImmutableCommitLogEntry.Builder commitBuilder = newCommit();
    KeyListBuildState keyListBuilder =
        new KeyListBuildState(
            commitBuilder,
            maxEmbeddedSize,
            maxEntitySize,
            loadFactor,
            e -> MINIMUM_BUCKET_SIZE >> 3);

    List<KeyListEntry> entries = createEntries(numEntries);
    entries.forEach(keyListBuilder::add);

    List<KeyListEntity> entities = keyListBuilder.finish();

    CommitLogEntry commit = commitBuilder.build();

    List<Integer> offsets =
        // Optional to make offsets effectively final
        Optional.ofNullable(commit.getKeyListEntityOffsets()).orElse(Collections.emptyList());

    assertThat(entities.size()).isEqualTo(offsets.size());
    assertThat(offsets).isEqualTo(offsets.stream().sorted().collect(Collectors.toList()));

    assertThat(commit.getKeyList())
        .extracting(KeyList::getKeys)
        .asInstanceOf(list(KeyListEntry.class))
        .filteredOn(Objects::nonNull)
        .hasSize(expectedEmbeddedKeys);

    assertThat(entities).hasSize(expectedBuckets);

    int totalBuckets = keyListBuilder.openAddressingBucketCount();
    int hashMask = totalBuckets - 1;
    assertThat(entries)
        .allSatisfy(
            entry -> {
              // The following code is effectively the logic to do a point-query for one key.

              // Get the open-addressing hash-bucket index
              int bucket = entry.getKey().hashCode() & hashMask;

              // Find the segment for the hash-bucket index.
              // Notes:
              // - segment 0 is the embedded key-list
              // - segment 1 is the first key-list-entity
              // - segment 2 is the second key-list-entity
              // - and so on...
              int segment = 0;
              // The "overall" hash-bucket index is represented by the first bucket in 'segment'
              int firstBucketInSegment = 0;
              for (int seg = 0; seg < offsets.size(); seg++) {
                int segOffset = offsets.get(seg);
                if (segOffset <= bucket) {
                  segment = seg + 1;
                  firstBucketInSegment = segOffset;
                } else {
                  break;
                }
              }

              // Relative index of the "overall" hash-bucket in the next segment
              int keyListOffset = bucket - firstBucketInSegment;
              int fetches = 0;
              if (keyListOffset >= 0) {
                for (int seg = segment; ; ) {
                  KeyList keyList =
                      seg == 0 ? commit.getKeyList() : entities.get(seg - 1).getKeys();
                  List<KeyListEntry> keys = keyList.getKeys();
                  for (int i = keyListOffset; i < keys.size(); i++) {
                    KeyListEntry key = keys.get(i);
                    if (key != null && key.equals(entry)) {
                      assertThat(fetches)
                          .describedAs("Fetches too often")
                          .isLessThanOrEqualTo(maxAllowedFetches);
                      return;
                    }
                    if (key == null) {
                      fail("KeyListEntry not found", entry);
                    }
                  }

                  // next segment
                  keyListOffset = 0;
                  seg++;
                  if (seg > entities.size()) {
                    seg = 0;
                  }
                  if (seg > 0) {
                    fetches++;
                  }
                }
              }
            });
  }

  private List<KeyListEntry> createEntries(int numEntries) {
    return IntStream.range(0, numEntries)
        .mapToObj(i -> entry("meep-" + i))
        .collect(Collectors.toList());
  }

  private static KeyListEntry entry(String key) {
    return KeyListEntry.of(ContentKey.of(key), ContentId.of(key), (byte) 99, randomHash());
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
