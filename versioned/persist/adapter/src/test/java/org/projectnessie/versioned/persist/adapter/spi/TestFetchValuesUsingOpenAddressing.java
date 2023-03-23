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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableContentId;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyListEntity;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

public class TestFetchValuesUsingOpenAddressing {

  /**
   * Test finding non-existent keys pushed out of the final (highest-indexed) segment by collisions.
   *
   * <p>This implies sufficient collisions to cause an insertion's bucket index to overrun the total
   * bucket count, effectively "wrapping" around the segment and bucket indices to the beginning
   * (i.e. the embedded segment). We'll also simulate exceptionally unlikely cases involving probing
   * through multiple segments before finding an open slot, though this generally implies a
   * misconfigured load factor and target size.
   */
  @Test
  public void segmentWrapAroundWithAbsentKey() {
    // Create four segments with two buckets each
    // The hashes of the non-embedded/external segments will be 01, 02, and 03 (as hex strings)
    FetchValuesUsingOpenAddressing fetch =
        new FetchValuesUsingOpenAddressing(
            getCommitFixture(2, 4, KeyWithSpecificHash::withMaxIntHash));

    // Make a key that won't match anything in the commit
    ContentKey nonExistentKey = new KeyWithSpecificHash(Integer.MAX_VALUE, ImmutableList.of("foo"));

    // Look for a non-existent key.
    // The fake segments here are all full, so this will probe through the entire hashtable.
    // Expect the final segment in round 0, then no segments (implying embedded) in round 1,
    // the first non-embedded segment in round 2, etc.
    // This has the side-effect of adding entityIdToSegment mappings, which we'll need to call
    // entityLoaded in the next part of this test.
    List<ContentKey> singleton = ImmutableList.of(nonExistentKey);
    assertEquals(ImmutableList.of(Hash.of("03")), fetch.entityIdsToFetch(0, 0, singleton));
    assertEquals(Collections.EMPTY_LIST, fetch.entityIdsToFetch(1, 0, singleton));
    assertEquals(ImmutableList.of(Hash.of("01")), fetch.entityIdsToFetch(2, 0, singleton));
    assertEquals(ImmutableList.of(Hash.of("02")), fetch.entityIdsToFetch(3, 0, singleton));

    Consumer<KeyListEntry> keyHits = mock(Consumer.class);
    fetch.entityLoaded(getKeyListEntity(6, 3, 2, KeyWithSpecificHash::withMaxIntHash));
    fetch.checkForKeys(0, singleton, keyHits);
    // Round 1 uses the embedded segment, no need to load it
    fetch.checkForKeys(1, singleton, keyHits);
    fetch.entityLoaded(getKeyListEntity(2, 1, 2, KeyWithSpecificHash::withMaxIntHash));
    fetch.checkForKeys(2, singleton, keyHits);
    fetch.entityLoaded(getKeyListEntity(4, 2, 2, KeyWithSpecificHash::withMaxIntHash));
    fetch.checkForKeys(3, singleton, keyHits);
    verifyNoInteractions(keyHits);
  }

  /**
   * Test {@link FetchValuesUsingOpenAddressing#segmentForKey(int, int)} on small hashtables.
   *
   * <p>The code in there interpreting return values of {@link java.util.Arrays#binarySearch(int[],
   * int)} has been correct since inception, but it can be a little tricky to think about the
   * various +1 and -1 offsets in play for nonexact matches. This just helped me prove that it was
   * correct to begin with, and should localize our search if we accidentally break it in the
   * future.
   *
   * <p>The edge case where {@code segmentSize} is {@code 1} should only happen if a user
   * misunderstands what the open-addressing-related config options actually do, or if they manage
   * to serialize abnormally huge {@code CommitLogEntry} instances individually exceeding the
   * segment size limit.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 4})
  public void segmentForKey(int segmentSize) {
    final int segmentCount = 4;

    FetchValuesUsingOpenAddressing fetch =
        new FetchValuesUsingOpenAddressing(
            getCommitFixture(segmentSize, segmentCount, KeyWithSpecificHash::withMaxIntHash));

    for (int bucketIndex = 0; bucketIndex < segmentSize * segmentCount; bucketIndex++) {
      assertEquals(bucketIndex / segmentSize, fetch.segmentForKey(bucketIndex, 0));
    }
  }

  /**
   * Exercise {@link FetchValuesUsingOpenAddressing#entityIdsToFetch(int, int, Collection)}
   * prefetching.
   *
   * <p>Users can configure the number of segments to prefetch ({@link
   * DatabaseAdapterConfig#getKeyListEntityPrefetch()}). An actual key-list could have fewer or more
   * segments than this configured int value. This test checks prefetch values starting from zero,
   * increasing towards the segment count, equalling it, and exceeding it.
   *
   * @see #negativeSegmentPrefetchThrowsException()
   */
  @Test
  public void segmentPrefetching() {
    final int segmentSize = 2;
    final int segmentCount = 4;

    final CommitLogEntry commitFixture =
        getCommitFixture(segmentSize, segmentCount, KeyWithSpecificHash::withMaxIntHash);
    final ContentKey nonExistentKey =
        new KeyWithSpecificHash(Integer.MAX_VALUE, ImmutableList.of("foo"));

    for (int prefetch = 0; prefetch < segmentCount * 2; prefetch++) {
      List<Hash> expectedHashes =
          Stream.of("03", null /* embedded segment */, "01", "02")
              .limit(prefetch + 1) // Exceeding the stream length on some iterations is fine
              .filter(Objects::nonNull)
              .map(Hash::of)
              .collect(Collectors.toList());

      // If this fails, it's likely a problem with this test's stream processing or assumptions,
      // rather than a problem with the system-under-test in the main tree.  Since our key hashes
      // to the highest-indexed segment, which is not the embedded segment, we can assume that this
      // collection always contains at least that segment's hash.
      Preconditions.checkState(0 < expectedHashes.size());

      FetchValuesUsingOpenAddressing fetch = new FetchValuesUsingOpenAddressing(commitFixture);
      assertThat(fetch.entityIdsToFetch(0, prefetch, ImmutableList.of(nonExistentKey)))
          .as("prefetch=" + prefetch)
          .containsExactlyInAnyOrderElementsOf(expectedHashes);
    }
  }

  /**
   * Validates key lookup in case of hash collision. Only two keys collide in each test case. The
   * {@code natural} position refers to the key that sits in the spot designated by its hash code.
   * The {@code moved} position refers to the key that sits in a spot that does not match its hash
   * code. This simulates key collision at key list construction time. The "moved" key may be moved
   * ahead or behind its natural position. The latter case happens when the search for a free spot
   * wraps around the end of the key list.
   *
   * @param movedKeyFoundInRound the lookup round when the "moved" key is expected to be found.
   */
  @ParameterizedTest
  @CsvSource({
    // Jump ahead collision in the first segment.
    "0, 2, 0, false",
    "0, 3, 0, false",
    "1, 2, 0, false",
    "1, 3, 0, false",
    // Jump ahead collision from the first segment to the last.
    "0, 15, 3, false",
    "1, 14, 3, false",
    "2, 13, 3, false",
    "3, 12, 3, false",
    // Wraparound from the first segment to a small index in the same segment.
    // Note: the first segment is scanned twice
    "2, 0, 4, false",
    "2, 1, 4, false",
    "3, 2, 4, false",
    // Wraparound from the second segment to and earlier index in the same segment.
    // Note: the second segment is scanned twice
    "5, 4, 4, false",
    "7, 5, 4, false",
    // Wraparound from the last segment to the second.
    "15, 6, 2, false",
    // Wraparound from the last segment to the first.
    "15, 0, 1, false",
    "14, 3, 1, false",
    // Wraparound from the first segment to a small index in the same segment without embedded.
    // One extra round is required to skip over the empty embedded segment.
    "2, 0, 5, true",
    "2, 1, 5, true",
    "3, 2, 5, true",
    // Wraparound from the last segment to the second without embedded.
    // One extra round is required to skip over the empty embedded segment.
    "15, 6, 3, true",
    // Wraparound from the last segment to the second without embedded.
    // One extra round is required to skip over the empty embedded segment.
    "15, 0, 2, true",
    "14, 3, 2, true",
    // Jump ahead collision in the same segment without embedded.
    "0, 3, 0, true",
    "1, 2, 0, true",
    "4, 5, 0, true",
    "12, 15, 0, true",
    // Jump ahead collision from the first segment to the last without embedded
    "0, 15, 3, true",
    "1, 14, 3, true",
  })
  public void keyCollision(
      int naturalPos, int movedPos, int movedKeyFoundInRound, boolean skipEmbedded) {
    Function<Integer, ContentKey> bucketToKey =
        bucket -> {
          // Simulate key collision by artificially crafting their hash codes instead of bothering
          // with real insertion.
          String name = "ordinary-" + bucket; // no hash collision
          int hash = bucket;
          if (bucket == movedPos) {
            name = "moved-" + bucket;
            // hash collides with `naturalPos` and this key is stored at `movedPos`
            hash = naturalPos;
          }
          if (bucket == naturalPos) {
            // hash collision, the key keeps its natural spot (the name helps debugging)
            name = "natural-" + bucket;
          }
          return new KeyWithSpecificHash(hash, ImmutableList.of(name));
        };

    // Note: test parameters depend on these two values
    int segmentCount = 4;
    int segmentSize = 4;

    if (skipEmbedded) {
      segmentCount++; // the embedded segment moves into the first external segment
    }

    CommitLogEntry entry =
        getCommitFixture(skipEmbedded ? 0 : segmentSize, segmentSize, segmentCount, bucketToKey);

    // Validate lookup for all 16 keys
    for (int b = 0; b < 16; b++) {
      ContentKey key = bucketToKey.apply(b);
      // Reset FetchValuesUsingOpenAddressing for each key for ease of validation
      FetchValuesUsingOpenAddressing fetch = new FetchValuesUsingOpenAddressing(entry);
      AtomicReference<KeyListEntry> hit = new AtomicReference<>();
      Collection<ContentKey> remaining = ImmutableList.of(key);
      // Simulate loading key lists in multiple rounds
      for (int r = 0; r < segmentCount + 1; r++) {
        // Note: Do not preload segments for each of validation
        List<Hash> hashes = fetch.entityIdsToFetch(r, 0, ImmutableList.of(key));
        hashes.forEach(
            h -> {
              int segment = Integer.parseInt(h.asString(), 16);
              int startingBucket = (skipEmbedded ? 0 : segmentSize) + ((segment - 1) * segmentSize);
              KeyListEntity keyListEntity =
                  getKeyListEntity(startingBucket, segment, segmentSize, bucketToKey);
              fetch.entityLoaded(keyListEntity);
            });

        remaining = fetch.checkForKeys(r, remaining, hit::set);
        if (hit.get() != null) {
          if (b == movedPos) { // The moved key requires loading extra segments
            assertThat(r)
                .describedAs("Expected round for key " + key)
                .isEqualTo(movedKeyFoundInRound);
          } else { // Ordinary keys are found on the first attempt
            assertThat(r).describedAs("Expected round for key " + key).isEqualTo(0);
          }
          break;
        }
      }

      // Make sure each key is found
      assertThat(hit.get())
          .describedAs("Key " + key)
          .isNotNull()
          .extracting(KeyListEntry::getKey)
          .isEqualTo(key);
    }
  }

  /**
   * Check {@link FetchValuesUsingOpenAddressing#entityIdsToFetch(int, int, Collection)} for an
   * informative exception when prefetch is negative.
   *
   * <p>Users can configure the number of segments to prefetch ({@link
   * DatabaseAdapterConfig#getKeyListEntityPrefetch()}), and negative values probably indicate a
   * misunderstanding.
   */
  @Test
  public void negativeSegmentPrefetchThrowsException() {
    final int invalidPrefetch = -1;
    final String expectedMessage =
        String.format("Key-list segment prefetch parameter %s cannot be negative", invalidPrefetch);

    FetchValuesUsingOpenAddressing fetch =
        new FetchValuesUsingOpenAddressing(
            getCommitFixture(2, 4, KeyWithSpecificHash::withMaxIntHash));
    assertThatThrownBy(
            () ->
                fetch.entityIdsToFetch(
                    0,
                    invalidPrefetch,
                    ImmutableList.of(
                        new KeyWithSpecificHash(Integer.MAX_VALUE, ImmutableList.of("foo")))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  private CommitLogEntry getCommitFixture(
      final int segmentSize, final int segmentCount, Function<Integer, ContentKey> bucketToKey) {
    return getCommitFixture(segmentSize, segmentSize, segmentCount, bucketToKey);
  }

  /**
   * Build a commit with fake segments of uniform size.
   *
   * <p>The keys in this commit's embedded key-list have a custom hashCode() that returns
   * {@linkplain Integer#MAX_VALUE}, causing lookups against them to start in the final segment at
   * round zero.
   *
   * @param embeddedSize size in units of individual entries (not bytes) of the embedded segment
   * @param segmentSize size in units of individual entries (not bytes)
   * @param segmentCount the number of segments, including the embedded segment
   */
  private CommitLogEntry getCommitFixture(
      final int embeddedSize,
      final int segmentSize,
      final int segmentCount,
      Function<Integer, ContentKey> bucketToKey) {
    // Create segments of uniform size.  This list has one fewer elements than segmentCount,
    // because the initial segment corresponding to the embedded-key-list doesn't have its
    // offset recorded (somewhere on the interval [0, second-segment-offset)).
    List<Integer> segmentOffsets = new ArrayList<>(segmentCount);
    List<Hash> keyListIds = new ArrayList<>(segmentCount);
    for (int i = 1, offset = embeddedSize; i < segmentCount; i++, offset += segmentSize) {
      segmentOffsets.add(offset);
      String hexString = intToPaddedHexString(i);
      keyListIds.add(Hash.of(hexString));
    }

    // Build some keys for the embedded-key-list stored with the commit
    ImmutableKeyList.Builder embeddedKeyListBuilder = ImmutableKeyList.builder();
    for (int i = 0; i < embeddedSize; i++) {
      ImmutableKeyListEntry entry = getKeyListEntry(bucketToKey.apply(i));
      embeddedKeyListBuilder.addKeys(entry);
    }
    ImmutableKeyList embeddedKeyList = embeddedKeyListBuilder.build();

    // All of the constants below are completely arbitrary, no special significance
    return ImmutableCommitLogEntry.builder()
        .addAllKeyListEntityOffsets(segmentOffsets)
        .createdTime(42L)
        .commitSeq(0L)
        .metadata(ByteString.EMPTY)
        .keyListDistance(20)
        .keyList(embeddedKeyList)
        .keyListBucketCount(embeddedSize + (segmentCount - 1) * segmentSize)
        .keyListsIds(keyListIds)
        .hash(Hash.of("c0ffee"))
        .build();
  }

  /**
   * Convert an int to a hex string of length 2, left padding with "0", if necessary.
   *
   * <p>This method only supports {@code 0 <= i <= 255}.
   */
  private static String intToPaddedHexString(int i) {
    Preconditions.checkArgument(0 <= i && i <= 255, i + " exceeds unsigned byte range");
    return String.format("%02x", i);
  }

  /** Builds a fake {@linkplain KeyListEntity}. */
  private static KeyListEntity getKeyListEntity(
      int startingBucket,
      int segmentIndex,
      int entryCount,
      Function<Integer, ContentKey> bucketToKey) {
    ImmutableKeyList.Builder listBuilder = ImmutableKeyList.builder();

    for (int i = 0, bucket = startingBucket; i < entryCount; i++, bucket++) {
      listBuilder.addKeys(getKeyListEntry(bucketToKey.apply(bucket)));
    }

    return ImmutableKeyListEntity.builder()
        .id(Hash.of(intToPaddedHexString(segmentIndex)))
        .keys(listBuilder.build())
        .build();
  }

  private static ImmutableKeyListEntry getKeyListEntry(ContentKey k) {
    return ImmutableKeyListEntry.builder()
        .key(k)
        .payload((byte) 99)
        .contentId(ImmutableContentId.of(k + "_id"))
        .build();
  }

  /** All instances return {@linkplain Integer#MAX_VALUE} from {@linkplain #hashCode()}. */
  private static class KeyWithSpecificHash extends ContentKey {

    private final int hashCode;
    final List<String> elements;

    public KeyWithSpecificHash(int hashCode, List<String> elements) {
      this.hashCode = hashCode;
      this.elements = ImmutableList.copyOf(elements);
    }

    private static ContentKey withMaxIntHash(int bucket) {
      return new KeyWithSpecificHash(Integer.MAX_VALUE, ImmutableList.of("" + bucket));
    }

    @Override
    public List<String> getElements() {
      return elements;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ContentKey)) {
        return false;
      }
      ContentKey that = (ContentKey) o;
      boolean equals = Objects.equals(elements, that.getElements());
      if (equals) {
        assertThat(hashCode)
            .describedAs("equals/hashCode mismatch between %s and %s", this, that)
            .isEqualTo(that.hashCode());
      }
      return equals;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return "" + hashCode + ":" + super.toString();
    }
  }
}
