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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
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
        new FetchValuesUsingOpenAddressing(getCommitFixture(2, 4));

    // Make a key that won't match anything in the commit
    Key nonExistentKey = new KeyWithMaxIntHash(ImmutableList.of("foo"));

    // Look for a non-existent key.
    // The fake segments here are all full, so this will probe through the entire hashtable.
    // Expect the final segment in round 0, then no segments (implying embedded) in round 1,
    // the first non-embedded segment in round 2, etc.
    // This has the side-effect of adding entityIdToSegment mappings, which we'll need to call
    // entityLoaded in the next part of this test.
    List<Key> singleton = ImmutableList.of(nonExistentKey);
    assertEquals(ImmutableList.of(Hash.of("03")), fetch.entityIdsToFetch(0, 0, singleton));
    assertEquals(Collections.EMPTY_LIST, fetch.entityIdsToFetch(1, 0, singleton));
    assertEquals(ImmutableList.of(Hash.of("01")), fetch.entityIdsToFetch(2, 0, singleton));
    assertEquals(ImmutableList.of(Hash.of("02")), fetch.entityIdsToFetch(3, 0, singleton));

    Consumer<KeyListEntry> keyHits = mock(Consumer.class);
    fetch.entityLoaded(getKeyListEntity(3, 2));
    fetch.checkForKeys(0, singleton, keyHits);
    // Round 1 uses the embedded segment, no need to load it
    fetch.checkForKeys(1, singleton, keyHits);
    fetch.entityLoaded(getKeyListEntity(1, 2));
    fetch.checkForKeys(2, singleton, keyHits);
    fetch.entityLoaded(getKeyListEntity(2, 2));
    fetch.checkForKeys(3, singleton, keyHits);
    verifyNoInteractions(keyHits);
  }

  /** Test finding extant keys pushed out of the final (highest-indexed) segment by collisions. */
  @Test
  public void segmentWrapAroundWithPresentKey() {
    // Create four segments with two buckets each
    // The hashes of the non-embedded/external segments will be 01, 02, and 03 (as hex strings)
    FetchValuesUsingOpenAddressing fetch =
        new FetchValuesUsingOpenAddressing(getCommitFixture(2, 4));

    // This element is in the zeroth bucket and the zeroth/embedded segment, but its hash would have
    // naturally placed it in the final segment.  This simulates a hash collision in the final
    // segment resolved by a linear probe that wrapped into the embedded segment.
    ImmutableKeyListEntry zerothEntry = getEmbeddedEntry(0);

    // Start searching at round zero, expecting to touch the final segment
    List<Key> zerothKeySingleton = ImmutableList.of(zerothEntry.getKey());
    assertEquals(ImmutableList.of(Hash.of("03")), fetch.entityIdsToFetch(0, 0, zerothKeySingleton));
    fetch.entityLoaded(getKeyListEntity(3, 2));

    // Checking for keys at round zero should miss, but it should return our key
    Consumer<KeyListEntry> keyHits = mock(Consumer.class);
    assertEquals(
        zerothKeySingleton, fetch.checkForKeys(0, ImmutableList.of(zerothEntry.getKey()), keyHits));
    verifyNoInteractions(keyHits);

    // Continue searching at round one, expecting to wrap into the embedded segment
    keyHits = mock(Consumer.class);
    // entityIdsToFetch should think we want the embedded segment, and so return an empty list
    assertEquals(Collections.EMPTY_LIST, fetch.entityIdsToFetch(1, 0, zerothKeySingleton));
    // checkForKeys should find a hit for our one and only search key, and so return no leftovers
    assertEquals(
        Collections.EMPTY_LIST,
        fetch.checkForKeys(1, ImmutableList.of(zerothEntry.getKey()), keyHits));
    // confirm the key hit
    verify(keyHits).accept(eq(zerothEntry));
    verifyNoMoreInteractions(keyHits);
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
        new FetchValuesUsingOpenAddressing(getCommitFixture(segmentSize, segmentCount));

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

    final CommitLogEntry commitFixture = getCommitFixture(segmentSize, segmentCount);
    final Key nonExistentKey = new KeyWithMaxIntHash(ImmutableList.of("foo"));

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
        new FetchValuesUsingOpenAddressing(getCommitFixture(2, 4));
    assertThatThrownBy(
            () ->
                fetch.entityIdsToFetch(
                    0,
                    invalidPrefetch,
                    ImmutableList.of(new KeyWithMaxIntHash(ImmutableList.of("foo")))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  /**
   * Build a commit with fake segments of uniform size.
   *
   * <p>The keys in this commit's embedded key-list have a custom hashCode() that returns
   * {@linkplain Integer#MAX_VALUE}, causing lookups against them to start in the final segment at
   * round zero.
   *
   * @param segmentSize size in units of individual entries (not bytes)
   * @param segmentCount the number of segments, including the embedded segment
   */
  private CommitLogEntry getCommitFixture(final int segmentSize, final int segmentCount) {
    // Create segments of uniform size.  This list has one fewer elements than segmentCount,
    // because the initial segment corresponding to the embedded-key-list doesn't have its
    // offset recorded (somewhere on the interval [0, second-segment-offset)).
    List<Integer> segmentOffsets = new ArrayList<>(segmentCount / segmentSize);
    List<Hash> keyListIds = new ArrayList<>(segmentCount / segmentSize);
    for (int i = 1; i < segmentCount; i++) {
      segmentOffsets.add(i * segmentSize);
      String hexString = intToPaddedHexString(i);
      keyListIds.add(Hash.of(hexString));
    }

    // Build some keys for the embedded-key-list stored with the commit
    ImmutableKeyList.Builder embeddedKeyListBuilder = ImmutableKeyList.builder();
    for (int i = 0; i < segmentSize; i++) {
      ImmutableKeyListEntry entry = getEmbeddedEntry(i);
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
        .keyListBucketCount(segmentCount * segmentSize)
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

  /**
   * Builds a fake {@linkplain KeyListEntity}.
   *
   * <p>All keys appearing in this entity have a custom hashCode() that returns {@linkplain
   * Integer#MAX_VALUE}.
   */
  private static KeyListEntity getKeyListEntity(int segmentIndex, int entryCount) {
    String prefix = "seg" + intToPaddedHexString(segmentIndex) + "_";

    ImmutableKeyList.Builder listBuilder = ImmutableKeyList.builder();

    for (int i = 0; i < entryCount; i++) {
      String keyString = prefix + i;
      listBuilder.addKeys(getKeyListEntry(keyString));
    }

    return ImmutableKeyListEntity.builder()
        .id(Hash.of(intToPaddedHexString(segmentIndex)))
        .keys(listBuilder.build())
        .build();
  }

  private static ImmutableKeyListEntry getEmbeddedEntry(int bucketIndex) {
    return getKeyListEntry("embedded_" + bucketIndex);
  }

  private static ImmutableKeyListEntry getKeyListEntry(String keyString) {
    Key k = new KeyWithMaxIntHash(ImmutableList.of(keyString));
    return ImmutableKeyListEntry.builder()
        .key(k)
        .payload((byte) 99)
        .contentId(ImmutableContentId.of(keyString + "_id"))
        .build();
  }

  /** All instances return {@linkplain Integer#MAX_VALUE} from {@linkplain #hashCode()}. */
  private static class KeyWithMaxIntHash extends Key {

    final List<String> elements;

    public KeyWithMaxIntHash(List<String> elements) {
      this.elements = ImmutableList.copyOf(elements);
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
      if (!(o instanceof Key)) {
        return false;
      }
      Key that = (Key) o;
      return Objects.equals(elements, that.getElements());
    }

    @Override
    public int hashCode() {
      return Integer.MAX_VALUE;
    }
  }
}
