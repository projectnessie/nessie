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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

final class FetchValuesUsingOpenAddressing {

  final int[] keyListSegmentOffsets;
  final int keyListCount;
  final KeyListEntry[][] keyListsArray;
  final Object2IntHashMap<Hash> entityIdToSegment;
  final int hashMask;
  final List<Hash> keyListIds;

  FetchValuesUsingOpenAddressing(CommitLogEntry entry) {
    List<Integer> keyListSegmentOffsetsList = entry.getKeyListEntityOffsets();
    keyListSegmentOffsets =
        keyListSegmentOffsetsList != null
            ? keyListSegmentOffsetsList.stream().mapToInt(Integer::intValue).toArray()
            : new int[0];
    keyListCount = 1 + keyListSegmentOffsets.length;

    keyListsArray = new KeyListEntry[keyListCount][];
    keyListsArray[0] = entry.getKeyList().getKeys().toArray(new KeyListEntry[0]);

    entityIdToSegment = new Object2IntHashMap<>(keyListCount, Hashing.DEFAULT_LOAD_FACTOR, -1);

    hashMask = entry.getKeyListBucketCount() - 1;

    keyListIds = entry.getKeyListsIds();
  }

  /** Calculates the open-addressing bucket for a key. */
  int bucketForKey(Key key) {
    return key.hashCode() & hashMask;
  }

  /**
   * Identifies the segment for a bucket. Segment 0 is the embedded key list, segment 1 is the first
   * key-list-entity.
   */
  int segmentForKey(int bucket, int round) {
    int binIdx = Arrays.binarySearch(keyListSegmentOffsets, bucket);
    int segment = binIdx >= 0 ? binIdx + 1 : -binIdx - 1;
    return (segment + round) & hashMask;
  }

  /**
   * Identify the {@link KeyListEntity}s that need to be fetched to get the {@link KeyListEntry}s
   * for {@code remainingKeys}.
   */
  List<Hash> entityIdsToFetch(int round, int prefetchEntities, Collection<Key> remainingKeys) {
    // Identify the key-list segments to fetch
    List<Hash> entitiesToFetch = new ArrayList<>();
    for (Key key : remainingKeys) {
      int keyBucket = bucketForKey(key);
      int segment = segmentForKey(keyBucket, round);

      for (int prefetch = 0; ; prefetch++) {
        if (segment > 0) {
          int entitySegment = segment - 1;
          Hash entityId = keyListIds.get(entitySegment);
          if (entityIdToSegment.put(entityId, segment) == -1) {
            entitiesToFetch.add(entityId);
          }
        }
        if (prefetch >= prefetchEntities) {
          break;
        }
        segment++;
        if (segment == keyListCount) {
          segment = 0;
        }
      }
    }
    return entitiesToFetch;
  }

  /**
   * Callback to memoize a loaded {@link KeyListEntity} so it can be used in {@link
   * #checkForKeys(int, Collection, Consumer)}.
   */
  void entityLoaded(KeyListEntity keyListEntity) {
    Integer index = entityIdToSegment.get(keyListEntity.getId());
    keyListsArray[index] = keyListEntity.getKeys().getKeys().toArray(new KeyListEntry[0]);
  }

  /**
   * Find the {@link KeyListEntry}s for the {@code remainingKeys}, return the {@link Key}s that
   * could not be found yet, but are likely in the next segment.
   */
  Collection<Key> checkForKeys(
      int round, Collection<Key> remainingKeys, Consumer<KeyListEntry> resultConsumer) {
    List<Key> keysForNextRound = new ArrayList<>();
    for (Key key : remainingKeys) {
      int keyBucket = bucketForKey(key);
      int segment = segmentForKey(keyBucket, round);
      int offsetInSegment = 0;
      if (round == 0) {
        offsetInSegment = keyBucket;
        if (segment > 0) {
          offsetInSegment -= keyListSegmentOffsets[segment - 1];
        }
      }

      KeyListEntry[] keys = keyListsArray[segment];
      for (int i = offsetInSegment; ; i++) {
        KeyListEntry keyListEntry = keys[i];
        if (keyListEntry == null) {
          // key _not_ found
          break;
        } else if (keyListEntry.getKey().equals(key)) {
          resultConsumer.accept(keyListEntry);
          break;
        } else if (i == keys.length - 1) {
          keysForNextRound.add(key);
          break;
        }
      }
    }
    return keysForNextRound;
  }
}
