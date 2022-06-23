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

import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

/**
 * Helper object for {@link AbstractDatabaseAdapter#buildKeyList(AutoCloseable, CommitLogEntry,
 * Consumer, Function)}.
 */
class KeyListBuildState {

  static final int MINIMUM_BUCKET_SIZE = 4096;
  private final ImmutableCommitLogEntry.Builder newCommitEntry;

  private final int maxEmbeddedKeyListSize;
  private final int maxKeyListEntitySize;
  private final ToIntFunction<KeyListEntry> serializedEntrySize;
  private final List<KeyListEntry> entries = new ArrayList<>();

  KeyListBuildState(
      ImmutableCommitLogEntry.Builder newCommitEntry,
      int maxEmbeddedKeyListSize,
      int maxKeyListEntitySize,
      ToIntFunction<KeyListEntry> serializedEntrySize) {
    this.newCommitEntry = newCommitEntry;
    this.maxEmbeddedKeyListSize = maxEmbeddedKeyListSize;
    this.maxKeyListEntitySize = maxKeyListEntitySize;
    this.serializedEntrySize = serializedEntrySize;
  }

  void add(KeyListEntry entry) {
    entries.add(entry);
  }

  int bucket(KeyListEntry entry, int bucketCount) {
    return Math.abs(entry.getKey().hashCode() % bucketCount);
  }

  int calcBucketCount(int totalSize, int maxBucketSize) {
    int bucketCount = totalSize / maxBucketSize;
    if (totalSize % maxBucketSize != 0) {
      bucketCount++;
    }
    // Distribution of the "final" bucket size is rather non-uniform, especially for very similar
    // keys. For example, adding 500 keys, each with one element representing the string
    // representation of the int range 0..500 results in a poor distribution: from buckets with 0
    // entries up to buckets with nearly 20 buckets, where the "ideal" number of entries is 8.
    //
    // In theory, using the next-power-of-2 _should_ mitigate the above problem, but sadly it does
    // not. And we might up persisting way more entities as necessary.
    return bucketCount;
  }

  List<KeyListEntity> finish() {
    int totalSize = entries.stream().mapToInt(serializedEntrySize).sum();

    if (totalSize <= maxEmbeddedKeyListSize) {
      ImmutableKeyList.Builder embeddedBuilder = ImmutableKeyList.builder();
      sortBucket(entries);
      embeddedBuilder.addAllKeys(entries);
      newCommitEntry.keyList(embeddedBuilder.build());
      return Collections.emptyList();
    }

    int maxBucketSize = Math.max(MINIMUM_BUCKET_SIZE, maxKeyListEntitySize);
    int bucketCount = calcBucketCount(totalSize, maxBucketSize);
    List<List<KeyListEntry>> buckets = new ArrayList<>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      buckets.add(new ArrayList<>());
    }

    for (KeyListEntry entry : entries) {
      int bucket = bucket(entry, bucketCount);
      buckets.get(bucket).add(entry);
    }

    IntFunction<List<KeyListEntry>> sortedBucket =
        idx -> {
          List<KeyListEntry> bucket = buckets.get(idx);
          bucket.sort(Comparator.comparing(KeyListEntry::getKey));
          return bucket;
        };

    List<KeyListEntity> newKeyListEntities = new ArrayList<>(bucketCount);

    for (int b = 0; b < buckets.size(); b++) {
      KeyList keyList = ImmutableKeyList.builder().addAllKeys(sortedBucket.apply(b)).build();
      Hash id = randomHash();
      newKeyListEntities.add(KeyListEntity.of(id, keyList));
      newCommitEntry.addKeyListsIds(id);
    }

    return newKeyListEntities;
  }

  private static void sortBucket(List<KeyListEntry> bucket) {
    bucket.sort(Comparator.comparing(KeyListEntry::getKey));
  }
}
