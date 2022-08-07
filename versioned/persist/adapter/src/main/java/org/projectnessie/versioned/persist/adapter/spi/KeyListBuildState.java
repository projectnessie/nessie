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

import static java.lang.Integer.numberOfLeadingZeros;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

/**
 * Compute the {@link CommitLogEntry#getKeyList() embedded key-list} and {@link KeyListEntity}s,
 * accessible via {@link CommitLogEntry#getKeyListsIds()}.
 *
 * <p>Commits with {@link CommitLogEntry.KeyListVariant#OPEN_ADDRESSING} are represented as an
 * open-addressing hash map with {@link org.projectnessie.versioned.Key} as the map key. "Bucket"
 * refers to a cell in this hash map. Collisions are resolved with linear probing.
 *
 * <p>That open-addressing hash map is split into multiple segments, if necessary.
 *
 * <p>The first segment is represented by the {@link CommitLogEntry#getKeyList() embedded key-list}
 * with a serialized size goal up to {@link DatabaseAdapterConfig#getMaxKeyListSize()}. All
 * following segments have a serialized size up to {@link
 * DatabaseAdapterConfig#getMaxKeyListEntitySize()} as the goal.
 *
 * <p>Maximum size constraints are fulfilled using a best-effort approach.
 *
 * <p>The first segment represented by the embedded key list is always present, even if it contains
 * no elements. Zero or more non-embedded segments follow this first segment. The total number of
 * segments is therefore the number of non-embedded segments plus one.
 *
 * <p>In general, the number of buckets should be significantly greater than the number of segments.
 *
 * <p>Used by {@link AbstractDatabaseAdapter#buildKeyList(AutoCloseable, CommitLogEntry, Consumer,
 * Function)}.
 */
class KeyListBuildState {

  static final int MINIMUM_BUCKET_SIZE = 4096;
  private final ImmutableCommitLogEntry.Builder newCommitEntry;

  private final int maxEmbeddedKeyListSize;
  private final int maxKeyListEntitySize;
  private final float loadFactor;
  private final ToIntFunction<KeyListEntry> serializedEntrySize;
  private final List<KeyListEntry> entries = new ArrayList<>();

  KeyListBuildState(
      ImmutableCommitLogEntry.Builder newCommitEntry,
      int maxEmbeddedKeyListSize,
      int maxKeyListEntitySize,
      float loadFactor,
      ToIntFunction<KeyListEntry> serializedEntrySize) {
    this.newCommitEntry = newCommitEntry;
    this.maxEmbeddedKeyListSize = maxEmbeddedKeyListSize;
    this.maxKeyListEntitySize = maxKeyListEntitySize;
    this.loadFactor = loadFactor;
    this.serializedEntrySize = serializedEntrySize;
  }

  void add(KeyListEntry entry) {
    entries.add(entry);
  }

  static int nextPowerOfTwo(final int v) {
    return 1 << (32 - numberOfLeadingZeros(v - 1));
  }

  List<KeyListEntity> finish() {
    // Build open-addressing map
    int openAddressingBuckets = openAddressingBucketCount();
    int openAddressingMask = openAddressingBuckets - 1;
    KeyListEntry[] openAddressingHashMap = new KeyListEntry[openAddressingBuckets];
    for (KeyListEntry entry : entries) {
      int hash = entry.getKey().hashCode();
      int bucket = hash & openAddressingMask;

      // add to map
      for (int i = bucket; ; ) {
        if (openAddressingHashMap[i] == null) {
          openAddressingHashMap[i] = entry;
          break;
        }

        i++;
        if (i == openAddressingBuckets) {
          i = 0;
        }
      }
    }

    // Generate all KeyList objects. The first one for the embedded key-list and all other ones
    // for key-list-entities. Offsets are kept for key-list-entities, because the embedded
    // key-list's is always the first one.

    List<Integer> offsets = new ArrayList<>();
    List<KeyList> keyLists = new ArrayList<>();

    // The units for both of these segment sizes are bytes
    int segmentSize = 0;
    int maxSegmentSize = maxEmbeddedKeyListSize;
    ImmutableKeyList.Builder keyListBuilder = ImmutableKeyList.builder();
    for (int i = 0; i < openAddressingHashMap.length; i++) {
      KeyListEntry entry = openAddressingHashMap[i];

      if (entry != null) {
        int entrySize = serializedEntrySize.applyAsInt(entry);

        if (segmentSize + entrySize > maxSegmentSize) {
          maxSegmentSize = maxKeyListEntitySize;
          offsets.add(i);
          keyLists.add(keyListBuilder.build());
          keyListBuilder = ImmutableKeyList.builder();
          segmentSize = 0;
        }

        segmentSize += entrySize;
      }
      keyListBuilder.addKeys(entry);
    }

    if (segmentSize > 0) {
      // Only add the last key-list-entity if it actually contains entries (not all null)
      keyLists.add(keyListBuilder.build());
    }

    // Very rare, but it's possible that there are no keys at all.
    if (!keyLists.isEmpty()) {
      newCommitEntry.keyList(keyLists.get(0));
    } else {
      newCommitEntry.keyList(KeyList.EMPTY);
    }
    newCommitEntry.keyListLoadFactor(loadFactor);
    newCommitEntry.keyListBucketCount(openAddressingBuckets);

    List<KeyListEntity> builtEntities =
        keyLists.stream()
            .skip(1)
            .map(keyList -> KeyListEntity.of(randomHash(), keyList))
            .collect(Collectors.toList());

    if (!builtEntities.isEmpty()) {
      builtEntities.stream().map(KeyListEntity::getId).forEach(newCommitEntry::addKeyListsIds);
      newCommitEntry.addAllKeyListEntityOffsets(offsets);
    }

    return builtEntities;
  }

  @VisibleForTesting
  int openAddressingBucketCount() {
    return nextPowerOfTwo((int) (entries.size() / loadFactor));
  }
}
