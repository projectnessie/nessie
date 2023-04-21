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
package org.projectnessie.versioned.persist.adapter;

import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Hashable;

/** Represents a commit-log-entry stored in the database. */
@Value.Immutable
public interface CommitLogEntry extends Hashable {
  /** Creation timestamp in microseconds since epoch. */
  long getCreatedTime();

  @Override
  Hash getHash();

  /**
   * Monotonically increasing counter representing the number of commits since the "beginning of
   * time.
   */
  long getCommitSeq();

  /** Zero, one or more parent-entry hashes of this commit, nearest parent first. */
  List<Hash> getParents();

  /** Serialized commit-metadata. */
  ByteString getMetadata();

  /**
   * List of all {@code Put} operations, with their keys, content-types and serialized {@code
   * Content}.
   */
  List<KeyWithBytes> getPuts();

  /** List of "unchanged" keys, from {@code Delete} commit operations. */
  List<ContentKey> getDeletes();

  /**
   * The list of all "reachable" or "known" {@link ContentKey}s up to this commit-log-entry's
   * <em>parent</em> commit consists of all entries in this {@link
   * org.projectnessie.versioned.persist.adapter.KeyList} plus the {@link
   * org.projectnessie.versioned.persist.adapter.KeyListEntity}s via {@link #getKeyListsIds()}.
   *
   * <p>This key-list checkpoint, if present, does <em>not</em> contain the key-changes in this
   * entry in {@link #getPuts()} and {@link #getDeletes()}.
   */
  @Nullable
  @jakarta.annotation.Nullable
  KeyList getKeyList();

  /**
   * IDs of for the linked {@link org.projectnessie.versioned.persist.adapter.KeyListEntity} that,
   * together with {@link #getKeyList()} make the complete {@link ContentKey} for this commit.
   */
  List<Hash> getKeyListsIds();

  /** The indices of the first occupied bucket in each non-embedded segment of the key list. */
  @Nullable
  @jakarta.annotation.Nullable
  List<Integer> getKeyListEntityOffsets();

  /**
   * Load factor for key lists hashed using open-addressing.
   *
   * <p>This is a user-configured value, typically on {@code (0, 1)}. In practice, key lists may be
   * hashed at a lower effective load factor than configured here (e.g. for alignment), but
   * generally not a higher one.
   */
  @Nullable
  @jakarta.annotation.Nullable
  Float getKeyListLoadFactor();

  /**
   * The cumulative total of buckets across all segments.
   *
   * <p>This is generally much greater than the segment count. Addressable buckets have indices on
   * the interval {@code [0, this value)}.
   */
  @Nullable
  @jakarta.annotation.Nullable
  Integer getKeyListBucketCount();

  /** Number of commits since the last complete key-list. */
  int getKeyListDistance();

  List<Hash> getAdditionalParents();

  @Value.Default
  default KeyListVariant getKeyListVariant() {
    return KeyListVariant.EMBEDDED_AND_EXTERNAL_MRU;
  }

  default boolean hasKeySummary() {
    return getKeyListVariant() != KeyListVariant.EMBEDDED_AND_EXTERNAL_MRU || getKeyList() != null;
  }

  static CommitLogEntry of(
      long createdTime,
      Hash hash,
      long commitSeq,
      Iterable<Hash> parents,
      ByteString metadata,
      Iterable<KeyWithBytes> puts,
      Iterable<ContentKey> deletes,
      int keyListDistance,
      KeyList keyList,
      Iterable<Hash> keyListIds,
      Iterable<Integer> keyListEntityOffsets,
      Iterable<Hash> additionalParents) {
    ImmutableCommitLogEntry.Builder c =
        ImmutableCommitLogEntry.builder()
            .createdTime(createdTime)
            .hash(hash)
            .commitSeq(commitSeq)
            .parents(parents)
            .metadata(metadata)
            .puts(puts)
            .deletes(deletes)
            .keyListDistance(keyListDistance)
            .keyList(keyList)
            .addAllKeyListsIds(keyListIds)
            .addAllAdditionalParents(additionalParents);
    if (keyListEntityOffsets != null) {
      c.addAllKeyListEntityOffsets(keyListEntityOffsets);
    }
    return c.build();
  }

  enum KeyListVariant {
    /**
     * The variant in which Nessie versions 0.10.0..0.30.0 write {@link CommitLogEntry#getKeyList()
     * embedded key-list} and {@link KeyListEntity key-list entities}.
     *
     * <p>Most recently updated {@link KeyListEntry}s appear first.
     *
     * <p>The {@link CommitLogEntry#getKeyList() embedded key-list} is filled first up to {@link
     * DatabaseAdapterConfig#getMaxKeyListSize()}, more {@link KeyListEntry}s are written to
     * "external"{@link KeyListEntity key-list entities} with their IDs in {@link
     * CommitLogEntry#getKeyListsIds()}.
     */
    EMBEDDED_AND_EXTERNAL_MRU,
    /**
     * The variant in which Nessie versions since 0.31.0 maintains {@link
     * CommitLogEntry#getKeyList() embedded key-list} and {@link KeyListEntity key-list entities}.
     *
     * <p>{@link KeyListEntry}s are maintained as an open-addressing hash map with {@link
     * ContentKey} as the map key.
     *
     * <p>That open-addressing hash map is split into multiple segments, if necessary.
     *
     * <p>The first segment is represented by the {@link CommitLogEntry#getKeyList() embedded
     * key-list} with a serialized size goal up to {@link
     * DatabaseAdapterConfig#getMaxKeyListSize()}. All following segments have a serialized size up
     * to {@link DatabaseAdapterConfig#getMaxKeyListEntitySize()} as the goal.
     *
     * <p>Maximum size constraints are fulfilled using a best-effort approach.
     */
    OPEN_ADDRESSING
  }
}
