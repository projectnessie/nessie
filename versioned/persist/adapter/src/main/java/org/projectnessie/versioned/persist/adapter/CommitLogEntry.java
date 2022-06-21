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

import com.google.protobuf.ByteString;
import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;

/** Represents a commit-log-entry stored in the database. */
@Value.Immutable
public interface CommitLogEntry {
  /** Creation timestamp in microseconds since epoch. */
  long getCreatedTime();

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
  List<Key> getDeletes();

  /**
   * The list of all "reachable" or "known" {@link org.projectnessie.versioned.Key}s up to this
   * commit-log-entry's <em>parent</em> commit consists of all entries in this {@link
   * org.projectnessie.versioned.persist.adapter.KeyList} plus the {@link
   * org.projectnessie.versioned.persist.adapter.KeyListEntity}s via {@link #getKeyListsIds()}.
   *
   * <p>This key-list checkpoint, if present, does <em>not</em> contain the key-changes in this
   * entry in {@link #getPuts()} and {@link #getDeletes()}.
   */
  @Nullable
  KeyList getKeyList();

  /**
   * IDs of for the linked {@link org.projectnessie.versioned.persist.adapter.KeyListEntity} that,
   * together with {@link #getKeyList()} make the complete {@link org.projectnessie.versioned.Key}
   * for this commit.
   */
  List<Hash> getKeyListsIds();

  /** Number of commits since the last complete key-list. */
  int getKeyListDistance();

  List<Hash> getAdditionalParents();

  @Value.Default
  default KeyListVariant getKeyListVariant() {
    return KeyListVariant.EMBEDDED_AND_EXTERNAL_MRU;
  }

  static CommitLogEntry of(
      long createdTime,
      Hash hash,
      long commitSeq,
      Iterable<Hash> parents,
      ByteString metadata,
      Iterable<KeyWithBytes> puts,
      Iterable<Key> deletes,
      int keyListDistance,
      KeyList keyList,
      Iterable<Hash> keyListIds,
      Iterable<Hash> additionalParents) {
    return ImmutableCommitLogEntry.builder()
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
        .addAllAdditionalParents(additionalParents)
        .build();
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
     * The variant in which Nessie versions since 0.31.0 either write a <em>sorted</em> {@link
     * CommitLogEntry#getKeyList() embedded key-list} <em>or</em> multiple {@link KeyListEntity
     * key-list entities}, each representing a hash bucket.
     *
     * <p>If the total serialized size of all serialized {@link KeyListEntry} objects is up to
     * {@link DatabaseAdapterConfig#getMaxKeyListSize()}, the {@link CommitLogEntry#getKeyList()
     * embedded key-list} contains key-list-entries sorted by {@link KeyListEntry#getKey() content
     * key}.
     *
     * <p>Otherwise the embedded key-list will be empty. All keys will be persisted as "external"
     * {@link KeyListEntity key-list entities}. Each {@link KeyListEntity key-list entity}
     * represents a hash bucket. All {@link KeyListEntry}s in each bucket are sorted by {@link
     * KeyListEntry#getKey() content key}.
     */
    SORTED_AND_HASHED
  }
}
