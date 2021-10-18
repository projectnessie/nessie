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

  /** Zero, one or more parent-entry hashes of this commit, nearest parent first. */
  List<Hash> getParents();

  /** Serialized commit-metadata. */
  ByteString getMetadata();

  /**
   * List of all {@code Put} operations, with their keys, content-types and serialized {@code
   * Contents}.
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

  static CommitLogEntry of(
      long createdTime,
      Hash hash,
      List<Hash> parents,
      ByteString metadata,
      List<KeyWithBytes> puts,
      List<Key> deletes,
      int keyListDistance,
      KeyList keyList,
      List<Hash> keyListIds) {
    return ImmutableCommitLogEntry.builder()
        .createdTime(createdTime)
        .hash(hash)
        .parents(parents)
        .metadata(metadata)
        .puts(puts)
        .deletes(deletes)
        .keyListDistance(keyListDistance)
        .keyList(keyList)
        .addAllKeyListsIds(keyListIds)
        .build();
  }
}
