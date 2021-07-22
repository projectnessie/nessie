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
package org.projectnessie.versioned.tiered.adapter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.ByteString;
import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;

/** Represents a commit-log-entry, reachable via named references. */
@Value.Immutable(lazyhash = true)
@JsonSerialize(as = ImmutableCommitLogEntry.class)
@JsonDeserialize(as = ImmutableCommitLogEntry.class)
public interface CommitLogEntry {
  /** Creation timestamp in microseconds since epoch. */
  long getCreatedTime();

  Hash getHash();

  /** Zero, one or more parent-entry hashes of this commit, nearest parent first. */
  List<Hash> getParents();

  ByteString getMetadata();

  List<KeyWithBytes> getPuts();

  List<Key> getUnchanged();

  List<Key> getDeletes();

  /**
   * List of all "reachable" or "known" {@link Key}s up to this commit-log-entry's <em>parent</em>
   * commit.
   *
   * <p>This key-list checkpoint, if present, does <em>not</em> contain the key-changes in this
   * entry in {@link #getPuts()} and {@link #getDeletes()}.
   */
  @Nullable
  EmbeddedKeyList getKeyList();

  int getKeyListDistance();

  static CommitLogEntry of(
      long createdTime,
      Hash hash,
      List<Hash> parents,
      ByteString metadata,
      List<KeyWithBytes> puts,
      List<Key> unchanged,
      List<Key> deletes,
      int keyListDistance,
      EmbeddedKeyList keyList) {
    return ImmutableCommitLogEntry.builder()
        .createdTime(createdTime)
        .hash(hash)
        .parents(parents)
        .metadata(metadata)
        .puts(puts)
        .unchanged(unchanged)
        .deletes(deletes)
        .keyListDistance(keyListDistance)
        .keyList(keyList)
        .build();
  }
}
