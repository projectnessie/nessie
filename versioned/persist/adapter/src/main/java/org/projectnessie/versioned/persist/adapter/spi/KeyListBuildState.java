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
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;

/**
 * Helper object for {@link AbstractDatabaseAdapter#buildKeyList(AutoCloseable, CommitLogEntry,
 * Consumer, Function)}.
 */
class KeyListBuildState {

  private final ImmutableCommitLogEntry.Builder newCommitEntry;

  private final int maxEmbeddedKeyListSize;
  private final int maxKeyListEntitySize;
  private final ToIntFunction<KeyListEntry> serializedEntrySize;

  /** Builder for {@link CommitLogEntry#getKeyList()}. */
  private ImmutableKeyList.Builder embeddedBuilder = ImmutableKeyList.builder();
  /** Builder for {@link KeyListEntity}. */
  private ImmutableKeyList.Builder currentKeyList;
  /** Already built {@link KeyListEntity}s. */
  private List<KeyListEntity> newKeyListEntities = new ArrayList<>();

  /** Flag whether {@link CommitLogEntry#getKeyList()} is being filled. */
  private boolean embedded = true;

  /** Current size of either the {@link CommitLogEntry} or current {@link KeyListEntity}. */
  private int currentSize;

  KeyListBuildState(
      int initialSize,
      ImmutableCommitLogEntry.Builder newCommitEntry,
      int maxEmbeddedKeyListSize,
      int maxKeyListEntitySize,
      ToIntFunction<KeyListEntry> serializedEntrySize) {
    this.currentSize = initialSize;
    this.newCommitEntry = newCommitEntry;
    this.maxEmbeddedKeyListSize = maxEmbeddedKeyListSize;
    this.maxKeyListEntitySize = maxKeyListEntitySize;
    this.serializedEntrySize = serializedEntrySize;
  }

  void add(KeyListEntry keyListEntry) {
    int entrySize = serializedEntrySize.applyAsInt(keyListEntry);
    if (embedded) {
      // filling the embedded key-list in CommitLogEntry

      if (currentSize + entrySize < maxEmbeddedKeyListSize) {
        // CommitLogEntry.keyList still has room
        addToEmbedded(keyListEntry, entrySize);
      } else {
        // CommitLogEntry.keyList is "full", switch to the first KeyListEntity
        embedded = false;
        newKeyListEntity();
        addToKeyListEntity(keyListEntry, entrySize);
      }
    } else {
      // filling linked key-lists via CommitLogEntry.keyListIds

      if (currentSize + entrySize > maxKeyListEntitySize) {
        // current KeyListEntity is "full", switch to a new one
        finishKeyListEntity();
        newKeyListEntity();
      }
      addToKeyListEntity(keyListEntry, entrySize);
    }
  }

  private void finishKeyListEntity() {
    Hash id = randomHash();
    newKeyListEntities.add(KeyListEntity.of(id, currentKeyList.build()));
    newCommitEntry.addKeyListsIds(id);
  }

  private void newKeyListEntity() {
    currentSize = 0;
    currentKeyList = ImmutableKeyList.builder();
  }

  private void addToKeyListEntity(KeyListEntry keyListEntry, int keyTypeSize) {
    currentSize += keyTypeSize;
    currentKeyList.addKeys(keyListEntry);
  }

  private void addToEmbedded(KeyListEntry keyListEntry, int keyTypeSize) {
    currentSize += keyTypeSize;
    embeddedBuilder.addKeys(keyListEntry);
  }

  void finish() {
    // If there's an "unfinished" KeyListEntity, build it.
    if (currentKeyList != null) {
      finishKeyListEntity();
    }
    newCommitEntry.keyList(embeddedBuilder.build());
  }

  List<KeyListEntity> buildNewKeyListEntities() {
    return newKeyListEntities;
  }
}
