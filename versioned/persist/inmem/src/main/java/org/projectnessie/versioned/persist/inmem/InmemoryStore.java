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
package org.projectnessie.versioned.persist.inmem;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;

public class InmemoryStore implements DatabaseConnectionProvider<InmemoryConfig> {

  final ConcurrentMap<ByteString, AtomicReference<RepoDescription>> repoDesc =
      new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, AtomicReference<GlobalStatePointer>> globalStatePointer =
      new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> globalStateLog = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> commitLog = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> keyLists = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> refLog = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> refHeads = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> refNames = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> refLogHeads = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> attachments = new ConcurrentHashMap<>();
  final ConcurrentMap<ByteString, ByteString> attachmentKeys = new ConcurrentHashMap<>();

  public InmemoryStore() {}

  @Override
  public void configure(InmemoryConfig config) {}

  @Override
  public void initialize() {}

  @Override
  public void close() {
    globalStateLog.clear();
    commitLog.clear();
    keyLists.clear();
    refLog.clear();
    refHeads.clear();
    refNames.clear();
    refLogHeads.clear();
    attachments.clear();
    attachmentKeys.clear();
  }

  void reinitializeRepo(ByteString keyPrefix) {
    Stream.of(
            repoDesc,
            globalStatePointer,
            globalStateLog,
            commitLog,
            keyLists,
            refLog,
            refHeads,
            refNames,
            refLogHeads,
            attachments,
            attachmentKeys)
        .forEach(map -> map.keySet().removeIf(bytes -> bytes.startsWith(keyPrefix)));
  }
}
