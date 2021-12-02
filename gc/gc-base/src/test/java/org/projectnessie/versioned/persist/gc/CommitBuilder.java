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
package org.projectnessie.versioned.persist.gc;

import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;

public final class CommitBuilder {
  final Instant ts;
  final NamedRef ref;
  final Dataset dataset;
  final Map<ContentId, ByteString> globals = new HashMap<>();
  final List<KeyWithBytes> puts = new ArrayList<>();
  final List<Key> deletes = new ArrayList<>();

  CommitBuilder(Dataset dataset, NamedRef ref, Instant ts) {
    this.ts = ts;
    this.ref = ref;
    this.dataset = dataset;
  }

  /**
   * Adds a "Put operation" for a key/content-id/content-value and whether that value is expected to
   * be live or non-live after GC. Note: the expectExpired can be passed as true, if this content is
   * expected to be expired after gc
   */
  CommitBuilder putContent(ContentKey key, Content content, boolean expectExpired) {
    ContentId cid = ContentId.of(content.getId());
    puts.add(
        KeyWithBytes.of(
            Key.of(key.getElements().toArray(new String[0])),
            cid,
            (byte) Dataset.STORE_WORKER.getType(content).ordinal(),
            Dataset.STORE_WORKER.toStoreOnReferenceState(content)));
    if (Dataset.STORE_WORKER.requiresGlobalState(content)) {
      globals.put(cid, Dataset.STORE_WORKER.toStoreGlobalState(content));
    }
    Map<String, Set<Content>> expect =
        expectExpired ? dataset.expectedExpired : dataset.expectedLive;
    expect.computeIfAbsent(content.getId(), x -> new HashSet<>()).add(content);
    return this;
  }

  /** Adds a "Delete operation" for a key. */
  CommitBuilder deleteContent(ContentKey delete) {
    deletes.add(Key.of(delete.getElements().toArray(new String[0])));
    return this;
  }

  /** Build and record this commit. */
  Dataset commit() {
    return dataset.recordCommit(
        adapter -> {
          ImmutableCommitAttempt.Builder commit =
              ImmutableCommitAttempt.builder()
                  .commitMetaSerialized(
                      Dataset.STORE_WORKER
                          .getMetadataSerializer()
                          .toBytes(
                              ImmutableCommitMeta.builder()
                                  .commitTime(ts)
                                  .committer("")
                                  .authorTime(ts)
                                  .author("")
                                  .message("foo message")
                                  .build()))
                  .putAllGlobal(globals)
                  .commitToBranch((BranchName) ref)
                  .addAllPuts(puts)
                  .addAllDeletes(deletes);
          try {
            adapter.commit(commit.build());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
