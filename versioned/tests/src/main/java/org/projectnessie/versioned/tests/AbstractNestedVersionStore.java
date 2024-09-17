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
package org.projectnessie.versioned.tests;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.DefaultMetadataRewriter.DEFAULT_METADATA_REWRITER;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.PaginationIterator;

public abstract class AbstractNestedVersionStore {
  protected final VersionStore store;

  protected AbstractNestedVersionStore(VersionStore store) {
    this.store = store;
  }

  protected VersionStore store() {
    return store;
  }

  protected StorageAssertions storageCheckpoint() {
    if (store instanceof ValidatingVersionStore) {
      return ((ValidatingVersionStore) store).storageCheckpoint();
    }
    return new StorageAssertions(); // non-validating
  }

  protected List<Commit> commitsList(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    try (PaginationIterator<Commit> s = store().getCommits(ref, fetchAdditionalInfo)) {
      return newArrayList(s);
    }
  }

  protected <T> List<T> commitsListMap(Ref ref, int limit, Function<Commit, T> mapper)
      throws ReferenceNotFoundException {
    try (PaginationIterator<Commit> s = store().getCommits(ref, false)) {
      List<T> r = new ArrayList<>();
      while (r.size() < limit && s.hasNext()) {
        r.add(mapper.apply(s.next()));
      }
      return r;
    }
  }

  protected static Commit commit(Hash hash, String commitMeta, Hash parentHash) {
    return commit(
        hash,
        CommitMeta.builder()
            .message(commitMeta)
            .hash(hash.asString())
            .addParentCommitHashes(parentHash.asString())
            .build(),
        parentHash);
  }

  protected static Commit commit(Hash hash, CommitMeta commitMessage) {
    return commit(hash, commitMessage, null);
  }

  protected static Commit commit(Hash hash, CommitMeta commitMessage, Hash parentHash) {
    ImmutableCommit.Builder builder = Commit.builder().hash(hash).commitMeta(commitMessage);
    if (parentHash != null) {
      builder.parentHash(parentHash);
    }
    return builder.build();
  }

  protected CommitBuilder forceCommit(String message) {
    return new CommitBuilder(store()).withMetadata(CommitMeta.fromMessage(message));
  }

  protected CommitBuilder commit(String message) {
    return commit(store(), message);
  }

  protected CommitBuilder commit(VersionStore store, String message) {
    return new CommitBuilder(store).withMetadata(CommitMeta.fromMessage(message)).fromLatest();
  }

  protected Put put(String key, Content value) {
    return Put.of(ContentKey.of(key), value);
  }

  protected Delete delete(String key) {
    return Delete.of(ContentKey.of(key));
  }

  protected Unchanged unchanged(String key) {
    return Unchanged.of(ContentKey.of(key));
  }

  /** Exclude {@code main} branch in tests. */
  protected boolean filterMainBranch(ReferenceInfo<CommitMeta> r) {
    return !r.getNamedRef().getName().equals("main");
  }

  protected static void assertCommitMeta(
      SoftAssertions soft, List<Commit> current, List<Commit> expected) {
    soft.assertThat(current)
        .map(Commit::getCommitMeta)
        .map(
            m ->
                (CommitMeta)
                    CommitMeta.builder().from(m).hash(null).parentCommitHashes(emptyList()).build())
        .containsExactlyElementsOf(
            expected.stream()
                .map(Commit::getCommitMeta)
                .map(DEFAULT_METADATA_REWRITER::rewriteSingle)
                .collect(Collectors.toList()));
  }

  protected static Content contentWithoutId(ContentResult content) {
    return content != null ? requireNonNull(content.content()).withId(null) : null;
  }

  protected static Content contentWithoutId(Content content) {
    return content != null ? content.withId(null) : null;
  }

  protected static Optional<Content> contentWithoutId(Optional<Content> content) {
    return content.map(AbstractNestedVersionStore::contentWithoutId);
  }

  protected static Map<ContentKey, Content> contentsWithoutId(
      Map<ContentKey, ContentResult> valueMap) {
    return valueMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> contentWithoutId(e.getValue())));
  }

  protected static List<Diff> diffsWithoutContentId(List<Diff> diffs) {
    return diffs.stream()
        .map(
            d ->
                Diff.of(
                    d.getFromKey(),
                    d.getToKey(),
                    contentWithoutId(d.getFromValue()),
                    contentWithoutId(d.getToValue())))
        .collect(Collectors.toList());
  }

  protected static List<Operation> operationsWithoutContentId(List<Operation> operations) {
    if (operations == null) {
      return null;
    }
    return operations.stream()
        .map(
            op -> {
              if (op instanceof Put) {
                Put put = (Put) op;
                Content content = put.getContent();
                return Put.of(put.getKey(), contentWithoutId(content));
              }
              return op;
            })
        .collect(Collectors.toList());
  }
}
