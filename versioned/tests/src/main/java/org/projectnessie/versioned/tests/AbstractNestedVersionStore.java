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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;

public abstract class AbstractNestedVersionStore {
  protected final VersionStore store;

  protected AbstractNestedVersionStore(VersionStore store) {
    this.store = store;
  }

  protected VersionStore store() {
    return store;
  }

  protected List<Commit> commitsList(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return commitsList(ref, Function.identity(), fetchAdditionalInfo);
  }

  protected <T> List<T> commitsList(
      Ref ref, Function<Stream<Commit>, Stream<T>> streamFunction, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    try (Stream<Commit> s = store().getCommits(ref, fetchAdditionalInfo)) {
      return streamFunction.apply(s).collect(Collectors.toList());
    }
  }

  protected static Commit commit(Hash hash, String commitMeta) {
    return commit(hash, commitMeta, null);
  }

  protected static Commit commit(Hash hash, String commitMeta, Hash parentHash) {
    return commit(hash, CommitMeta.fromMessage(commitMeta), parentHash);
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
    return new CommitBuilder(store()).withMetadata(CommitMeta.fromMessage(message)).fromLatest();
  }

  protected Put put(String key, Content value) {
    return Put.of(Key.of(key), value);
  }

  protected Delete delete(String key) {
    return Delete.of(Key.of(key));
  }

  protected Unchanged unchanged(String key) {
    return Unchanged.of(Key.of(key));
  }

  /** Exclude {@code main} branch in tests. */
  protected boolean filterMainBranch(ReferenceInfo<CommitMeta> r) {
    return !r.getNamedRef().getName().equals("main");
  }

  protected static void assertCommitMeta(
      SoftAssertions soft,
      List<Commit> current,
      List<Commit> expected,
      MetadataRewriter<CommitMeta> commitMetaModifier) {
    soft.assertThat(current)
        .map(Commit::getCommitMeta)
        .containsExactlyElementsOf(
            expected.stream()
                .map(Commit::getCommitMeta)
                .map(commitMetaModifier::rewriteSingle)
                .collect(Collectors.toList()));
  }
}
