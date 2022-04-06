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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.testworker.CommitMessage.commitMessage;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;

public abstract class AbstractNestedVersionStore {
  protected final VersionStore<BaseContent, CommitMessage, BaseContent.Type> store;

  protected AbstractNestedVersionStore(
      VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    this.store = store;
  }

  protected VersionStore<BaseContent, CommitMessage, BaseContent.Type> store() {
    return store;
  }

  protected List<Commit<CommitMessage, BaseContent>> commitsList(
      Ref ref, boolean fetchAdditionalInfo) throws ReferenceNotFoundException {
    return commitsList(ref, Function.identity(), fetchAdditionalInfo);
  }

  protected <T> List<T> commitsList(
      Ref ref,
      Function<Stream<Commit<CommitMessage, BaseContent>>, Stream<T>> streamFunction,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    try (Stream<Commit<CommitMessage, BaseContent>> s =
        store().getCommits(ref, fetchAdditionalInfo)) {
      return streamFunction.apply(s).collect(Collectors.toList());
    }
  }

  protected static Commit<CommitMessage, BaseContent> commit(Hash hash, String commitMeta) {
    return commit(hash, commitMessage(commitMeta));
  }

  protected static Commit<CommitMessage, BaseContent> commit(
      Hash hash, CommitMessage commitMessage) {
    return Commit.<CommitMessage, BaseContent>builder()
        .hash(hash)
        .commitMeta(commitMessage)
        .build();
  }

  protected CommitBuilder<BaseContent, CommitMessage, BaseContent.Type> forceCommit(
      String message) {
    return new CommitBuilder<>(store()).withMetadata(commitMessage(message));
  }

  protected CommitBuilder<BaseContent, CommitMessage, BaseContent.Type> commit(String message) {
    return new CommitBuilder<>(store()).withMetadata(commitMessage(message)).fromLatest();
  }

  protected Put<BaseContent> put(String key, BaseContent value) {
    return Put.of(Key.of(key), value);
  }

  protected Delete<BaseContent> delete(String key) {
    return Delete.of(Key.of(key));
  }

  protected Unchanged<BaseContent> unchanged(String key) {
    return Unchanged.of(Key.of(key));
  }

  /** Exclude {@code main} branch in tests. */
  protected boolean filterMainBranch(ReferenceInfo<CommitMessage> r) {
    return !r.getNamedRef().getName().equals("main");
  }

  protected static void assertCommitMeta(
      List<Commit<CommitMessage, BaseContent>> current,
      List<Commit<CommitMessage, BaseContent>> expected,
      MetadataRewriter<CommitMessage> commitMetaModifier) {
    assertThat(current)
        .map(Commit::getCommitMeta)
        .containsExactlyElementsOf(
            expected.stream()
                .map(Commit::getCommitMeta)
                .map(commitMetaModifier::rewriteSingle)
                .collect(Collectors.toList()));
  }
}
