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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringStoreWorker;
import org.projectnessie.versioned.StringStoreWorker.TestEnum;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;

public abstract class AbstractNestedVersionStore {
  protected final VersionStore<String, String, TestEnum> store;

  protected AbstractNestedVersionStore(VersionStore<String, String, TestEnum> store) {
    this.store = store;
  }

  protected VersionStore<String, String, TestEnum> store() {
    return store;
  }

  protected List<Commit<String, String>> commitsList(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return commitsList(ref, Function.identity(), fetchAdditionalInfo);
  }

  protected <T> List<T> commitsList(
      Ref ref,
      Function<Stream<Commit<String, String>>, Stream<T>> streamFunction,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    try (Stream<Commit<String, String>> s = store().getCommits(ref, fetchAdditionalInfo)) {
      return streamFunction.apply(s).collect(Collectors.toList());
    }
  }

  protected static Commit<String, String> commit(Hash hash, String commitMeta) {
    return Commit.<String, String>builder().hash(hash).commitMeta(commitMeta).build();
  }

  protected CommitBuilder<String, String, StringStoreWorker.TestEnum> forceCommit(String message) {
    return new CommitBuilder<>(store()).withMetadata(message);
  }

  protected CommitBuilder<String, String, StringStoreWorker.TestEnum> commit(String message) {
    return new CommitBuilder<>(store()).withMetadata(message).fromLatest();
  }

  protected Put<String> put(String key, String value) {
    return Put.of(Key.of(key), value);
  }

  protected Delete<String> delete(String key) {
    return Delete.of(Key.of(key));
  }

  protected Unchanged<String> unchanged(String key) {
    return Unchanged.of(Key.of(key));
  }

  /** Exclude {@code main} branch in tests. */
  protected boolean filterMainBranch(ReferenceInfo<String> r) {
    return !r.getNamedRef().getName().equals("main");
  }

  protected static void assertCommitMeta(
      List<Commit<String, String>> current,
      List<Commit<String, String>> expected,
      Function<String, String> commitMetaModifier) {
    assertThat(current)
        .map(Commit::getCommitMeta)
        .containsExactlyElementsOf(
            expected.stream()
                .map(Commit::getCommitMeta)
                .map(commitMetaModifier)
                .collect(Collectors.toList()));
  }
}
