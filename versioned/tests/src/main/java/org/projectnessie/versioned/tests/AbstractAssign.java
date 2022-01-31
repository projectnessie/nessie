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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringStoreWorker.TestEnum;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;

public abstract class AbstractAssign extends AbstractNestedVersionStore {
  protected AbstractAssign(VersionStore<String, String, TestEnum> store) {
    super(store);
  }

  @Test
  public void assign() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());
    final Hash initialHash = store().hashOnReference(branch, Optional.empty());

    final Hash commit = commit("Some commit").toBranch(branch);
    store().create(BranchName.of("bar"), Optional.of(commit));
    store().create(TagName.of("tag1"), Optional.of(commit));
    store().create(TagName.of("tag2"), Optional.of(commit));
    store().create(TagName.of("tag3"), Optional.of(commit));

    final Hash anotherCommit = commit("Another commit").toBranch(branch);
    store().assign(TagName.of("tag2"), Optional.of(commit), anotherCommit);
    store().assign(TagName.of("tag3"), Optional.empty(), anotherCommit);

    assertThrows(
        ReferenceNotFoundException.class,
        () -> store().assign(BranchName.of("baz"), Optional.empty(), anotherCommit));
    assertThrows(
        ReferenceNotFoundException.class,
        () -> store().assign(TagName.of("unknowon-tag"), Optional.empty(), anotherCommit));

    assertThrows(
        ReferenceConflictException.class,
        () -> store().assign(TagName.of("tag1"), Optional.of(initialHash), commit));
    assertThrows(
        ReferenceConflictException.class,
        () -> store().assign(TagName.of("tag1"), Optional.of(initialHash), anotherCommit));
    assertThrows(
        ReferenceNotFoundException.class,
        () -> store().assign(TagName.of("tag1"), Optional.of(commit), Hash.of("1234567890abcdef")));

    assertThat(commitsList(branch, false))
        .contains(commit(anotherCommit, "Another commit"), commit(commit, "Some commit"));

    assertThat(commitsList(BranchName.of("bar"), false)).contains(commit(commit, "Some commit"));

    assertThat(commitsList(TagName.of("tag1"), false)).contains(commit(commit, "Some commit"));

    assertThat(commitsList(TagName.of("tag2"), false))
        .contains(commit(anotherCommit, "Another commit"), commit(commit, "Some commit"));
  }

  /** Assigning a branch/tag to a fresh main without any commits didn't work in 0.9.2 */
  @Test
  public void assignReferenceToFreshMain()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException,
          ReferenceConflictException {
    ReferenceInfo<String> main = store.getNamedRef("main", GetNamedRefsParams.DEFAULT);
    assertThat(store().getCommits(main.getHash(), false)).isEmpty();
    try (Stream<ReferenceInfo<String>> refs = store().getNamedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs)
          .extracting(r -> r.getNamedRef().getName())
          .containsExactly(main.getNamedRef().getName());
    }

    BranchName testBranch = BranchName.of("testBranch");
    Hash testBranchHash = store.create(testBranch, Optional.empty());
    store.assign(testBranch, Optional.of(testBranchHash), main.getHash());
    assertThat(store.getNamedRef(testBranch.getName(), GetNamedRefsParams.DEFAULT).getHash())
        .isEqualTo(main.getHash());

    TagName testTag = TagName.of("testTag");
    Hash testTagHash = store.create(testTag, Optional.empty());
    store.assign(testTag, Optional.of(testTagHash), main.getHash());
    assertThat(store.getNamedRef(testTag.getName(), GetNamedRefsParams.DEFAULT).getHash())
        .isEqualTo(main.getHash());
  }
}
