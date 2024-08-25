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

import static java.util.Collections.emptyList;
import static org.assertj.core.util.Lists.newArrayList;

import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.paging.PaginationIterator;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractAssign extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractAssign(VersionStore store) {
    super(store);
  }

  @Test
  public void assign() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());
    final Hash initialHash = store().hashOnReference(branch, Optional.empty(), emptyList());

    final Hash commit = commit("Some commit").toBranch(branch);
    store().create(BranchName.of("bar"), Optional.of(commit));
    store().create(TagName.of("tag1"), Optional.of(commit));
    store().create(TagName.of("tag2"), Optional.of(commit));
    store().create(TagName.of("tag3"), Optional.of(commit));

    final Hash anotherCommit = commit("Another commit").toBranch(branch);
    ReferenceAssignedResult referenceAssignedResult =
        store().assign(TagName.of("tag2"), commit, anotherCommit);
    soft.assertThat(referenceAssignedResult.getPreviousHash()).isEqualTo(commit);
    soft.assertThat(referenceAssignedResult.getCurrentHash()).isEqualTo(anotherCommit);
    soft.assertThat(referenceAssignedResult.getNamedRef()).isEqualTo(TagName.of("tag2"));

    referenceAssignedResult = store().assign(TagName.of("tag3"), commit, anotherCommit);
    soft.assertThat(referenceAssignedResult.getPreviousHash()).isEqualTo(commit);
    soft.assertThat(referenceAssignedResult.getCurrentHash()).isEqualTo(anotherCommit);
    soft.assertThat(referenceAssignedResult.getNamedRef()).isEqualTo(TagName.of("tag3"));

    soft.assertThatThrownBy(() -> store().assign(BranchName.of("baz"), commit, anotherCommit))
        .isInstanceOf(ReferenceNotFoundException.class);
    soft.assertThatThrownBy(() -> store().assign(TagName.of("unknowon-tag"), commit, anotherCommit))
        .isInstanceOf(ReferenceNotFoundException.class);

    soft.assertThatThrownBy(() -> store().assign(TagName.of("tag1"), initialHash, commit))
        .isInstanceOf(ReferenceConflictException.class);
    soft.assertThatThrownBy(() -> store().assign(TagName.of("tag1"), initialHash, anotherCommit))
        .isInstanceOf(ReferenceConflictException.class);
    soft.assertThatThrownBy(
            () -> store().assign(TagName.of("tag1"), commit, Hash.of("1234567890abcdef")))
        .isInstanceOf(ReferenceNotFoundException.class);

    soft.assertThat(commitsList(branch, false))
        .contains(
            commit(anotherCommit, "Another commit", commit),
            commit(commit, "Some commit", initialHash));

    soft.assertThat(commitsList(BranchName.of("bar"), false))
        .contains(commit(commit, "Some commit", initialHash));

    soft.assertThat(commitsList(TagName.of("tag1"), false))
        .contains(commit(commit, "Some commit", initialHash));

    soft.assertThat(commitsList(TagName.of("tag2"), false))
        .contains(
            commit(anotherCommit, "Another commit", commit),
            commit(commit, "Some commit", initialHash));
  }

  /* Assigning a branch/tag to a fresh main without any commits didn't work in 0.9.2 */
  @Test
  public void assignReferenceToFreshMain()
      throws ReferenceNotFoundException,
          ReferenceAlreadyExistsException,
          ReferenceConflictException {
    ReferenceInfo<CommitMeta> main = store.getNamedRef("main", GetNamedRefsParams.DEFAULT);
    try (PaginationIterator<Commit> commits = store().getCommits(main.getHash(), false)) {
      soft.assertThat(commits).isExhausted();
    }
    try (PaginationIterator<ReferenceInfo<CommitMeta>> refs =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      soft.assertThat(newArrayList(refs))
          .extracting(r -> r.getNamedRef().getName())
          .containsExactly(main.getNamedRef().getName());
    }

    BranchName testBranch = BranchName.of("testBranch");
    Hash testBranchHash = store.create(testBranch, Optional.empty()).getHash();
    ReferenceAssignedResult referenceAssignedResult =
        store.assign(testBranch, testBranchHash, main.getHash());
    soft.assertThat(referenceAssignedResult.getPreviousHash()).isEqualTo(main.getHash());
    soft.assertThat(referenceAssignedResult.getCurrentHash()).isEqualTo(testBranchHash);
    soft.assertThat(referenceAssignedResult.getNamedRef()).isEqualTo(testBranch);

    soft.assertThat(store.getNamedRef(testBranch.getName(), GetNamedRefsParams.DEFAULT).getHash())
        .isEqualTo(main.getHash());

    TagName testTag = TagName.of("testTag");
    Hash testTagHash = store.create(testTag, Optional.empty()).getHash();
    referenceAssignedResult = store.assign(testTag, testTagHash, main.getHash());
    soft.assertThat(referenceAssignedResult.getPreviousHash()).isEqualTo(main.getHash());
    soft.assertThat(referenceAssignedResult.getCurrentHash()).isEqualTo(testTagHash);
    soft.assertThat(referenceAssignedResult.getNamedRef()).isEqualTo(testTag);

    soft.assertThat(store.getNamedRef(testTag.getName(), GetNamedRefsParams.DEFAULT).getHash())
        .isEqualTo(main.getHash());
  }
}
