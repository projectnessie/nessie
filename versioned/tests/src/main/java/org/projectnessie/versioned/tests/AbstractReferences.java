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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;

public abstract class AbstractReferences extends AbstractNestedVersionStore {
  protected AbstractReferences(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  /*
   * Test:
   * - Create a branch with no hash assigned to it
   * - check that a hash is returned by toHash
   * - check the branch is returned by getNamedRefs
   * - check that no commits are returned using getCommits
   * - check the branch cannot be created
   * - check the branch can be deleted
   */
  @Test
  public void createAndDeleteBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());
    final Hash hash = store().hashOnReference(branch, Optional.empty());
    assertThat(hash).isNotNull();

    final BranchName anotherBranch = BranchName.of("bar");
    final Hash createHash = store().create(anotherBranch, Optional.of(hash));
    final Hash commitHash = commit("Some Commit").toBranch(anotherBranch);
    assertNotEquals(createHash, commitHash);

    final BranchName anotherAnotherBranch = BranchName.of("baz");
    final Hash otherCreateHash = store().create(anotherAnotherBranch, Optional.of(commitHash));
    assertEquals(commitHash, otherCreateHash);

    List<ReferenceInfo<CommitMessage>> namedRefs;
    try (Stream<ReferenceInfo<CommitMessage>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT).filter(this::filterMainBranch)) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs)
        .containsExactlyInAnyOrder(
            ReferenceInfo.of(hash, branch),
            ReferenceInfo.of(commitHash, anotherBranch),
            ReferenceInfo.of(commitHash, anotherAnotherBranch));

    assertThat(commitsList(branch, false)).isEmpty();
    assertThat(commitsList(anotherBranch, false)).hasSize(1);
    assertThat(commitsList(anotherAnotherBranch, false)).hasSize(1);
    assertThat(commitsList(hash, false)).isEmpty(); // empty commit should not be listed
    assertThat(commitsList(commitHash, false)).hasSize(1); // empty commit should not be listed

    assertThrows(
        ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.empty()));
    assertThrows(
        ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.of(hash)));

    store().delete(branch, Optional.of(hash));
    assertThrows(
        ReferenceNotFoundException.class, () -> store().hashOnReference(branch, Optional.empty()));
    try (Stream<ReferenceInfo<CommitMessage>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT).filter(this::filterMainBranch)) {
      assertThat(str).hasSize(2); // bar + baz
    }
    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(hash)));
  }

  /*
   * Test:
   * - Create a branch with no hash assigned to it
   * - add a commit to the branch
   * - create a tag for the initial hash
   * - create another tag for the hash after the commit
   * - check that cannot create existing tags, or tag with no assigned hash
   * - check that a hash is returned by toHash
   * - check the tags are returned by getNamedRefs
   * - check that expected commits are returned by getCommits
   * - check the branch can be deleted
   */
  @Test
  public void createAndDeleteTag() throws Exception {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    final Hash initialHash = store().hashOnReference(branch, Optional.empty());
    final Hash commitHash = commit("Some commit").toBranch(branch);

    final TagName tag = TagName.of("tag");
    store().create(tag, Optional.of(initialHash));

    final TagName anotherTag = TagName.of("another-tag");
    store().create(anotherTag, Optional.of(commitHash));

    assertThrows(
        ReferenceAlreadyExistsException.class, () -> store().create(tag, Optional.of(initialHash)));

    assertThat(store().hashOnReference(tag, Optional.empty())).isEqualTo(initialHash);
    assertThat(store().hashOnReference(anotherTag, Optional.empty())).isEqualTo(commitHash);

    List<ReferenceInfo<CommitMessage>> namedRefs;
    try (Stream<ReferenceInfo<CommitMessage>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT).filter(this::filterMainBranch)) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs)
        .containsExactlyInAnyOrder(
            ReferenceInfo.of(commitHash, branch),
            ReferenceInfo.of(initialHash, tag),
            ReferenceInfo.of(commitHash, anotherTag));

    assertThat(commitsList(tag, false)).isEmpty();
    assertThat(commitsList(initialHash, false)).isEmpty(); // empty commit should not be listed

    assertThat(commitsList(anotherTag, false)).hasSize(1);
    assertThat(commitsList(commitHash, false)).hasSize(1); // empty commit should not be listed

    store().delete(tag, Optional.of(initialHash));
    assertThrows(
        ReferenceNotFoundException.class, () -> store().hashOnReference(tag, Optional.empty()));
    try (Stream<ReferenceInfo<CommitMessage>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT).filter(this::filterMainBranch)) {
      assertThat(str).hasSize(2); // foo + another-tag
    }
    assertThrows(
        ReferenceNotFoundException.class, () -> store().delete(tag, Optional.of(initialHash)));
  }

  /**
   * Rudimentary test for {@link VersionStore#getNamedRef(String, GetNamedRefsParams)}. Better tests
   * in {@code AbstractGetNamedReferences} in {@code :nessie-versioned-persist-tests}.
   */
  @Test
  void getNamedRef() throws VersionStoreException {
    final BranchName branch = BranchName.of("getNamedRef");
    Hash hashFromCreate = store().create(branch, Optional.empty());
    assertThat(store().hashOnReference(branch, Optional.empty())).isEqualTo(hashFromCreate);

    final Hash firstCommitHash = commit("First Commit").toBranch(branch);

    assertThat(store().getNamedRef(branch.getName(), GetNamedRefsParams.DEFAULT))
        .extracting(ReferenceInfo::getHash, ReferenceInfo::getNamedRef)
        .containsExactly(firstCommitHash, branch);

    final Hash secondCommitHash = commit("Second Commit").toBranch(branch);
    final Hash thirdCommitHash = commit("Third Commit").toBranch(branch);

    BranchName branchName = BranchName.of("getNamedRef_branch_" + secondCommitHash.asString());
    TagName tagName = TagName.of("getNamedRef_tag_" + thirdCommitHash.asString());

    store().create(branchName, Optional.of(secondCommitHash));
    store().create(tagName, Optional.of(thirdCommitHash));

    // Verifies that the result of "getNamedRef" for the branch created at "firstCommitHash" is
    // correct
    assertThat(store().getNamedRef(branchName.getName(), GetNamedRefsParams.DEFAULT))
        .extracting(ReferenceInfo::getHash, ReferenceInfo::getNamedRef)
        .containsExactly(secondCommitHash, branchName);

    // Verifies that the result of "getNamedRef" for the tag created at "firstCommitHash" is correct
    assertThat(store().getNamedRef(tagName.getName(), GetNamedRefsParams.DEFAULT))
        .extracting(ReferenceInfo::getHash, ReferenceInfo::getNamedRef)
        .containsExactly(thirdCommitHash, tagName);

    // Verifies that the result of "getNamedRef" for the branch created at "firstCommitHash" is
    // correct
    assertThat(store().getNamedRef(branchName.getName(), GetNamedRefsParams.DEFAULT))
        .extracting(ReferenceInfo::getHash, ReferenceInfo::getNamedRef)
        .containsExactly(secondCommitHash, branchName);

    assertThrows(
        ReferenceNotFoundException.class,
        () -> store().getNamedRef("unknown-ref", GetNamedRefsParams.DEFAULT));
    assertThrows(
        ReferenceNotFoundException.class,
        () -> store().getNamedRef("1234567890abcdef", GetNamedRefsParams.DEFAULT));
  }
}
