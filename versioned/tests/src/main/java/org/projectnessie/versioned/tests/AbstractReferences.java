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
import static org.assertj.core.util.Streams.stream;
import static org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions.BARE;
import static org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions.BASE_REFERENCE_RELATED_AND_COMMIT_META;
import static org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions.OMIT;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.paging.PaginationIterator;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractReferences extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractReferences(VersionStore store) {
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
    final Hash hash = store().hashOnReference(branch, Optional.empty(), emptyList());
    soft.assertThat(hash).isNotNull();

    final BranchName anotherBranch = BranchName.of("bar");
    ReferenceCreatedResult referenceCreatedResult =
        store().create(anotherBranch, Optional.of(hash));
    soft.assertThat(referenceCreatedResult.getHash()).isEqualTo(hash);
    soft.assertThat(referenceCreatedResult.getNamedRef()).isEqualTo(anotherBranch);

    final Hash commitHash = commit("Some Commit").toBranch(anotherBranch);
    soft.assertThat(commitHash).isNotEqualTo(referenceCreatedResult.getHash());

    final BranchName anotherAnotherBranch = BranchName.of("baz");
    referenceCreatedResult = store().create(anotherAnotherBranch, Optional.of(commitHash));
    soft.assertThat(referenceCreatedResult.getHash()).isEqualTo(commitHash);
    soft.assertThat(referenceCreatedResult.getNamedRef()).isEqualTo(anotherAnotherBranch);

    List<ReferenceInfo<CommitMeta>> namedRefs;
    try (PaginationIterator<ReferenceInfo<CommitMeta>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      namedRefs = stream(str).filter(this::filterMainBranch).collect(Collectors.toList());
    }
    soft.assertThat(namedRefs)
        .containsExactlyInAnyOrder(
            ReferenceInfo.of(hash, branch),
            ReferenceInfo.of(commitHash, anotherBranch),
            ReferenceInfo.of(commitHash, anotherAnotherBranch));

    soft.assertThat(commitsList(branch, false)).isEmpty();
    soft.assertThat(commitsList(anotherBranch, false)).hasSize(1);
    soft.assertThat(commitsList(anotherAnotherBranch, false)).hasSize(1);
    soft.assertThat(commitsList(hash, false)).isEmpty(); // empty commit should not be listed
    soft.assertThat(commitsList(commitHash, false)).hasSize(1); // empty commit should not be listed

    soft.assertThatThrownBy(() -> store().create(branch, Optional.empty()))
        .isInstanceOf(ReferenceAlreadyExistsException.class);
    soft.assertThatThrownBy(() -> store().create(branch, Optional.of(hash)))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    ReferenceDeletedResult referenceDeletedResult = store().delete(branch, hash);
    soft.assertThat(referenceDeletedResult.getHash()).isEqualTo(hash);
    soft.assertThat(referenceDeletedResult.getNamedRef()).isEqualTo(branch);

    soft.assertThatThrownBy(() -> store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isInstanceOf(ReferenceNotFoundException.class);
    try (PaginationIterator<ReferenceInfo<CommitMeta>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      soft.assertThat(stream(str).filter(this::filterMainBranch)).hasSize(2); // bar + baz
    }
    soft.assertThatThrownBy(() -> store().delete(branch, hash))
        .isInstanceOf(ReferenceNotFoundException.class);
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

    final Hash initialHash = store().hashOnReference(branch, Optional.empty(), emptyList());
    final Hash commitHash = commit("Some commit").toBranch(branch);

    final TagName tag = TagName.of("tag");
    ReferenceCreatedResult referenceCreatedResult = store().create(tag, Optional.of(initialHash));
    soft.assertThat(referenceCreatedResult.getHash()).isEqualTo(initialHash);
    soft.assertThat(referenceCreatedResult.getNamedRef()).isEqualTo(tag);

    final TagName anotherTag = TagName.of("another-tag");
    referenceCreatedResult = store().create(anotherTag, Optional.of(commitHash));
    soft.assertThat(referenceCreatedResult.getHash()).isEqualTo(commitHash);
    soft.assertThat(referenceCreatedResult.getNamedRef()).isEqualTo(anotherTag);

    soft.assertThatThrownBy(() -> store().create(tag, Optional.of(initialHash)))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    soft.assertThat(store().hashOnReference(tag, Optional.empty(), emptyList()))
        .isEqualTo(initialHash);
    soft.assertThat(store().hashOnReference(anotherTag, Optional.empty(), emptyList()))
        .isEqualTo(commitHash);

    List<ReferenceInfo<CommitMeta>> namedRefs;
    try (PaginationIterator<ReferenceInfo<CommitMeta>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      namedRefs = stream(str).filter(this::filterMainBranch).collect(Collectors.toList());
    }
    soft.assertThat(namedRefs)
        .containsExactlyInAnyOrder(
            ReferenceInfo.of(commitHash, branch),
            ReferenceInfo.of(initialHash, tag),
            ReferenceInfo.of(commitHash, anotherTag));

    soft.assertThat(commitsList(tag, false)).isEmpty();
    soft.assertThat(commitsList(initialHash, false)).isEmpty(); // empty commit should not be listed

    soft.assertThat(commitsList(anotherTag, false)).hasSize(1);
    soft.assertThat(commitsList(commitHash, false)).hasSize(1); // empty commit should not be listed

    ReferenceDeletedResult referenceDeletedResult = store().delete(tag, initialHash);
    soft.assertThat(referenceDeletedResult.getHash()).isEqualTo(initialHash);
    soft.assertThat(referenceDeletedResult.getNamedRef()).isEqualTo(tag);

    soft.assertThatThrownBy(() -> store().hashOnReference(tag, Optional.empty(), emptyList()))
        .isInstanceOf(ReferenceNotFoundException.class);
    try (PaginationIterator<ReferenceInfo<CommitMeta>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      soft.assertThat(stream(str).filter(this::filterMainBranch)).hasSize(2); // foo + another-tag
    }
    soft.assertThatThrownBy(() -> store().delete(tag, initialHash))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  /**
   * Rudimentary test for {@link VersionStore#getNamedRef(String, GetNamedRefsParams)}. Better tests
   * in {@code AbstractGetNamedReferences} in {@code :nessie-versioned-persist-tests}.
   */
  @Test
  void getNamedRef() throws VersionStoreException {
    final BranchName branch = BranchName.of("getNamedRef");
    Hash hashFromCreate = store().create(branch, Optional.empty()).getHash();
    soft.assertThat(store().hashOnReference(branch, Optional.empty(), emptyList()))
        .isEqualTo(hashFromCreate);

    final Hash firstCommitHash = commit("First Commit").toBranch(branch);

    soft.assertThat(store().getNamedRef(branch.getName(), GetNamedRefsParams.DEFAULT))
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
    soft.assertThat(store().getNamedRef(branchName.getName(), GetNamedRefsParams.DEFAULT))
        .extracting(ReferenceInfo::getHash, ReferenceInfo::getNamedRef)
        .containsExactly(secondCommitHash, branchName);

    // Verifies that the result of "getNamedRef" for the tag created at "firstCommitHash" is correct
    soft.assertThat(store().getNamedRef(tagName.getName(), GetNamedRefsParams.DEFAULT))
        .extracting(ReferenceInfo::getHash, ReferenceInfo::getNamedRef)
        .containsExactly(thirdCommitHash, tagName);

    // Verifies that the result of "getNamedRef" for the branch created at "firstCommitHash" is
    // correct
    soft.assertThat(store().getNamedRef(branchName.getName(), GetNamedRefsParams.DEFAULT))
        .extracting(ReferenceInfo::getHash, ReferenceInfo::getNamedRef)
        .containsExactly(secondCommitHash, branchName);

    soft.assertThatThrownBy(() -> store().getNamedRef("unknown-ref", GetNamedRefsParams.DEFAULT))
        .isInstanceOf(ReferenceNotFoundException.class);
    soft.assertThatThrownBy(
            () -> store().getNamedRef("1234567890abcdef", GetNamedRefsParams.DEFAULT))
        .isInstanceOf(ReferenceNotFoundException.class);

    soft.assertThatThrownBy(
            () ->
                store()
                    .getNamedRef(
                        branchName.getName(),
                        GetNamedRefsParams.builder()
                            .baseReference(BranchName.of("does-not-exist"))
                            .branchRetrieveOptions(BASE_REFERENCE_RELATED_AND_COMMIT_META)
                            .build()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessageContaining("'does-not-exist");
  }

  @Test
  void listBranchesOrTags() throws Exception {
    // Generate branches + tags with "interleaving" names
    Set<NamedRef> branches = new HashSet<>();
    Set<NamedRef> tags = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      NamedRef r = BranchName.of((char) ('a' + i) + "-b");
      store().create(r, Optional.empty());
      branches.add(r);
      r = TagName.of((char) ('a' + i) + "-t");
      store().create(r, Optional.empty());
      tags.add(r);
    }

    // Check branches (no tags)
    try (PaginationIterator<ReferenceInfo<CommitMeta>> refs =
        store()
            .getNamedRefs(
                GetNamedRefsParams.builder()
                    .branchRetrieveOptions(BARE)
                    .tagRetrieveOptions(OMIT)
                    .build(),
                null)) {
      soft.assertThat(Lists.newArrayList(refs))
          .extracting(ReferenceInfo::getNamedRef)
          .hasSize(branches.size() + 1) // --> main branch
          .containsAll(branches);
    }

    // Check tags (no branches)
    try (PaginationIterator<ReferenceInfo<CommitMeta>> refs =
        store()
            .getNamedRefs(
                GetNamedRefsParams.builder()
                    .branchRetrieveOptions(OMIT)
                    .tagRetrieveOptions(BARE)
                    .build(),
                null)) {
      soft.assertThat(Lists.newArrayList(refs))
          .extracting(ReferenceInfo::getNamedRef)
          .containsExactlyInAnyOrderElementsOf(tags);
    }

    // Check branches + tags
    try (PaginationIterator<ReferenceInfo<CommitMeta>> refs =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      soft.assertThat(Lists.newArrayList(refs))
          .extracting(ReferenceInfo::getNamedRef)
          .hasSize(branches.size() + tags.size() + 1) // --> main branch
          .containsAll(branches)
          .containsAll(tags);
    }
  }
}
