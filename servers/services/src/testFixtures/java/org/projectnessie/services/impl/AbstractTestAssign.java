/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.impl;

import static org.projectnessie.model.Reference.ReferenceType.BRANCH;
import static org.projectnessie.model.Reference.ReferenceType.TAG;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public abstract class AbstractTestAssign extends BaseTestServiceImpl {

  /* Assigning a branch/tag to a fresh main without any commits didn't work in 0.9.2 */
  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void testAssignRefToFreshMain(ReferenceMode refMode)
      throws BaseNessieClientServerException {
    Branch main = treeApi().getDefaultBranch();
    // make sure main doesn't have any commits
    soft.assertThat(commitLog(main.getName())).isEmpty();

    Branch testBranch = createBranch("testBranch");
    treeApi().assignReference(BRANCH, testBranch.getName(), testBranch.getHash(), main);
    Reference testBranchRef = getReference(testBranch.getName());
    soft.assertThat(testBranchRef.getHash()).isEqualTo(main.getHash());

    String testTag = "testTag";
    Reference testTagRef = createTag(testTag, main);
    soft.assertThat(testTagRef.getHash()).isNotNull();
    Reference transformed = refMode.transform(main);
    String expectedHash = testTagRef.getHash();
    if (refMode == ReferenceMode.NAME_ONLY) {
      soft.assertThatThrownBy(
              () -> treeApi().assignReference(TAG, testTag, expectedHash, transformed))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Target hash must be provided");
    } else {
      treeApi().assignReference(TAG, testTag, expectedHash, transformed);
      testTagRef = getReference(testTag);
      soft.assertThat(testTagRef.getHash()).isEqualTo(main.getHash());
    }
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void testAssignBranch(ReferenceMode refMode) throws BaseNessieClientServerException {
    Reference main = createBranch("test-main2");
    Branch branch = createBranch("test-branch2");

    // make a commit in main
    createCommits(main, 1, 1, main.getHash());
    main = getReference(main.getName());

    soft.assertThat(branch.getHash()).isNotEqualTo(main.getHash());

    // Assign the test branch to main
    Reference transformed = refMode.transform(main);
    if (refMode == ReferenceMode.NAME_ONLY) {
      soft.assertThatThrownBy(
              () ->
                  treeApi()
                      .assignReference(BRANCH, branch.getName(), branch.getHash(), transformed))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Target hash must be provided");
    } else {
      Branch assignedBranch =
          (Branch)
              treeApi().assignReference(BRANCH, branch.getName(), branch.getHash(), transformed);
      soft.assertThat(assignedBranch.getHash()).isEqualTo(main.getHash());

      Reference currentBranch = getReference(branch.getName());
      soft.assertThat(assignedBranch).isEqualTo(currentBranch);
    }
  }

  @ParameterizedTest
  @EnumSource(ReferenceMode.class)
  public void testAssignTag(ReferenceMode refMode) throws BaseNessieClientServerException {
    Reference main = createBranch("test-main2");
    Reference tag = createTag("testTag2", main);

    // make a commit in main
    createCommits(main, 1, 1, main.getHash());
    main = getReference(main.getName());

    soft.assertThat(tag.getHash()).isNotEqualTo(main.getHash());

    // Assign the test tag to main
    Reference transformed = refMode.transform(main);
    if (refMode == ReferenceMode.NAME_ONLY) {
      soft.assertThatThrownBy(
              () -> treeApi().assignReference(TAG, tag.getName(), tag.getHash(), transformed))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Target hash must be provided");
    } else {
      Tag assignedTag =
          (Tag) treeApi().assignReference(TAG, tag.getName(), tag.getHash(), transformed);
      soft.assertThat(assignedTag.getHash()).isEqualTo(main.getHash());

      Reference currentTag = getReference(tag.getName());
      soft.assertThat(assignedTag).isEqualTo(currentTag);
    }
  }
}
