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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableReferenceInfo;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceInfo.CommitsAheadBehind;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;

public abstract class AbstractGetNamedReferences {

  public static final String MAIN_BRANCH = "main";
  private static final Key SOME_KEY = Key.of("a", "b", "c");
  private static final String SOME_CONTENT_ID = "abc";
  private final DatabaseAdapter databaseAdapter;

  static RetrieveOptions COMPUTE_AHEAD_BEHIND =
      RetrieveOptions.builder().isComputeAheadBehind(true).build();
  static RetrieveOptions COMPUTE_COMMON_ANCESTOR =
      RetrieveOptions.builder().isComputeAheadBehind(true).build();
  static RetrieveOptions COMPUTE_ALL =
      RetrieveOptions.builder().isComputeAheadBehind(true).isComputeCommonAncestor(true).build();

  protected AbstractGetNamedReferences(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  public void parameterValidationNamedRefs() throws Exception {
    BranchName main = BranchName.of(MAIN_BRANCH);
    BranchName parameterValidation = BranchName.of("parameterValidation");
    TagName parameterValidationTag = TagName.of("parameterValidationTag");

    Hash hash = databaseAdapter.noAncestorHash();

    assertThat(databaseAdapter.hashOnReference(main, Optional.empty())).isEqualTo(hash);

    databaseAdapter.create(parameterValidation, hash);
    databaseAdapter.create(parameterValidationTag, hash);

    assertAll(
        () ->
            assertThatThrownBy(() -> databaseAdapter.namedRefs(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Parameter for GetNamedRefsParams must not be null."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRefs(
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRefs(
                            GetNamedRefsParams.builder()
                                .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRefs(
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRefs(
                            GetNamedRefsParams.builder()
                                .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRefs(
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .baseReference(BranchName.of("no-no-no"))
                                .build()))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage("Named reference 'no-no-no' not found"),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRefs(
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .baseReference(TagName.of("blah-no"))
                                .build()))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage("Named reference 'blah-no' not found"),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRefs(
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(RetrieveOptions.OMIT)
                                .tagRetrieveOptions(RetrieveOptions.OMIT)
                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Must retrieve branches or tags or both."));
  }

  @Test
  public void parameterValidationNamedRef() throws Exception {
    BranchName main = BranchName.of(MAIN_BRANCH);
    BranchName parameterValidation = BranchName.of("parameterValidation");
    TagName parameterValidationTag = TagName.of("parameterValidationTag");

    Hash hash = databaseAdapter.noAncestorHash();

    assertThat(databaseAdapter.hashOnReference(main, Optional.empty())).isEqualTo(hash);

    databaseAdapter.create(parameterValidation, hash);
    databaseAdapter.create(parameterValidationTag, hash);

    assertAll(
        () ->
            assertThatThrownBy(() -> databaseAdapter.namedRef(main.getName(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Parameter for GetNamedRefsParams must not be null"),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRef(
                            parameterValidation.getName(),
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRef(
                            parameterValidation.getName(),
                            GetNamedRefsParams.builder()
                                .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRef(
                            parameterValidation.getName(),
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRef(
                            parameterValidation.getName(),
                            GetNamedRefsParams.builder()
                                .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                                .build()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base reference name missing."),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRef(
                            parameterValidation.getName(),
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .baseReference(BranchName.of("no-no-no"))
                                .build()))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage("Named reference 'no-no-no' not found"),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRef(
                            parameterValidation.getName(),
                            GetNamedRefsParams.builder()
                                .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                                .baseReference(TagName.of("blah-no"))
                                .build()))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage("Named reference 'blah-no' not found"),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.namedRef(
                            parameterValidationTag.getName(),
                            GetNamedRefsParams.builder()
                                .tagRetrieveOptions(RetrieveOptions.OMIT)
                                .build()))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage(
                    "Named reference '" + parameterValidationTag.getName() + "' not found"));
  }

  @Test
  public void fromNoAncestor() throws Exception {
    BranchName main = BranchName.of(MAIN_BRANCH);
    BranchName branch = BranchName.of("fromNoAncestorBranch");
    BranchName branch2 = BranchName.of("fromNoAncestorBranch2");
    BranchName branch3 = BranchName.of("fromNoAncestorBranch3");
    TagName tag = TagName.of("fromNoAncestorTag");
    TagName tag2 = TagName.of("fromNoAncestorTag2");
    TagName tag3 = TagName.of("fromNoAncestorTag3");

    Hash hash = databaseAdapter.noAncestorHash();
    Hash mainHash = hash;
    Hash branchHash = hash;

    assertThat(databaseAdapter.hashOnReference(main, Optional.empty())).isEqualTo(hash);

    // Have 'main' + a branch + a tag
    //  all point to the "no ancestor" hash (aka "beginning of time")

    databaseAdapter.create(branch, hash);
    databaseAdapter.create(tag, hash);

    verifyReferences(
        new ExpectedNamedReference(main, mainHash, 0, 0, hash, null),
        new ExpectedNamedReference(branch, branchHash, 0, 0, hash, null),
        new ExpectedNamedReference(tag, hash, 0, 0, hash, null));

    // Add 100 commits to 'main'

    for (int i = 0; i < 100; i++) {
      mainHash = dummyCommit(main, mainHash, i + 1);
    }

    // Expect a commit-metadata for 'main', branch+tag are then 100 commits behind.

    verifyReferences(
        new ExpectedNamedReference(main, mainHash, 0, 0, hash, commitMetaFor(main, 100)),
        new ExpectedNamedReference(branch, branchHash, 0, 100, hash, null),
        new ExpectedNamedReference(tag, hash, 0, 100, hash, null));

    // Add 42 commits to branch

    for (int i = 0; i < 42; i++) {
      branchHash = dummyCommit(branch, branchHash, i + 1);
    }

    // same expectations as above, but branch is now also 42 commits ahead

    verifyReferences(
        new ExpectedNamedReference(main, mainHash, 0, 0, hash, commitMetaFor(main, 100)),
        new ExpectedNamedReference(branch, branchHash, 42, 100, hash, commitMetaFor(branch, 42)),
        new ExpectedNamedReference(tag, hash, 0, 100, hash, null));

    // create a branch2 + tag2 from 'main'

    Hash main100 = mainHash;
    Hash branch2Hash = databaseAdapter.create(branch2, main100);
    Hash tag2Hash = databaseAdapter.create(tag2, main100);

    // same expectations as above, but include branch2 + tag2
    // - common ancestor of branch2 + tag2 is the 100th commit on 'main'

    verifyReferences(
        new ExpectedNamedReference(main, mainHash, 0, 0, hash, commitMetaFor(main, 100)),
        new ExpectedNamedReference(branch, branchHash, 42, 100, hash, commitMetaFor(branch, 42)),
        new ExpectedNamedReference(tag, hash, 0, 100, hash, null),
        new ExpectedNamedReference(branch2, branch2Hash, 0, 0, main100, commitMetaFor(main, 100)),
        new ExpectedNamedReference(tag2, tag2Hash, 0, 0, main100, commitMetaFor(main, 100)));

    // add 900 commits to 'main'

    for (int i = 100; i < 1000; i++) {
      mainHash = dummyCommit(main, mainHash, i + 1);
    }

    // similar to the above, but:
    // - branch + tag are now 100+900 = 1000 commits behind

    verifyReferences(
        new ExpectedNamedReference(main, mainHash, 0, 0, hash, commitMetaFor(main, 1000)),
        new ExpectedNamedReference(branch, branchHash, 42, 1000, hash, commitMetaFor(branch, 42)),
        new ExpectedNamedReference(tag, hash, 0, 1000, hash, null),
        new ExpectedNamedReference(branch2, branch2Hash, 0, 900, main100, commitMetaFor(main, 100)),
        new ExpectedNamedReference(tag2, tag2Hash, 0, 900, main100, commitMetaFor(main, 100)));

    // add 42 commits to branch2

    for (int i = 0; i < 42; i++) {
      branch2Hash = dummyCommit(branch2, branch2Hash, i + 1);
    }

    // similar to the above, but:
    // - branch2 is now also 42 commits ahead

    verifyReferences(
        new ExpectedNamedReference(main, mainHash, 0, 0, hash, commitMetaFor(main, 1000)),
        new ExpectedNamedReference(branch, branchHash, 42, 1000, hash, commitMetaFor(branch, 42)),
        new ExpectedNamedReference(tag, hash, 0, 1000, hash, null),
        new ExpectedNamedReference(
            branch2, branch2Hash, 42, 900, main100, commitMetaFor(branch2, 42)),
        new ExpectedNamedReference(tag2, tag2Hash, 0, 900, main100, commitMetaFor(main, 100)));

    // Create branch2+tag3 at branch2

    Hash branch2plus42 = branch2Hash;
    Hash branch3Hash = databaseAdapter.create(branch3, branch2plus42);
    Hash tag3Hash = databaseAdapter.create(tag3, branch2plus42);

    // Add 42 commits to branch3

    for (int i = 0; i < 42; i++) {
      branch3Hash = dummyCommit(branch3, branch3Hash, i + 1);
    }

    // similar to the above, but:
    // - tag3 is 42 commits ahead
    // - branch3 is 84 commits ahead

    verifyReferences(
        new ExpectedNamedReference(main, mainHash, 0, 0, hash, commitMetaFor(main, 1000)),
        new ExpectedNamedReference(branch, branchHash, 42, 1000, hash, commitMetaFor(branch, 42)),
        new ExpectedNamedReference(tag, hash, 0, 1000, hash, null),
        new ExpectedNamedReference(
            branch2, branch2Hash, 42, 900, main100, commitMetaFor(branch2, 42)),
        new ExpectedNamedReference(tag2, tag2Hash, 0, 900, main100, commitMetaFor(main, 100)),
        new ExpectedNamedReference(
            branch3, branch3Hash, 84, 900, main100, commitMetaFor(branch3, 42)),
        new ExpectedNamedReference(tag3, tag3Hash, 42, 900, main100, commitMetaFor(branch2, 42)));
  }

  private ByteString commitMetaFor(NamedRef ref, int num) {
    return ByteString.copyFromUtf8("dummy commit " + ref.getName() + " " + num);
  }

  private Hash dummyCommit(BranchName branch, Hash expectedHash, int num) throws Exception {
    return databaseAdapter.commit(
        ImmutableCommitAttempt.builder()
            .commitMetaSerialized(commitMetaFor(branch, num))
            .commitToBranch(branch)
            .expectedHead(Optional.of(expectedHash))
            .addPuts(
                KeyWithBytes.of(
                    SOME_KEY,
                    ContentId.of(SOME_CONTENT_ID),
                    (byte) 42,
                    ByteString.copyFromUtf8("dummy content")))
            .build());
  }

  private void verifyReferences(ExpectedNamedReference... references) {
    assertAll(
        () -> verifyReferences(GetNamedRefsParams.builder().build(), references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .branchRetrieveOptions(RetrieveOptions.COMMIT_META)
                    .tagRetrieveOptions(RetrieveOptions.COMMIT_META)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .branchRetrieveOptions(RetrieveOptions.COMMIT_META)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .tagRetrieveOptions(RetrieveOptions.COMMIT_META)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                    .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                    .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .branchRetrieveOptions(COMPUTE_ALL)
                    .tagRetrieveOptions(COMPUTE_ALL)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .tagRetrieveOptions(COMPUTE_ALL)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder()
                    .baseReference(BranchName.of(MAIN_BRANCH))
                    .branchRetrieveOptions(COMPUTE_ALL)
                    .build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder().branchRetrieveOptions(RetrieveOptions.OMIT).build(),
                references),
        () ->
            verifyReferences(
                GetNamedRefsParams.builder().tagRetrieveOptions(RetrieveOptions.OMIT).build(),
                references));
  }

  private void verifyReferences(GetNamedRefsParams params, ExpectedNamedReference... references)
      throws ReferenceNotFoundException {
    List<ReferenceInfo<ByteString>> expectedRefs =
        Arrays.stream(references)
            .map(expectedRef -> expectedRef.expected(params))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    try (Stream<ReferenceInfo<ByteString>> refs = databaseAdapter.namedRefs(params)) {
      assertThat(refs)
          .describedAs("GetNamedRefsParams=%s - references=%s", params, references)
          .containsExactlyInAnyOrderElementsOf(expectedRefs);
    }

    for (ReferenceInfo<ByteString> expected : expectedRefs) {
      assertThat(databaseAdapter.namedRef(expected.getNamedRef().getName(), params))
          .isEqualTo(expected);
    }

    List<NamedRef> failureRefs =
        Arrays.stream(references)
            .map(
                expectedRef -> {
                  ReferenceInfo<ByteString> expected = expectedRef.expected(params);
                  if (expected == null) {
                    return expectedRef.ref;
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    for (NamedRef ref : failureRefs) {
      assertThatThrownBy(() -> databaseAdapter.namedRef(ref.getName(), params));
    }
  }

  static class ExpectedNamedReference {
    final NamedRef ref;
    final Hash hash;
    final CommitsAheadBehind aheadBehind;
    final Hash commonAncestor;
    final ByteString commitMeta;

    ExpectedNamedReference(
        NamedRef ref,
        Hash hash,
        int ahead,
        int behind,
        Hash commonAncestor,
        ByteString commitMeta) {
      this.ref = ref;
      this.hash = hash;
      this.aheadBehind = CommitsAheadBehind.of(ahead, behind);
      this.commonAncestor = commonAncestor;
      this.commitMeta = commitMeta;
    }

    ReferenceInfo<ByteString> expected(GetNamedRefsParams params) {
      RetrieveOptions opts;
      if (ref instanceof TagName) {
        opts = params.getTagRetrieveOptions();
      } else if (ref instanceof BranchName) {
        opts = params.getBranchRetrieveOptions();
      } else {
        throw new IllegalArgumentException("" + ref);
      }

      if (!opts.isRetrieve()) {
        return null;
      }

      ImmutableReferenceInfo.Builder<ByteString> builder =
          ReferenceInfo.<ByteString>builder().namedRef(ref).hash(hash);
      if (!ref.equals(params.getBaseReference())) {
        if (opts.isComputeAheadBehind()) {
          builder.aheadBehind(aheadBehind);
        }
        if (opts.isComputeCommonAncestor()) {
          builder.commonAncestor(commonAncestor);
        }
      }
      if (opts.isRetrieveCommitMetaForHead()) {
        builder.headCommitMeta(commitMeta);
      }
      return builder.build();
    }

    @Override
    public String toString() {
      return "ExpectedNamedReference{"
          + "ref="
          + ref
          + ", hash="
          + hash
          + ", aheadBehind="
          + aheadBehind
          + ", commonAncestor="
          + commonAncestor
          + '}';
    }
  }
}
