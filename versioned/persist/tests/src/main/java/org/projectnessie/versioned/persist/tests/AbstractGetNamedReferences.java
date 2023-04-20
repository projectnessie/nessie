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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableReferenceInfo;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceInfo.CommitsAheadBehind;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractGetNamedReferences {

  public static final String MAIN_BRANCH = "main";
  private static final ContentKey SOME_KEY = ContentKey.of("abc");
  private static final String SOME_CONTENT_ID = "abc";
  private final DatabaseAdapter databaseAdapter;

  static RetrieveOptions COMPUTE_AHEAD_BEHIND =
      RetrieveOptions.builder().isComputeAheadBehind(true).build();
  static RetrieveOptions COMPUTE_COMMON_ANCESTOR =
      RetrieveOptions.builder().isComputeAheadBehind(true).build();
  static RetrieveOptions COMPUTE_ALL =
      RetrieveOptions.builder().isComputeAheadBehind(true).isComputeCommonAncestor(true).build();

  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractGetNamedReferences(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  public void parameterValidationNamedRefs() throws Exception {
    BranchName main = BranchName.of(MAIN_BRANCH);
    BranchName parameterValidation = BranchName.of("parameterValidation");
    TagName parameterValidationTag = TagName.of("parameterValidationTag");

    Hash hash = databaseAdapter.noAncestorHash();

    soft.assertThat(databaseAdapter.hashOnReference(main, Optional.empty())).isEqualTo(hash);

    databaseAdapter.create(parameterValidation, hash);
    databaseAdapter.create(parameterValidationTag, hash);

    soft.assertThatThrownBy(() -> databaseAdapter.namedRefs(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Parameter for GetNamedRefsParams must not be null.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRefs(
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRefs(
                    GetNamedRefsParams.builder().tagRetrieveOptions(COMPUTE_AHEAD_BEHIND).build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRefs(
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                        .build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRefs(
                    GetNamedRefsParams.builder()
                        .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                        .build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRefs(
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .baseReference(BranchName.of("no-no-no"))
                        .build()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage("Named reference 'no-no-no' not found");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRefs(
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .baseReference(TagName.of("blah-no"))
                        .build()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage("Named reference 'blah-no' not found");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRefs(
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(RetrieveOptions.OMIT)
                        .tagRetrieveOptions(RetrieveOptions.OMIT)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must retrieve branches or tags or both.");
  }

  @Test
  public void parameterValidationNamedRef() throws Exception {
    BranchName main = BranchName.of(MAIN_BRANCH);
    BranchName parameterValidation = BranchName.of("parameterValidation");
    TagName parameterValidationTag = TagName.of("parameterValidationTag");

    Hash hash = databaseAdapter.noAncestorHash();

    soft.assertThat(databaseAdapter.hashOnReference(main, Optional.empty())).isEqualTo(hash);

    databaseAdapter.create(parameterValidation, hash);
    databaseAdapter.create(parameterValidationTag, hash);

    soft.assertThatThrownBy(() -> databaseAdapter.namedRef(main.getName(), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Parameter for GetNamedRefsParams must not be null");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRef(
                    parameterValidation.getName(),
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRef(
                    parameterValidation.getName(),
                    GetNamedRefsParams.builder().tagRetrieveOptions(COMPUTE_AHEAD_BEHIND).build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRef(
                    parameterValidation.getName(),
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                        .build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRef(
                    parameterValidation.getName(),
                    GetNamedRefsParams.builder()
                        .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
                        .build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Base reference name missing.");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRef(
                    parameterValidation.getName(),
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .baseReference(BranchName.of("no-no-no"))
                        .build()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage("Named reference 'no-no-no' not found");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRef(
                    parameterValidation.getName(),
                    GetNamedRefsParams.builder()
                        .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
                        .baseReference(TagName.of("blah-no"))
                        .build()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage("Named reference 'blah-no' not found");
    soft.assertThatThrownBy(
            () ->
                databaseAdapter.namedRef(
                    parameterValidationTag.getName(),
                    GetNamedRefsParams.builder().tagRetrieveOptions(RetrieveOptions.OMIT).build()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage("Named reference '" + parameterValidationTag.getName() + "' not found");
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
    Hash mainParent = null;
    Hash branchHash = hash;
    Hash branchParent = null;
    Hash tagParent = null;

    soft.assertThat(databaseAdapter.hashOnReference(main, Optional.empty())).isEqualTo(hash);

    // Have 'main' + a branch + a tag
    //  all point to the "no ancestor" hash (aka "beginning of time")

    databaseAdapter.create(branch, hash);
    databaseAdapter.create(tag, hash);

    verifyReferences(
        new ExpectedNamedReference(main, 0L, mainHash, mainParent, 0, 0, hash, null),
        new ExpectedNamedReference(branch, 0L, branchHash, branchParent, 0, 0, hash, null),
        new ExpectedNamedReference(tag, 0L, hash, tagParent, 0, 0, hash, null));

    // Add 10 commits to 'main'

    for (int i = 0; i < 10; i++) {
      mainParent = mainHash;
      mainHash = dummyCommit(main, mainHash, i + 1);
    }

    // Expect a commit-metadata for 'main', branch+tag are then 100 commits behind.

    verifyReferences(
        new ExpectedNamedReference(
            main, 10, mainHash, mainParent, 0, 0, hash, commitMetaFor(main, 10)),
        new ExpectedNamedReference(branch, 0, branchHash, branchParent, 0, 10, hash, null),
        new ExpectedNamedReference(tag, 0, hash, tagParent, 0, 10, hash, null));

    // Add 4 commits to branch

    for (int i = 0; i < 4; i++) {
      branchParent = branchHash;
      branchHash = dummyCommit(branch, branchHash, i + 1);
    }

    // same expectations as above, but branch is now also 42 commits ahead

    verifyReferences(
        new ExpectedNamedReference(
            main, 10, mainHash, mainParent, 0, 0, hash, commitMetaFor(main, 10)),
        new ExpectedNamedReference(
            branch, 4, branchHash, branchParent, 4, 10, hash, commitMetaFor(branch, 4)),
        new ExpectedNamedReference(tag, 0, hash, tagParent, 0, 10, hash, null));

    // create a branch2 + tag2 from 'main'

    Hash main10 = mainHash;
    Hash branch2Parent = mainParent;
    Hash branch2Hash = databaseAdapter.create(branch2, main10).getHash();
    Hash tag2Parent = mainParent;
    Hash tag2Hash = databaseAdapter.create(tag2, main10).getHash();

    // same expectations as above, but include branch2 + tag2
    // - common ancestor of branch2 + tag2 is the 100th commit on 'main'

    verifyReferences(
        new ExpectedNamedReference(
            main, 10, mainHash, mainParent, 0, 0, hash, commitMetaFor(main, 10)),
        new ExpectedNamedReference(
            branch, 4, branchHash, branchParent, 4, 10, hash, commitMetaFor(branch, 4)),
        new ExpectedNamedReference(tag, 0, hash, tagParent, 0, 10, hash, null),
        new ExpectedNamedReference(
            branch2, 10, branch2Hash, branch2Parent, 0, 0, main10, commitMetaFor(main, 10)),
        new ExpectedNamedReference(
            tag2, 10, tag2Hash, tag2Parent, 0, 0, main10, commitMetaFor(main, 10)));

    // add 10 more commits to 'main'

    for (int i = 10; i < 20; i++) {
      mainParent = mainHash;
      mainHash = dummyCommit(main, mainHash, i + 1);
    }

    // similar to the above, but:
    // - branch + tag are now 100+900 = 1000 commits behind

    verifyReferences(
        new ExpectedNamedReference(
            main, 20, mainHash, mainParent, 0, 0, hash, commitMetaFor(main, 20)),
        new ExpectedNamedReference(
            branch, 4, branchHash, branchParent, 4, 20, hash, commitMetaFor(branch, 4)),
        new ExpectedNamedReference(tag, 0, hash, tagParent, 0, 20, hash, null),
        new ExpectedNamedReference(
            branch2, 10, branch2Hash, branch2Parent, 0, 10, main10, commitMetaFor(main, 10)),
        new ExpectedNamedReference(
            tag2, 10, tag2Hash, tag2Parent, 0, 10, main10, commitMetaFor(main, 10)));

    // add 4 commits to branch2

    for (int i = 0; i < 4; i++) {
      branch2Parent = branch2Hash;
      branch2Hash = dummyCommit(branch2, branch2Hash, i + 1);
    }

    // similar to the above, but:
    // - branch2 is now also 42 commits ahead

    verifyReferences(
        new ExpectedNamedReference(
            main, 20, mainHash, mainParent, 0, 0, hash, commitMetaFor(main, 20)),
        new ExpectedNamedReference(
            branch, 4, branchHash, branchParent, 4, 20, hash, commitMetaFor(branch, 4)),
        new ExpectedNamedReference(tag, 0, hash, tagParent, 0, 20, hash, null),
        new ExpectedNamedReference(
            branch2, 14, branch2Hash, branch2Parent, 4, 10, main10, commitMetaFor(branch2, 4)),
        new ExpectedNamedReference(
            tag2, 10, tag2Hash, tag2Parent, 0, 10, main10, commitMetaFor(main, 10)));

    // Create branch2+tag3 at branch2

    Hash branch2plus42 = branch2Hash;
    Hash branch3Hash = databaseAdapter.create(branch3, branch2plus42).getHash();
    Hash branch3Parent = branch2Parent;
    Hash tag3Hash = databaseAdapter.create(tag3, branch2plus42).getHash();
    Hash tag3Parent = branch2Parent;

    // Add 42 commits to branch3

    for (int i = 0; i < 4; i++) {
      branch3Parent = branch3Hash;
      branch3Hash = dummyCommit(branch3, branch3Hash, i + 1);
    }

    // similar to the above, but:
    // - tag3 is 42 commits ahead
    // - branch3 is 84 commits ahead

    verifyReferences(
        new ExpectedNamedReference(
            main, 20, mainHash, mainParent, 0, 0, hash, commitMetaFor(main, 20)),
        new ExpectedNamedReference(
            branch, 4, branchHash, branchParent, 4, 20, hash, commitMetaFor(branch, 4)),
        new ExpectedNamedReference(tag, 0, hash, tagParent, 0, 20, hash, null),
        new ExpectedNamedReference(
            branch2, 14, branch2Hash, branch2Parent, 4, 10, main10, commitMetaFor(branch2, 4)),
        new ExpectedNamedReference(
            tag2, 10, tag2Hash, tag2Parent, 0, 10, main10, commitMetaFor(main, 10)),
        new ExpectedNamedReference(
            branch3, 18, branch3Hash, branch3Parent, 8, 10, main10, commitMetaFor(branch3, 4)),
        new ExpectedNamedReference(
            tag3, 14, tag3Hash, tag3Parent, 4, 10, main10, commitMetaFor(branch2, 4)));
  }

  private ByteString commitMetaFor(NamedRef ref, int num) {
    return ByteString.copyFromUtf8("dummy commit " + ref.getName() + " " + num);
  }

  private Hash dummyCommit(BranchName branch, Hash expectedHash, int num) throws Exception {
    return databaseAdapter
        .commit(
            ImmutableCommitParams.builder()
                .commitMetaSerialized(commitMetaFor(branch, num))
                .toBranch(branch)
                .expectedHead(Optional.of(expectedHash))
                .addPuts(
                    KeyWithBytes.of(
                        SOME_KEY,
                        ContentId.of(SOME_CONTENT_ID),
                        (byte) 42,
                        ByteString.copyFromUtf8("dummy content")))
                .build())
        .getCommit()
        .getHash();
  }

  private void verifyReferences(ExpectedNamedReference... references)
      throws ReferenceNotFoundException {
    verifyReferences(GetNamedRefsParams.builder().build(), references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .branchRetrieveOptions(RetrieveOptions.COMMIT_META)
            .tagRetrieveOptions(RetrieveOptions.COMMIT_META)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder().branchRetrieveOptions(RetrieveOptions.COMMIT_META).build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder().tagRetrieveOptions(RetrieveOptions.COMMIT_META).build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
            .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .branchRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .tagRetrieveOptions(COMPUTE_COMMON_ANCESTOR)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
            .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .branchRetrieveOptions(COMPUTE_AHEAD_BEHIND)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .tagRetrieveOptions(COMPUTE_AHEAD_BEHIND)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .branchRetrieveOptions(COMPUTE_ALL)
            .tagRetrieveOptions(COMPUTE_ALL)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .tagRetrieveOptions(COMPUTE_ALL)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(MAIN_BRANCH))
            .branchRetrieveOptions(COMPUTE_ALL)
            .build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder().branchRetrieveOptions(RetrieveOptions.OMIT).build(),
        references);
    verifyReferences(
        GetNamedRefsParams.builder().tagRetrieveOptions(RetrieveOptions.OMIT).build(), references);
  }

  private void verifyReferences(GetNamedRefsParams params, ExpectedNamedReference... references)
      throws ReferenceNotFoundException {
    List<ReferenceInfo<ByteString>> expectedRefs =
        Arrays.stream(references)
            .map(expectedRef -> expectedRef.expected(params))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    try (Stream<ReferenceInfo<ByteString>> refs = databaseAdapter.namedRefs(params)) {
      soft.assertThat(refs)
          .describedAs("GetNamedRefsParams=%s - references=%s", params, references)
          .containsExactlyInAnyOrderElementsOf(expectedRefs);
    }

    for (ReferenceInfo<ByteString> expected : expectedRefs) {
      soft.assertThat(databaseAdapter.namedRef(expected.getNamedRef().getName(), params))
          .describedAs("GetNamedRefsParams=%s - references=%s", params, references)
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
      soft.assertThatThrownBy(() -> databaseAdapter.namedRef(ref.getName(), params))
          .describedAs("GetNamedRefsParams=%s - references=%s", params, references);
    }

    soft.assertAll();
  }

  static class ExpectedNamedReference {
    final NamedRef ref;
    final long commitSeq;
    final Hash hash;
    final Hash parentHash;
    final CommitsAheadBehind aheadBehind;
    final Hash commonAncestor;
    final ByteString commitMeta;

    ExpectedNamedReference(
        NamedRef ref,
        long commitSeq,
        Hash hash,
        Hash parentHash,
        int ahead,
        int behind,
        Hash commonAncestor,
        ByteString commitMeta) {
      this.ref = ref;
      this.commitSeq = commitSeq;
      this.hash = hash;
      this.parentHash = parentHash;
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
        builder.headCommitMeta(commitMeta).commitSeq(commitSeq);
        if (parentHash != null) {
          builder.addParentHashes(parentHash);
        }
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
