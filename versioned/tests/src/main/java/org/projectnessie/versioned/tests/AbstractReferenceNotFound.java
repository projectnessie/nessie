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
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.MergeOp;
import org.projectnessie.versioned.VersionStore.TransplantOp;
import org.projectnessie.versioned.VersionStoreException;

public abstract class AbstractReferenceNotFound extends AbstractNestedVersionStore {
  protected AbstractReferenceNotFound(VersionStore store) {
    super(store);
  }

  private static class ReferenceNotFoundFunction {

    final String name;
    String msg;
    ThrowingFunction function;

    ReferenceNotFoundFunction(String name) {
      this.name = name;
    }

    ReferenceNotFoundFunction function(ThrowingFunction function) {
      this.function = function;
      return this;
    }

    ReferenceNotFoundFunction msg(String msg) {
      this.msg = msg;
      return this;
    }

    @Override
    public String toString() {
      return name;
    }

    @FunctionalInterface
    interface ThrowingFunction {
      void run(VersionStore store) throws VersionStoreException;
    }
  }

  @SuppressWarnings("MustBeClosedChecker")
  static List<ReferenceNotFoundFunction> referenceNotFoundFunctions() {
    return Arrays.asList(
        // getCommits()
        new ReferenceNotFoundFunction("getCommits/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getCommits(BranchName.of("this-one-should-not-exist"), false)),
        new ReferenceNotFoundFunction("getCommits/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getCommits(TagName.of("this-one-should-not-exist"), false)),
        new ReferenceNotFoundFunction("getCommits/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s -> s.getCommits(Hash.of("12341234123412341234123412341234123412341234"), false)),
        // getValue()
        new ReferenceNotFoundFunction("getValue/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValue(
                        BranchName.of("this-one-should-not-exist"), ContentKey.of("foo"), false)),
        new ReferenceNotFoundFunction("getValue/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValue(
                        TagName.of("this-one-should-not-exist"), ContentKey.of("foo"), false)),
        new ReferenceNotFoundFunction("getValue/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getValue(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        ContentKey.of("foo"),
                        false)),
        // getValues()
        new ReferenceNotFoundFunction("getValues/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        BranchName.of("this-one-should-not-exist"),
                        singletonList(ContentKey.of("foo")),
                        false)),
        new ReferenceNotFoundFunction("getValues/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        TagName.of("this-one-should-not-exist"),
                        singletonList(ContentKey.of("foo")),
                        false)),
        new ReferenceNotFoundFunction("getValues/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getValues(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        singletonList(ContentKey.of("foo")),
                        false)),
        // getKeys()
        new ReferenceNotFoundFunction("getKeys/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getKeys(
                        BranchName.of("this-one-should-not-exist"),
                        null,
                        false,
                        NO_KEY_RESTRICTIONS)),
        new ReferenceNotFoundFunction("getKeys/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getKeys(
                        TagName.of("this-one-should-not-exist"), null, false, NO_KEY_RESTRICTIONS)),
        new ReferenceNotFoundFunction("getKeys/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getKeys(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        null,
                        false,
                        NO_KEY_RESTRICTIONS)),
        // assign()
        new ReferenceNotFoundFunction("assign/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("this-one-should-not-exist"),
                        s.noAncestorHash(),
                        s.noAncestorHash())),
        new ReferenceNotFoundFunction("assign/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("main"),
                        s.noAncestorHash(),
                        Hash.of("12341234123412341234123412341234123412341234"))),
        // delete()
        new ReferenceNotFoundFunction("delete/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s -> s.delete(BranchName.of("this-one-should-not-exist"), s.noAncestorHash())),
        new ReferenceNotFoundFunction("delete/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s -> s.delete(BranchName.of("this-one-should-not-exist"), s.noAncestorHash())),
        // create()
        new ReferenceNotFoundFunction("create/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.create(
                        BranchName.of("foo"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")))),
        // commit()
        new ReferenceNotFoundFunction("commit/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.commit(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        CommitMeta.fromMessage("meta"),
                        singletonList(Delete.of(ContentKey.of("meep"))))),
        new ReferenceNotFoundFunction("commit/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.commit(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        CommitMeta.fromMessage("meta"),
                        singletonList(Delete.of(ContentKey.of("meep"))))),
        // transplant()
        new ReferenceNotFoundFunction("transplant/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.transplant(
                        TransplantOp.builder()
                            .fromRef(BranchName.of("source"))
                            .toBranch(BranchName.of("this-one-should-not-exist"))
                            .addSequenceToTransplant(
                                s.hashOnReference(
                                    BranchName.of("main"), Optional.empty(), emptyList()))
                            .build())),
        new ReferenceNotFoundFunction("transplant/hash/empty")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.transplant(
                        TransplantOp.builder()
                            .fromRef(BranchName.of("source"))
                            .toBranch(BranchName.of("main"))
                            .expectedHash(
                                Optional.of(
                                    Hash.of("12341234123412341234123412341234123412341234")))
                            .addSequenceToTransplant(
                                Hash.of("12341234123412341234123412341234123412341234"))
                            .build())),
        new ReferenceNotFoundFunction("transplant/empty/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.transplant(
                        TransplantOp.builder()
                            .fromRef(BranchName.of("source"))
                            .toBranch(BranchName.of("main"))
                            .addSequenceToTransplant(
                                Hash.of("12341234123412341234123412341234123412341234"))
                            .build())),
        // diff()
        new ReferenceNotFoundFunction("diff/from-hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getDiffs(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        BranchName.of("main"),
                        null,
                        NO_KEY_RESTRICTIONS)),
        new ReferenceNotFoundFunction("diff/to-branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getDiffs(
                        BranchName.of("main"),
                        BranchName.of("this-one-should-not-exist"),
                        null,
                        NO_KEY_RESTRICTIONS)),
        new ReferenceNotFoundFunction("diff/from-hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getDiffs(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        BranchName.of("main"),
                        null,
                        NO_KEY_RESTRICTIONS)),
        new ReferenceNotFoundFunction("diff/from-branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getDiffs(
                        BranchName.of("this-one-should-not-exist"),
                        BranchName.of("main"),
                        null,
                        NO_KEY_RESTRICTIONS)),
        // merge()
        new ReferenceNotFoundFunction("merge/hash/empty")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.merge(
                        MergeOp.builder()
                            .fromRef(BranchName.of("source"))
                            .fromHash(Hash.of("12341234123412341234123412341234123412341234"))
                            .toBranch(BranchName.of("main"))
                            .build())),
        new ReferenceNotFoundFunction("merge/empty/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.merge(
                        MergeOp.builder()
                            .fromRef(BranchName.of("source"))
                            .fromHash(s.noAncestorHash())
                            .toBranch(BranchName.of("main"))
                            .expectedHash(
                                Optional.of(
                                    Hash.of("12341234123412341234123412341234123412341234")))
                            .build())),
        new ReferenceNotFoundFunction("merge/hash/empty")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.merge(
                        MergeOp.builder()
                            .fromRef(BranchName.of("source"))
                            .fromHash(Hash.of("12341234123412341234123412341234123412341234"))
                            .toBranch(BranchName.of("main"))
                            .build())),
        new ReferenceNotFoundFunction("merge/empty/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.merge(
                        MergeOp.builder()
                            .fromRef(BranchName.of("source"))
                            .fromHash(s.noAncestorHash())
                            .toBranch(BranchName.of("main"))
                            .expectedHash(
                                Optional.of(
                                    Hash.of("12341234123412341234123412341234123412341234")))
                            .build())));
  }

  @ParameterizedTest
  @MethodSource("referenceNotFoundFunctions")
  void referenceNotFound(ReferenceNotFoundFunction f) {
    assertThatThrownBy(() -> f.function.run(store()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage(f.msg);
  }
}
