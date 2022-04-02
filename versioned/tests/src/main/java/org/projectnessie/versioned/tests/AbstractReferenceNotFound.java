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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.versioned.testworker.CommitMessage.commitMessage;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;

public abstract class AbstractReferenceNotFound extends AbstractNestedVersionStore {
  protected AbstractReferenceNotFound(
      VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  private static class ReferenceNotFoundFunction {

    final String name;
    String msg;
    ThrowingFunction setup;
    ThrowingFunction function;

    ReferenceNotFoundFunction(String name) {
      this.name = name;
    }

    ReferenceNotFoundFunction setup(ThrowingFunction setup) {
      this.setup = setup;
      return this;
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
      void run(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store)
          throws VersionStoreException;
    }
  }

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
            .function(s -> s.getValue(BranchName.of("this-one-should-not-exist"), Key.of("foo"))),
        new ReferenceNotFoundFunction("getValue/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getValue(TagName.of("this-one-should-not-exist"), Key.of("foo"))),
        new ReferenceNotFoundFunction("getValue/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getValue(
                        Hash.of("12341234123412341234123412341234123412341234"), Key.of("foo"))),
        // getValues()
        new ReferenceNotFoundFunction("getValues/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        BranchName.of("this-one-should-not-exist"), singletonList(Key.of("foo")))),
        new ReferenceNotFoundFunction("getValues/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        TagName.of("this-one-should-not-exist"), singletonList(Key.of("foo")))),
        new ReferenceNotFoundFunction("getValues/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getValues(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        singletonList(Key.of("foo")))),
        // getKeys()
        new ReferenceNotFoundFunction("getKeys/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getKeys(BranchName.of("this-one-should-not-exist"))),
        new ReferenceNotFoundFunction("getKeys/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getKeys(TagName.of("this-one-should-not-exist"))),
        new ReferenceNotFoundFunction("getKeys/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(s -> s.getKeys(Hash.of("12341234123412341234123412341234123412341234"))),
        // assign()
        new ReferenceNotFoundFunction("assign/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        s.noAncestorHash())),
        new ReferenceNotFoundFunction("assign/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("main"),
                        Optional.empty(),
                        Hash.of("12341234123412341234123412341234123412341234"))),
        // delete()
        new ReferenceNotFoundFunction("delete/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.delete(BranchName.of("this-one-should-not-exist"), Optional.empty())),
        new ReferenceNotFoundFunction("delete/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.delete(BranchName.of("this-one-should-not-exist"), Optional.empty())),
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
                        commitMessage("meta"),
                        singletonList(Delete.of(Key.of("meep"))))),
        new ReferenceNotFoundFunction("commit/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.commit(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        commitMessage("meta"),
                        singletonList(Delete.of(Key.of("meep"))))),
        // transplant()
        new ReferenceNotFoundFunction("transplant/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        singletonList(s.hashOnReference(BranchName.of("main"), Optional.empty())),
                        l -> l.get(0),
                        false)),
        new ReferenceNotFoundFunction("transplant/hash/empty")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        singletonList(Hash.of("12341234123412341234123412341234123412341234")),
                        l -> l.get(0),
                        true)),
        new ReferenceNotFoundFunction("transplant/empty/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.empty(),
                        singletonList(Hash.of("12341234123412341234123412341234123412341234")),
                        l -> l.get(0),
                        false)),
        // merge()
        new ReferenceNotFoundFunction("merge/hash/empty")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.merge(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        BranchName.of("main"),
                        Optional.empty(),
                        l -> l.get(0),
                        true)),
        new ReferenceNotFoundFunction("merge/empty/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.merge(
                        s.noAncestorHash(),
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        l -> l.get(0),
                        true)));
  }

  @ParameterizedTest
  @MethodSource("referenceNotFoundFunctions")
  void referenceNotFound(ReferenceNotFoundFunction f) throws Exception {
    if (f.setup != null) {
      f.setup.run(store());
    }
    assertThatThrownBy(() -> f.function.run(store()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage(f.msg);
  }
}
