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
package org.projectnessie.versioned.tests;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.projectnessie.versioned.tests.AbstractVersionStoreTestBase.METADATA_REWRITER;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.VersionStore;

/** Verifies that namespace validation, if disabled, is not effective. */
@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractNoNamespaceValidation {

  @InjectSoftAssertions protected SoftAssertions soft;

  protected abstract VersionStore store();

  @Test
  void commit() throws Exception {
    BranchName branch = BranchName.of("noNamespaceValidation");
    store().create(branch, Optional.empty());
    soft.assertThatCode(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("commit"),
                        singletonList(
                            Put.of(ContentKey.of("name", "spaced", "table"), newOnRef("foo")))))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void mergeTransplant(boolean merge, boolean individual) throws Exception {
    BranchName root = BranchName.of("root");
    BranchName branch = BranchName.of("branch");
    store().create(root, Optional.empty());

    CommitResult<Commit> rootHead =
        store()
            .commit(
                root,
                Optional.empty(),
                CommitMeta.fromMessage("common ancestor"),
                singletonList(Put.of(ContentKey.of("dummy"), newOnRef("dummy"))));

    store().create(branch, Optional.of(rootHead.getCommitHash()));

    soft.assertThatCode(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("commit"),
                        singletonList(
                            Put.of(ContentKey.of("name", "spaced", "table"), newOnRef("foo")))))
        .doesNotThrowAnyException();

    Hash commit1 = store().hashOnReference(branch, Optional.empty());

    soft.assertThatCode(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("commit"),
                        singletonList(Put.of(ContentKey.of("another", "table"), newOnRef("bar")))))
        .doesNotThrowAnyException();

    Hash commit2 = store().hashOnReference(branch, Optional.empty());

    soft.assertThatCode(
            () -> {
              if (merge) {
                store()
                    .merge(
                        branch,
                        commit2,
                        root,
                        Optional.empty(),
                        METADATA_REWRITER,
                        individual,
                        emptyMap(),
                        MergeBehavior.NORMAL,
                        false,
                        false);
              } else {
                store()
                    .transplant(
                        branch,
                        root,
                        Optional.empty(),
                        asList(commit1, commit2),
                        METADATA_REWRITER,
                        individual,
                        emptyMap(),
                        MergeBehavior.NORMAL,
                        false,
                        false);
              }
            })
        .doesNotThrowAnyException();
  }
}
