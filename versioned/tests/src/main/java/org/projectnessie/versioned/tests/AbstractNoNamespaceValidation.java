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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.MergeOp;
import org.projectnessie.versioned.VersionStore.TransplantOp;

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
  @CsvSource({"false,false", "false,true", "true,true"})
  void mergeTransplant(boolean merge, boolean individual) throws Exception {
    BranchName root = BranchName.of("root");
    BranchName branch = BranchName.of("branch");
    store().create(root, Optional.empty());

    CommitResult rootHead =
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

    Hash commit1 = store().hashOnReference(branch, Optional.empty(), emptyList());

    soft.assertThatCode(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        CommitMeta.fromMessage("commit"),
                        singletonList(Put.of(ContentKey.of("another", "table"), newOnRef("bar")))))
        .doesNotThrowAnyException();

    Hash commit2 = store().hashOnReference(branch, Optional.empty(), emptyList());

    soft.assertThatCode(
            () -> {
              if (merge) {
                store()
                    .merge(
                        MergeOp.builder().fromRef(branch).fromHash(commit2).toBranch(root).build());
              } else {
                store()
                    .transplant(
                        TransplantOp.builder()
                            .fromRef(branch)
                            .toBranch(root)
                            .addSequenceToTransplant(commit1, commit2)
                            .build());
              }
            })
        .doesNotThrowAnyException();
  }
}
