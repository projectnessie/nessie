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

import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractDuplicateTable extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractDuplicateTable(VersionStore store) {
    super(store);
  }

  /**
   * Test behavior when a table (content-key) is created on two different branches using either
   * table-types with equal and different content-ids.
   */
  @Test
  void duplicateTableOnBranches() throws Throwable {
    ContentKey key = ContentKey.of("some-table");

    BranchName branch0 = BranchName.of("globalStateDuplicateTable-main");
    store().create(branch0, Optional.empty());
    // commit just something to have a "real" common ancestor and not "beginning of time", which
    // means no-common-ancestor
    Hash ancestor =
        store()
            .commit(
                branch0,
                Optional.empty(),
                CommitMeta.fromMessage("initial commit"),
                ImmutableList.of(Put.of(ContentKey.of("unrelated-table"), newOnRef("value"))))
            .getCommitHash();

    // Create a table with the same name on two branches.
    BranchName branch1 = BranchName.of("globalStateDuplicateTable-branch1");
    BranchName branch2 = BranchName.of("globalStateDuplicateTable-branch2");
    soft.assertThat(store().create(branch1, Optional.of(ancestor)).getHash()).isEqualTo(ancestor);
    soft.assertThat(store().create(branch2, Optional.of(ancestor)).getHash()).isEqualTo(ancestor);

    List<Operation> putForBranch1;
    List<Operation> putForBranch2;
    Content valuebranch1 = newOnRef("create table");
    Content valuebranch2 = newOnRef("create table");

    putForBranch1 = ImmutableList.of(Put.of(key, valuebranch1));
    putForBranch2 = ImmutableList.of(Put.of(key, valuebranch2));

    store()
        .commit(branch1, Optional.empty(), CommitMeta.fromMessage("create table"), putForBranch1);
    soft.assertThat(contentWithoutId(store().getValue(branch1, key, false)))
        .isEqualTo(valuebranch1);

    ThrowingCallable createTableOnOtherBranch =
        () ->
            store()
                .commit(
                    branch2,
                    Optional.empty(),
                    CommitMeta.fromMessage("create table on other branch"),
                    putForBranch2);

    createTableOnOtherBranch.call();
    soft.assertThat(contentWithoutId(store().getValue(branch2, key, false)))
        .isEqualTo(valuebranch2);
  }
}
