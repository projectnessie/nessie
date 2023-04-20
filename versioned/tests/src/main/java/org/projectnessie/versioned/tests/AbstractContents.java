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
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractContents extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractContents(VersionStore store) {
    super(store);
  }

  @Test
  public void getValueForEmptyBranch()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty-branch");
    store().create(branch, Optional.empty());
    final Hash hash = store().hashOnReference(branch, Optional.empty());

    soft.assertThat(store().getValue(hash, ContentKey.of("arbitrary"))).isNull();
  }

  @Test
  void recreateTable() throws Exception {
    BranchName branch = BranchName.of("recreateTable-main");
    ContentKey key = ContentKey.of("recreateTable");

    store().create(branch, Optional.empty());
    // commit just something to have a "real" common ancestor and not "beginning of time", which
    // means no-common-ancestor
    Content initialState = newOnRef("value");
    CommitResult<Commit> ancestor =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("create table"),
                singletonList(Put.of(key, initialState)));
    soft.assertThat(contentWithoutId(store().getValue(branch, key))).isEqualTo(initialState);
    soft.assertThat(contentWithoutId(store().getValue(ancestor.getCommit().getHash(), key)))
        .isEqualTo(initialState);

    CommitResult<Commit> delete =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("drop table"),
                ImmutableList.of(Delete.of(key)));
    soft.assertThat(store().getValue(branch, key)).isNull();
    soft.assertThat(store().getValue(delete.getCommit().getHash(), key)).isNull();

    Content recreateState = newOnRef("value");
    CommitResult<Commit> recreate =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("drop table"),
                ImmutableList.of(Put.of(key, recreateState)));
    soft.assertThat(contentWithoutId(store().getValue(branch, key))).isEqualTo(recreateState);
    soft.assertThat(contentWithoutId(store().getValue(recreate.getCommit().getHash(), key)))
        .isEqualTo(recreateState);
  }
}
