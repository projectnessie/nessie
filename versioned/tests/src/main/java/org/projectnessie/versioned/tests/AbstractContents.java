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
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringStoreWorker;
import org.projectnessie.versioned.StringStoreWorker.TestEnum;
import org.projectnessie.versioned.VersionStore;

public abstract class AbstractContents extends AbstractNestedVersionStore {
  protected AbstractContents(VersionStore<String, String, TestEnum> store) {
    super(store);
  }

  @Test
  public void getValueForEmptyBranch()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty-branch");
    store().create(branch, Optional.empty());
    final Hash hash = store().hashOnReference(branch, Optional.empty());

    assertThat(store().getValue(hash, Key.of("arbitrary"))).isNull();
  }

  @Test
  void recreateTable() throws Exception {
    BranchName branch = BranchName.of("recreateTable-main");
    Key key = Key.of("recreateTable");

    store().create(branch, Optional.empty());
    // commit just something to have a "real" common ancestor and not "beginning of time", which
    // means no-common-ancestor
    Hash ancestor =
        store()
            .commit(
                branch,
                Optional.empty(),
                "create table",
                singletonList(
                    Put.of(
                        key,
                        StringStoreWorker.withStateAndId(
                            "initial-state", "value", "CONTENT-ID-1"))));
    assertThat(store().getValue(branch, key)).isEqualTo("initial-state|value@CONTENT-ID-1");
    assertThat(store().getValue(ancestor, key)).isEqualTo("initial-state|value@CONTENT-ID-1");

    Hash delete =
        store().commit(branch, Optional.empty(), "drop table", ImmutableList.of(Delete.of(key)));
    assertThat(store().getValue(branch, key)).isNull();
    assertThat(store().getValue(delete, key)).isNull();

    Hash recreate =
        store()
            .commit(
                branch,
                Optional.empty(),
                "drop table",
                ImmutableList.of(
                    Put.of(
                        key,
                        StringStoreWorker.withStateAndId(
                            "recreate-state", "value", "CONTENT-ID-DIFFERENT"))));
    assertThat(store().getValue(branch, key))
        .isEqualTo("recreate-state|value@CONTENT-ID-DIFFERENT");
    assertThat(store().getValue(recreate, key))
        .isEqualTo("recreate-state|value@CONTENT-ID-DIFFERENT");
  }
}
