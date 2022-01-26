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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.StringStoreWorker.TestEnum;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;

public abstract class AbstractDiff extends AbstractNestedVersionStore {
  protected AbstractDiff(VersionStore<String, String, TestEnum> store) {
    super(store);
  }

  @Test
  protected void checkDiff() throws VersionStoreException {
    final BranchName branch = BranchName.of("checkDiff");
    store().create(branch, Optional.empty());
    final Hash initial = store().hashOnReference(branch, Optional.empty());

    final Hash firstCommit =
        commit("First Commit").put("k1", "v1").put("k2", "v2").toBranch(branch);
    final Hash secondCommit =
        commit("Second Commit").put("k2", "v2a").put("k3", "v3").toBranch(branch);

    List<Diff<String>> startToSecond =
        store().getDiffs(initial, secondCommit).collect(Collectors.toList());
    assertThat(startToSecond)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k1"), Optional.empty(), Optional.of("v1")),
            Diff.of(Key.of("k2"), Optional.empty(), Optional.of("v2a")),
            Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3")));

    List<Diff<String>> secondToStart =
        store().getDiffs(secondCommit, initial).collect(Collectors.toList());
    assertThat(secondToStart)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k1"), Optional.of("v1"), Optional.empty()),
            Diff.of(Key.of("k2"), Optional.of("v2a"), Optional.empty()),
            Diff.of(Key.of("k3"), Optional.of("v3"), Optional.empty()));

    List<Diff<String>> firstToSecond =
        store().getDiffs(firstCommit, secondCommit).collect(Collectors.toList());
    assertThat(firstToSecond)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k2"), Optional.of("v2"), Optional.of("v2a")),
            Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3")));

    List<Diff<String>> firstToFirst =
        store().getDiffs(firstCommit, firstCommit).collect(Collectors.toList());
    assertTrue(firstToFirst.isEmpty());
  }
}
