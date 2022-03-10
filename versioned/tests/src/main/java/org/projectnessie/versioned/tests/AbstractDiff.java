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
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;
import org.projectnessie.versioned.testworker.OnRefOnly;

public abstract class AbstractDiff extends AbstractNestedVersionStore {

  public static final OnRefOnly V_1 = newOnRef("v1");
  public static final OnRefOnly V_2 = newOnRef("v2");
  public static final OnRefOnly V_2_A = newOnRef("v2a");
  public static final OnRefOnly V_3 = newOnRef("v3");

  protected AbstractDiff(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  @Test
  protected void checkDiff() throws VersionStoreException {
    final BranchName branch = BranchName.of("checkDiff");
    store().create(branch, Optional.empty());
    final Hash initial = store().hashOnReference(branch, Optional.empty());

    final Hash firstCommit = commit("First Commit").put("k1", V_1).put("k2", V_2).toBranch(branch);
    final Hash secondCommit =
        commit("Second Commit").put("k2", V_2_A).put("k3", V_3).toBranch(branch);

    List<Diff<BaseContent>> startToSecond = diffAsList(initial, secondCommit);
    assertThat(startToSecond)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k1"), Optional.empty(), Optional.of(V_1)),
            Diff.of(Key.of("k2"), Optional.empty(), Optional.of(V_2_A)),
            Diff.of(Key.of("k3"), Optional.empty(), Optional.of(V_3)));

    List<Diff<BaseContent>> secondToStart = diffAsList(secondCommit, initial);
    assertThat(secondToStart)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k1"), Optional.of(V_1), Optional.empty()),
            Diff.of(Key.of("k2"), Optional.of(V_2_A), Optional.empty()),
            Diff.of(Key.of("k3"), Optional.of(V_3), Optional.empty()));

    List<Diff<BaseContent>> firstToSecond = diffAsList(firstCommit, secondCommit);
    assertThat(firstToSecond)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k2"), Optional.of(V_2), Optional.of(V_2_A)),
            Diff.of(Key.of("k3"), Optional.empty(), Optional.of(V_3)));

    List<Diff<BaseContent>> firstToFirst = diffAsList(firstCommit, firstCommit);
    assertTrue(firstToFirst.isEmpty());
  }

  private List<Diff<BaseContent>> diffAsList(Hash initial, Hash secondCommit)
      throws ReferenceNotFoundException {
    try (Stream<Diff<BaseContent>> diffStream = store().getDiffs(initial, secondCommit)) {
      return diffStream.collect(Collectors.toList());
    }
  }
}
