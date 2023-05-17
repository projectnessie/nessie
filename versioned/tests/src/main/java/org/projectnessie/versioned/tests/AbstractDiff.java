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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.testworker.OnRefOnly;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractDiff extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  public static final OnRefOnly V_1 = newOnRef("v1");
  public static final OnRefOnly V_2 = newOnRef("v2");
  public static final OnRefOnly V_2_A = newOnRef("v2a");
  public static final OnRefOnly V_3 = newOnRef("v3");
  public static final OnRefOnly V_1_A = newOnRef("v1a");

  protected AbstractDiff(VersionStore store) {
    super(store);
  }

  @Test
  protected void checkDiff() throws VersionStoreException {
    BranchName branch = BranchName.of("checkDiff");
    store().create(branch, Optional.empty());
    Hash initial = store().hashOnReference(branch, Optional.empty());

    ContentKey k1 = ContentKey.of("k1");
    ContentKey k2 = ContentKey.of("k2");
    ContentKey k3 = ContentKey.of("k3");
    ContentKey k1a = ContentKey.of("k1a");

    Hash firstCommit = commit("First Commit").put("k1", V_1).put("k2", V_2).toBranch(branch);
    Content k2content = store().getValue(branch, k2);
    Hash secondCommit =
        commit("Second Commit")
            .put("k2", V_2_A.withId(k2content))
            .put("k3", V_3)
            .put("k1a", V_1_A)
            .toBranch(branch);

    List<Diff> startToSecond = diffAsList(initial, secondCommit);
    soft.assertThat(diffsWithoutContentId(startToSecond))
        .containsExactlyInAnyOrder(
            Diff.of(k1, Optional.empty(), Optional.of(V_1)),
            Diff.of(k2, Optional.empty(), Optional.of(V_2_A)),
            Diff.of(k3, Optional.empty(), Optional.of(V_3)),
            Diff.of(k1a, Optional.empty(), Optional.of(V_1_A)));

    List<Diff> secondToStart = diffAsList(secondCommit, initial);
    soft.assertThat(diffsWithoutContentId(secondToStart))
        .containsExactlyInAnyOrder(
            Diff.of(k1, Optional.of(V_1), Optional.empty()),
            Diff.of(k2, Optional.of(V_2_A), Optional.empty()),
            Diff.of(k3, Optional.of(V_3), Optional.empty()),
            Diff.of(k1a, Optional.of(V_1_A), Optional.empty()));

    List<Diff> firstToSecond = diffAsList(firstCommit, secondCommit);
    soft.assertThat(diffsWithoutContentId(firstToSecond))
        .containsExactlyInAnyOrder(
            Diff.of(k1a, Optional.empty(), Optional.of(V_1_A)),
            Diff.of(k2, Optional.of(V_2), Optional.of(V_2_A)),
            Diff.of(k3, Optional.empty(), Optional.of(V_3)));

    List<Diff> secondToFirst = diffAsList(secondCommit, firstCommit);
    soft.assertThat(diffsWithoutContentId(secondToFirst))
        .containsExactlyInAnyOrder(
            Diff.of(k1a, Optional.of(V_1_A), Optional.empty()),
            Diff.of(k2, Optional.of(V_2_A), Optional.of(V_2)),
            Diff.of(k3, Optional.of(V_3), Optional.empty()));

    soft.assertThat(diffAsList(firstCommit, firstCommit)).isEmpty();

    // Key restrictions
    if (store().getClass().getName().endsWith("VersionStoreImpl")) {
      soft.assertThat(
              diffAsList(initial, secondCommit, ContentKey.of("k"), ContentKey.of("l"), null, null))
          .extracting(Diff::getKey)
          .containsExactlyInAnyOrder(k1, k2, k3, k1a);

      soft.assertThat(diffAsList(initial, secondCommit, ContentKey.of("k"), null, null, null))
          .extracting(Diff::getKey)
          .containsExactlyInAnyOrder(k1, k2, k3, k1a);

      soft.assertThat(diffAsList(initial, secondCommit, null, k2, null, null))
          .extracting(Diff::getKey)
          .containsExactlyInAnyOrder(k1, k2, k1a);

      soft.assertThat(diffAsList(initial, secondCommit, k1a, k2, null, null))
          .extracting(Diff::getKey)
          .containsExactlyInAnyOrder(k1a, k2);

      soft.assertThat(diffAsList(initial, secondCommit, k2, k2, null, null))
          .extracting(Diff::getKey)
          .containsExactlyInAnyOrder(k2);

      soft.assertThat(
              diffAsList(
                  initial, secondCommit, ContentKey.of("k4"), ContentKey.of("l"), null, null))
          .isEmpty();

      // prefix

      soft.assertThat(diffAsList(initial, secondCommit, null, null, ContentKey.of("k1"), null))
          .extracting(Diff::getKey)
          .containsExactlyInAnyOrder(k1);

      soft.assertThat(diffAsList(initial, secondCommit, null, null, ContentKey.of("k"), null))
          .extracting(Diff::getKey)
          .isEmpty();

      soft.assertThat(diffAsList(initial, secondCommit, null, null, ContentKey.of("x"), null))
          .isEmpty();
    }
    soft.assertThat(
            diffAsList(initial, secondCommit, null, null, null, k -> k1.equals(k) || k3.equals(k)))
        .extracting(Diff::getKey)
        .containsExactlyInAnyOrder(k1, k3);

    soft.assertThat(diffAsList(initial, secondCommit, null, null, null, k -> false)).isEmpty();
  }

  private List<Diff> diffAsList(Hash initial, Hash secondCommit) throws ReferenceNotFoundException {
    return diffAsList(initial, secondCommit, null, null, null, null);
  }

  private List<Diff> diffAsList(
      Hash initial,
      Hash secondCommit,
      ContentKey minKey,
      ContentKey maxKey,
      ContentKey prefixKey,
      Predicate<ContentKey> keyPredicate)
      throws ReferenceNotFoundException {
    try (PaginationIterator<Diff> diffStream =
        store().getDiffs(initial, secondCommit, null, minKey, maxKey, prefixKey, keyPredicate)) {
      List<Diff> r = new ArrayList<>();
      diffStream.forEachRemaining(r::add);
      return r;
    }
  }
}
