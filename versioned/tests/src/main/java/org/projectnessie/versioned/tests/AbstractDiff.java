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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.KeyRestrictions;
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
  public static final IcebergTable V_12 = IcebergTable.of("foo", 42, 43, 44, 45);
  public static final IcebergTable V_22 = IcebergTable.of("bar", 42, 43, 44, 45);

  protected AbstractDiff(VersionStore store) {
    super(store);
  }

  @Test
  protected void checkDiffDifferentContents() throws VersionStoreException {
    BranchName branch1 = BranchName.of("checkDiffOtherContent1");
    BranchName branch2 = BranchName.of("checkDiffOtherContent2");
    store().create(branch1, Optional.empty());
    store().create(branch2, Optional.empty());
    Hash commit1 = commit("Commit1").put("k1", V_1).put("k2", V_2).toBranch(branch1);
    Hash commit2 = commit("Commit2").put("k1", V_12).put("k2", V_22).toBranch(branch2);
    ContentKey k1 = ContentKey.of("k1");
    ContentKey k2 = ContentKey.of("k2");

    Map<ContentKey, ContentResult> contents1 =
        store().getValues(branch1, newArrayList(k1, k2), false);
    IdentifiedContentKey ik11 = contents1.get(k1).identifiedKey();
    IdentifiedContentKey ik12 = contents1.get(k2).identifiedKey();

    Map<ContentKey, ContentResult> contents2 =
        store().getValues(branch2, newArrayList(k1, k2), false);
    IdentifiedContentKey ik21 = contents2.get(k1).identifiedKey();
    IdentifiedContentKey ik22 = contents2.get(k2).identifiedKey();

    List<Diff> diff = diffAsList(commit1, commit2);
    soft.assertThat(diffsWithoutContentId(diff))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(
            tuple(ik11, ik21, Optional.of(V_1), Optional.of(V_12)),
            tuple(ik12, ik22, Optional.of(V_2), Optional.of(V_22)));
  }

  @Test
  protected void checkDiffRename() throws VersionStoreException {
    BranchName branch1 = BranchName.of("checkDiffRename");
    ContentKey k1 = ContentKey.of("k1");
    ContentKey k2 = ContentKey.of("k2");

    store().create(branch1, Optional.empty());
    Hash commit1 = commit("Commit1").put("k1", V_1).toBranch(branch1);

    ContentResult contents1 = store().getValue(commit1, k1, false);
    IdentifiedContentKey ik1 = contents1.identifiedKey();
    Content content = requireNonNull(contents1.content());

    // Rename operation
    Hash commit2 = commit("Commit2").delete("k1").put("k2", content).toBranch(branch1);

    ContentResult contents2 = store().getValue(commit2, k2, false);
    IdentifiedContentKey ik2 = contents2.identifiedKey();

    List<Diff> diff = diffAsList(commit1, commit2);
    soft.assertThat(ik1.lastElement().contentId()).isEqualTo(content.getId());
    soft.assertThat(ik2.lastElement().contentId()).isEqualTo(content.getId());
    soft.assertThat(diffsWithoutContentId(diff))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(
            tuple(ik1, null, Optional.of(V_1), Optional.empty()),
            tuple(null, ik2, Optional.empty(), Optional.of(V_1)));
  }

  @Test
  protected void checkDiffDropAndCreate() throws VersionStoreException {
    BranchName branch1 = BranchName.of("checkDiffReadd");
    ContentKey k1 = ContentKey.of("k1");
    ContentKey k2 = ContentKey.of("k2");

    store().create(branch1, Optional.empty());
    Hash commit1 = commit("Commit1").put("k1", V_1).toBranch(branch1);

    // Rename operation
    Hash commit2 = commit("Commit2").delete("k1").put("k2", V_2).toBranch(branch1);

    ContentResult contents1 = store().getValue(commit1, k1, false);
    IdentifiedContentKey ik1 = contents1.identifiedKey();
    Content content1 = requireNonNull(contents1.content());
    ContentResult contents2 = store().getValue(commit2, k2, false);
    IdentifiedContentKey ik2 = contents2.identifiedKey();
    Content content2 = requireNonNull(contents2.content());

    soft.assertThat(ik1.lastElement().contentId())
        .isEqualTo(content1.getId())
        .isNotEqualTo(content2.getId());
    soft.assertThat(ik2.lastElement().contentId())
        .isEqualTo(content2.getId())
        .isNotEqualTo(content1.getId());

    List<Diff> diff = diffAsList(commit1, commit2);
    soft.assertThat(diffsWithoutContentId(diff))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(
            tuple(ik1, null, Optional.of(V_1), Optional.empty()),
            tuple(null, ik2, Optional.empty(), Optional.of(V_2)));
  }

  @Test
  protected void checkDiffReadd() throws VersionStoreException {
    BranchName branch1 = BranchName.of("checkDiffReadd");
    ContentKey k = ContentKey.of("k");

    store().create(branch1, Optional.empty());
    Hash commit1 = commit("Commit1").put("k", V_1).toBranch(branch1);
    Content content1 = requireNonNull(store().getValue(commit1, k, false).content());

    // Re-add operations (have to happen in 2 commits)
    commit("Commit2").delete("k").toBranch(branch1);
    Hash commit3 = commit("Commit3").put("k", V_2).toBranch(branch1);
    Content content3 = requireNonNull(store().getValue(commit3, k, false).content());

    ContentResult contents1 = store().getValue(commit1, k, false);
    IdentifiedContentKey ik1 = contents1.identifiedKey();
    ContentResult contents2 = store().getValue(commit3, k, false);
    IdentifiedContentKey ik2 = contents2.identifiedKey();

    soft.assertThat(ik1.lastElement().contentId())
        .isEqualTo(content1.getId())
        .isNotEqualTo(content3.getId());
    soft.assertThat(ik2.lastElement().contentId())
        .isEqualTo(content3.getId())
        .isNotEqualTo(content1.getId());

    List<Diff> diff = diffAsList(commit1, commit3);
    soft.assertThat(diffsWithoutContentId(diff))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(tuple(ik1, ik2, Optional.of(V_1), Optional.of(V_2)));
  }

  @Test
  protected void checkDiff() throws VersionStoreException {
    BranchName branch = BranchName.of("checkDiff");
    store().create(branch, Optional.empty());
    Hash initial = store().hashOnReference(branch, Optional.empty(), emptyList());

    ContentKey k1 = ContentKey.of("k1");
    ContentKey k2 = ContentKey.of("k2");
    ContentKey k3 = ContentKey.of("k3");
    ContentKey k1a = ContentKey.of("k1a");

    Hash firstCommit = commit("First Commit").put("k1", V_1).put("k2", V_2).toBranch(branch);
    Content k2content =
        requireNonNull(store().getValue(branch, ContentKey.of("k2"), false).content());
    Hash secondCommit =
        commit("Second Commit")
            .put("k2", V_2_A.withId(k2content.getId()))
            .put("k3", V_3)
            .put("k1a", V_1_A)
            .toBranch(branch);

    Map<ContentKey, ContentResult> contents =
        store().getValues(branch, newArrayList(k1, k2, k3, k1a), false);
    IdentifiedContentKey ik1 = contents.get(k1).identifiedKey();
    IdentifiedContentKey ik2 = contents.get(k2).identifiedKey();
    IdentifiedContentKey ik3 = contents.get(k3).identifiedKey();
    IdentifiedContentKey ik1a = contents.get(k1a).identifiedKey();

    List<Diff> startToSecond = diffAsList(initial, secondCommit);
    soft.assertThat(diffsWithoutContentId(startToSecond))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(
            tuple(null, ik1, Optional.empty(), Optional.of(V_1)),
            tuple(null, ik2, Optional.empty(), Optional.of(V_2_A)),
            tuple(null, ik3, Optional.empty(), Optional.of(V_3)),
            tuple(null, ik1a, Optional.empty(), Optional.of(V_1_A)));

    List<Diff> secondToStart = diffAsList(secondCommit, initial);
    soft.assertThat(diffsWithoutContentId(secondToStart))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(
            tuple(ik1, null, Optional.of(V_1), Optional.empty()),
            tuple(ik2, null, Optional.of(V_2_A), Optional.empty()),
            tuple(ik3, null, Optional.of(V_3), Optional.empty()),
            tuple(ik1a, null, Optional.of(V_1_A), Optional.empty()));

    List<Diff> firstToSecond = diffAsList(firstCommit, secondCommit);
    soft.assertThat(diffsWithoutContentId(firstToSecond))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(
            tuple(null, ik1a, Optional.empty(), Optional.of(V_1_A)),
            tuple(ik2, ik2, Optional.of(V_2), Optional.of(V_2_A)),
            tuple(null, ik3, Optional.empty(), Optional.of(V_3)));

    List<Diff> secondToFirst = diffAsList(secondCommit, firstCommit);
    soft.assertThat(diffsWithoutContentId(secondToFirst))
        .extracting(Diff::getFromKey, Diff::getToKey, Diff::getFromValue, Diff::getToValue)
        .containsExactlyInAnyOrder(
            tuple(ik1a, null, Optional.of(V_1_A), Optional.empty()),
            tuple(ik2, ik2, Optional.of(V_2_A), Optional.of(V_2)),
            tuple(ik3, null, Optional.of(V_3), Optional.empty()));

    soft.assertThat(diffAsList(firstCommit, firstCommit)).isEmpty();

    // Key restrictions
    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder()
                    .minKey(ContentKey.of("k"))
                    .maxKey(ContentKey.of("l"))
                    .build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .containsExactlyInAnyOrder(
            tuple(null, ik1), tuple(null, ik2), tuple(null, ik3), tuple(null, ik1a));

    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder().minKey(ContentKey.of("k")).build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .containsExactlyInAnyOrder(
            tuple(null, ik1), tuple(null, ik2), tuple(null, ik3), tuple(null, ik1a));

    soft.assertThat(diffAsList(initial, secondCommit, KeyRestrictions.builder().maxKey(k2).build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .containsExactlyInAnyOrder(tuple(null, ik1), tuple(null, ik2), tuple(null, ik1a));

    soft.assertThat(
            diffAsList(
                initial, secondCommit, KeyRestrictions.builder().minKey(k1a).maxKey(k2).build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .containsExactlyInAnyOrder(tuple(null, ik1a), tuple(null, ik2));

    soft.assertThat(
            diffAsList(
                initial, secondCommit, KeyRestrictions.builder().minKey(k2).maxKey(k2).build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .containsExactlyInAnyOrder(tuple(null, ik2));

    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder()
                    .minKey(ContentKey.of("k4"))
                    .maxKey(ContentKey.of("l"))
                    .build()))
        .isEmpty();

    // prefix

    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder().prefixKey(ContentKey.of("k1")).build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .containsExactlyInAnyOrder(tuple(null, ik1));

    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder().prefixKey(ContentKey.of("k")).build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .isEmpty();

    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder().prefixKey(ContentKey.of("x")).build()))
        .isEmpty();
    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder()
                    .contentKeyPredicate((k, t) -> k1.equals(k) || k3.equals(k))
                    .build()))
        .extracting(Diff::getFromKey, Diff::getToKey)
        .containsExactlyInAnyOrder(tuple(null, ik1), tuple(null, ik3));

    soft.assertThat(
            diffAsList(
                initial,
                secondCommit,
                KeyRestrictions.builder().contentKeyPredicate((k, t) -> false).build()))
        .isEmpty();
  }

  private List<Diff> diffAsList(Hash initial, Hash secondCommit) throws ReferenceNotFoundException {
    return diffAsList(initial, secondCommit, NO_KEY_RESTRICTIONS);
  }

  private List<Diff> diffAsList(Hash initial, Hash secondCommit, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    try (PaginationIterator<Diff> diffStream =
        store().getDiffs(initial, secondCommit, null, keyRestrictions)) {
      List<Diff> r = new ArrayList<>();
      diffStream.forEachRemaining(r::add);
      return r;
    }
  }
}
