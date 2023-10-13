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
package org.projectnessie.versioned.storage.commontests;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.model.CommitConsistency.COMMIT_CONSISTENT;
import static org.projectnessie.model.CommitConsistency.COMMIT_CONTENT_INCONSISTENT;
import static org.projectnessie.model.CommitConsistency.COMMIT_INCONSISTENT;
import static org.projectnessie.model.CommitConsistency.NOT_CHECKED;
import static org.projectnessie.versioned.ReferenceHistory.ReferenceHistoryElement.referenceHistoryElement;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_INCREMENTAL_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_SERIALIZED_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceHistory;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
import org.projectnessie.versioned.tests.AbstractNestedVersionStore;
import org.projectnessie.versioned.tests.CommitBuilder;

@SuppressWarnings("unused")
@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractCommitConsistency extends AbstractNestedVersionStore {

  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractCommitConsistency(VersionStore store) {
    super(store);
  }

  @Test
  void referenceDeletionWorksWithoutHeadCommit(@NessiePersist Persist persist) throws Exception {
    BranchName branch = BranchName.of("no-head-commit");
    store().create(branch, Optional.empty());

    Hash head =
        commit("deleted-commit").put("tab", IcebergTable.of("x", 1, 2, 3, 4)).toBranch(branch);

    ReferenceHistory history = store().getReferenceHistory(branch.getName(), null);
    soft.assertThat(history)
        .extracting(h -> h.current().commitConsistency(), ReferenceHistory::commitLogConsistency)
        .containsExactly(COMMIT_CONSISTENT, NOT_CHECKED);
    history = store().getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(h -> h.current().commitConsistency(), ReferenceHistory::commitLogConsistency)
        .containsExactly(COMMIT_CONSISTENT, COMMIT_CONSISTENT);

    persist.deleteObj(hashToObjId(head));

    history = store().getReferenceHistory(branch.getName(), null);
    soft.assertThat(history)
        .extracting(h -> h.current().commitConsistency(), ReferenceHistory::commitLogConsistency)
        .containsExactly(COMMIT_INCONSISTENT, NOT_CHECKED);
    history = store().getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(h -> h.current().commitConsistency(), ReferenceHistory::commitLogConsistency)
        .containsExactly(COMMIT_INCONSISTENT, COMMIT_INCONSISTENT);

    store().delete(branch, head);

    soft.assertThatThrownBy(() -> store().getNamedRef(branch.getName(), GetNamedRefsParams.DEFAULT))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  /**
   * Create a couple of commits that use reference indexes (spilled out to external objects),
   * corrupt those in various ways and eventually assign the branch to another commit, where the
   * branch's current HEAD does not exist.
   */
  @Test
  void corruptAndReassignWithReferenceIndex(
      @NessieStoreConfig(name = CONFIG_MAX_INCREMENTAL_INDEX_SIZE, value = "1024")
          @NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "1024")
          @NessiePersist
          Persist persist)
      throws Exception {
    VersionStore store = new VersionStoreImpl(persist);

    BranchName branch = BranchName.of("main");

    BiFunction<Integer, Integer, ContentKey> contentKey =
        (c, k) -> ContentKey.of("commit-" + c + "-key-" + k);

    List<Hash> ids = new ArrayList<>();
    int numCommits = 10;
    int keysPerCommit = 50;
    for (int c = 0; c < numCommits; c++) {
      CommitBuilder commit = commit(store, "commit 1");
      for (int k = 0; k < keysPerCommit; k++) {
        commit.put(
            contentKey.apply(c, k),
            IcebergTable.of(
                "meta-" + c + "-" + k + "-12345678901234567890123456789012345678901234567890",
                1,
                2,
                3,
                4));
      }
      ids.add(commit.toBranch(branch));
    }
    Collections.reverse(ids);

    ReferenceLogic referenceLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);

    List<CommitObj> commits = new ArrayList<>();
    List<CommitMeta> meta = new ArrayList<>();
    for (Hash id : ids) {
      CommitObj commit = requireNonNull(commitLogic.fetchCommit(hashToObjId(id)));
      commits.add(commit);
      meta.add(toCommitMeta(commit));
    }

    List<ReferenceHistory.ReferenceHistoryElement> expected = new ArrayList<>();
    for (int i = 1; i < ids.size(); i++) {
      expected.add(referenceHistoryElement(ids.get(i), COMMIT_CONSISTENT, meta.get(i)));
    }
    expected.add(referenceHistoryElement(objIdToHash(EMPTY_OBJ_ID), COMMIT_CONSISTENT, null));

    ReferenceHistory history = store.getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(ids.get(0), COMMIT_CONSISTENT, meta.get(0)),
            expected,
            COMMIT_CONSISTENT);

    // Delete reference index object of latest commit --> COMMIT_INCONSISTENT w/ meta

    CommitObj latestCommit = requireNonNull(commits.get(0));
    soft.assertThat(latestCommit).extracting(CommitObj::referenceIndex).isNotNull();

    persist.deleteObj(requireNonNull(latestCommit.referenceIndex()));

    history = store.getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(ids.get(0), COMMIT_INCONSISTENT, meta.get(0)),
            expected,
            COMMIT_INCONSISTENT);

    // Delete commit object of latest commit --> COMMIT_INCONSISTENT w/o meta

    persist.deleteObj(latestCommit.id());

    history = store.getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(ids.get(0), COMMIT_INCONSISTENT, null),
            expected,
            COMMIT_INCONSISTENT);

    // Reassign HEAD of branch to 2nd-to-last commit

    store.assign(branch, ids.get(0), ids.get(1));

    expected.add(0, history.current());

    history = store.getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(ids.get(1), COMMIT_CONSISTENT, meta.get(1)),
            expected,
            COMMIT_CONSISTENT);
  }

  /** Corrupt commit and content objects, validate the reference history elements. */
  @Test
  void corruptIncrementally(@NessiePersist Persist persist) throws Exception {
    BranchName branch = BranchName.of("main");

    ReferenceHistory history = store().getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(store().noAncestorHash(), COMMIT_CONSISTENT, null),
            emptyList(),
            COMMIT_CONSISTENT);

    ContentKey t1 = ContentKey.of("t1");
    ContentKey t2 = ContentKey.of("t2");
    ContentKey t3 = ContentKey.of("t3");

    Hash commit1 = commit("commit 1").put(t1, IcebergTable.of("x1", 1, 2, 3, 4)).toBranch(branch);
    Hash commit2 = commit("commit 2").put(t2, IcebergTable.of("x2", 1, 2, 3, 4)).toBranch(branch);
    Hash commit3 = commit("commit 3").put(t3, IcebergTable.of("x3", 1, 2, 3, 4)).toBranch(branch);

    ReferenceLogic referenceLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);

    CommitObj commitObj1 = requireNonNull(commitLogic.fetchCommit(hashToObjId(commit1)));
    CommitObj commitObj2 = requireNonNull(commitLogic.fetchCommit(hashToObjId(commit2)));
    CommitObj commitObj3 = requireNonNull(commitLogic.fetchCommit(hashToObjId(commit3)));
    CommitMeta meta1 = toCommitMeta(commitObj1);
    CommitMeta meta2 = toCommitMeta(commitObj2);
    CommitMeta meta3 = toCommitMeta(commitObj3);
    StoreIndex<CommitOp> index1 = indexesLogic.buildCompleteIndex(commitObj1, Optional.empty());
    StoreIndex<CommitOp> index2 = indexesLogic.buildCompleteIndex(commitObj2, Optional.empty());
    StoreIndex<CommitOp> index3 = indexesLogic.buildCompleteIndex(commitObj3, Optional.empty());

    history = store().getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(commit3, COMMIT_CONSISTENT, meta3),
            asList(
                referenceHistoryElement(commit2, COMMIT_CONSISTENT, meta2),
                referenceHistoryElement(commit1, COMMIT_CONSISTENT, meta1),
                referenceHistoryElement(store().noAncestorHash(), COMMIT_CONSISTENT, null)),
            COMMIT_CONSISTENT);

    // Delete content object "t2"
    persist.deleteObj(
        requireNonNull(requireNonNull(index2.get(keyToStoreKey(t2))).content().value()));

    history = store().getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(commit3, COMMIT_CONTENT_INCONSISTENT, meta3),
            asList(
                referenceHistoryElement(commit2, COMMIT_CONTENT_INCONSISTENT, meta2),
                referenceHistoryElement(commit1, COMMIT_CONSISTENT, meta1),
                referenceHistoryElement(store().noAncestorHash(), COMMIT_CONSISTENT, null)),
            COMMIT_CONTENT_INCONSISTENT);

    // Delete commit obj 1
    persist.deleteObj(hashToObjId(commit1));

    history = store().getReferenceHistory(branch.getName(), 20);
    soft.assertThat(history)
        .extracting(
            ReferenceHistory::current,
            ReferenceHistory::previous,
            ReferenceHistory::commitLogConsistency)
        .containsExactly(
            referenceHistoryElement(commit3, COMMIT_CONTENT_INCONSISTENT, meta3),
            asList(
                referenceHistoryElement(commit2, COMMIT_CONTENT_INCONSISTENT, meta2),
                referenceHistoryElement(commit1, COMMIT_INCONSISTENT, null),
                referenceHistoryElement(store().noAncestorHash(), COMMIT_CONSISTENT, null)),
            COMMIT_INCONSISTENT);
  }
}
