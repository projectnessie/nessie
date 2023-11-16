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
package org.projectnessie.versioned.storage.common.logic;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.commontests.AbstractIndexesLogicTests;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestIndexesLogicImpl extends AbstractIndexesLogicTests {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void completeIndexesInCommitChain(boolean discrete) throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogicImpl indexesLogic = new IndexesLogicImpl(persist);

    Map<StoreKey, ObjId> keyValue = new HashMap<>();
    Map<StoreKey, ObjId> keyValueSecondary = new HashMap<>();

    // 5 commits, all "complete"
    List<ObjId> tail = fiveCompleteCommits(keyValue);

    // 5 commits that represent a "merged branch"
    List<ObjId> secondary = new ArrayList<>();
    secondary.add(EMPTY_OBJ_ID);
    for (int i = 0; i < 5; i++) {
      incompleteCommit(secondary, "secondary" + i, keyValueSecondary, 7 + i, b -> {});
    }
    ObjId secondaryHeadId = secondary.get(0);
    CommitObj secondaryHead = commitLogic.fetchCommit(secondaryHeadId);
    soft.assertThat(secondaryHead).isNotNull();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () -> indexesLogic.buildCompleteIndex(requireNonNull(secondaryHead), Optional.empty()));

    // add incomplete commit, with a reference to a set of other incomplete commits,
    // simulating a merge
    incompleteCommit(tail, "pointer", keyValue, 6, b -> b.addSecondaryParents(secondaryHeadId));

    // Create 5 commits with "incomplete" indexes
    for (int i = 0; i < 5; i++) {
      incompleteCommit(tail, "incomplete" + i, keyValue, 7 + i, b -> {});
    }

    // Need 11 commits here (test sanity check) - plus one for EMPTY_OBJ_ID
    soft.assertThat(tail).hasSize(11 + 1);

    ObjId headId = tail.get(0);
    CommitObj head = commitLogic.fetchCommit(headId);
    soft.assertThat(head).isNotNull();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> indexesLogic.buildCompleteIndex(requireNonNull(head), Optional.empty()));

    soft.assertThat(indexesLogic.findCommitsWithIncompleteIndex(headId))
        .isEqualTo(tail.subList(0, 6));
    soft.assertThat(indexesLogic.findCommitsWithIncompleteIndex(secondaryHeadId))
        .isEqualTo(secondary.subList(0, 5));

    if (discrete) {
      Deque<ObjId> deque = new ArrayDeque<>();
      indexesLogic.completeIndexesInCommitChain(headId, deque, () -> {});
      soft.assertThat(deque).containsExactly(secondaryHeadId);

      deque = new ArrayDeque<>();
      indexesLogic.completeIndexesInCommitChain(secondaryHeadId, deque, () -> {});
      soft.assertThat(deque).isEmpty();

      deque = new ArrayDeque<>();
      indexesLogic.completeIndexesInCommitChain(headId, deque, () -> {});
      soft.assertThat(deque).isEmpty();

      deque = new ArrayDeque<>();
      indexesLogic.completeIndexesInCommitChain(secondaryHeadId, deque, () -> {});
      soft.assertThat(deque).isEmpty();
    } else {
      soft.assertThatThrownBy(
              () -> indexesLogic.completeIndexesInCommitChain(randomObjId(), () -> {}))
          .isInstanceOf(ObjNotFoundException.class);
      indexesLogic.completeIndexesInCommitChain(EMPTY_OBJ_ID, () -> {});
      indexesLogic.completeIndexesInCommitChain(headId, () -> {});
      indexesLogic.completeIndexesInCommitChain(headId, () -> {});
    }

    soft.assertThat(indexesLogic.findCommitsWithIncompleteIndex(headId)).isEmpty();
    soft.assertThat(indexesLogic.findCommitsWithIncompleteIndex(secondaryHeadId)).isEmpty();

    for (ObjId id : tail) {
      CommitObj commit = commitLogic.fetchCommit(id);
      if (commit == null) { // EMPTY_OBJ_ID
        continue;
      }
      soft.assertThat(commit)
          .describedAs("commit %s", id)
          .extracting(CommitObj::incompleteIndex, InstanceOfAssertFactories.BOOLEAN)
          .isFalse();
      soft.assertThatCode(() -> indexesLogic.buildCompleteIndex(commit, Optional.empty()))
          .doesNotThrowAnyException();
    }
    for (ObjId id : secondary) {
      CommitObj commit = commitLogic.fetchCommit(id);
      if (commit == null) { // EMPTY_OBJ_ID
        continue;
      }
      soft.assertThat(commit)
          .describedAs("commit %s", id)
          .extracting(CommitObj::incompleteIndex, InstanceOfAssertFactories.BOOLEAN)
          .isFalse();
      soft.assertThatCode(() -> indexesLogic.buildCompleteIndex(commit, Optional.empty()))
          .doesNotThrowAnyException();
    }
  }

  private void incompleteCommit(
      List<ObjId> tail,
      String key,
      Map<StoreKey, ObjId> keyValue,
      long seq,
      Consumer<CommitObj.Builder> b) {
    StoreKey k = key(key);
    ObjId v = randomObjId();
    soft.assertThat(keyValue.put(k, v)).isNull();

    CommitLogic commitLogic = commitLogic(persist);
    ObjId cid = randomObjId();
    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    index.add(indexElement(k, commitOp(ADD, 42, v)));
    CommitObj.Builder c =
        commitBuilder()
            .id(cid)
            .incompleteIndex(true)
            .incrementalIndex(index.serialize())
            .seq(seq)
            .created(42L)
            .message("Commit " + key)
            .headers(EMPTY_COMMIT_HEADERS);
    b.accept(c);
    tail.forEach(c::addTail);
    commitLogic.storeCommit(c.build(), emptyList());
    tail.add(0, cid);
  }

  private List<ObjId> fiveCompleteCommits(Map<StoreKey, ObjId> keyValue) throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    ObjId headId = EMPTY_OBJ_ID;
    List<ObjId> tail = new ArrayList<>();
    tail.add(headId);

    // Create 5 commits with "complete" indexes
    for (int i = 0; i < 5; i++) {
      StoreKey k = key("tail" + i);
      UUID cid = new UUID(123L, i);
      ObjId v = randomObjId();
      soft.assertThat(keyValue.put(k, v)).isNull();

      CommitObj commit =
          commitLogic.doCommit(
              newCommitBuilder()
                  .parentCommitId(headId)
                  .addAdds(commitAdd(k, 42, v, null, cid))
                  .message("Commit " + i)
                  .headers(EMPTY_COMMIT_HEADERS)
                  .build(),
              emptyList());
      headId = commit.id();
      tail.add(0, headId);
    }

    return tail;
  }
}
