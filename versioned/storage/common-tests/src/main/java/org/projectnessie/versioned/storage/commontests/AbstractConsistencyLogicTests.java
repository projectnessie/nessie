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
package org.projectnessie.versioned.storage.commontests;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_INCREMENTAL_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_SERIALIZED_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.VALUE_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.CommitLogic.ValueReplacement.NO_VALUE_REPLACEMENT;
import static org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution.CONFLICT;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.DiffEntry.diffEntry;
import static org.projectnessie.versioned.storage.common.logic.DiffQuery.diffQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.consistencyLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.emptyPagingToken;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import com.google.common.collect.Lists;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitConflict;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.ConsistencyLogic;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.DiffEntry;
import org.projectnessie.versioned.storage.common.logic.HeadsAndForkPoints;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.PagingToken;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

/** {@link CommitLogic} related tests to be run against every {@link Persist} implementation. */
@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class AbstractConsistencyLogicTests {
  static final String NO_COMMON_ANCESTOR_IN_PARENTS_OF = "No common ancestor in parents of ";

  public static final String STD_MESSAGE = "foo";
  public static final StoreKey KEY_ONE = key("one");
  public static final StoreKey KEY_AAA = key("aaa");

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  public static CreateCommit.Builder stdCommit() {
    return newCommitBuilder()
        .parentCommitId(EMPTY_OBJ_ID)
        .headers(EMPTY_COMMIT_HEADERS)
        .message(STD_MESSAGE);
  }

  @Test
  public void fetchCommit() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    assertThat(commitLogic.fetchCommit(EMPTY_OBJ_ID)).isNull();

    ObjId commitId = requireNonNull(commitLogic.doCommit(stdCommit().build(), emptyList())).id();

    soft.assertThat(commitLogic.fetchCommits(commitId, commitId))
        .extracting(Obj::id, CommitObj::message)
        .containsExactly(tuple(commitId, STD_MESSAGE), tuple(commitId, STD_MESSAGE));
  }

  @Test
  public void fetchCommits() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    assertThat(commitLogic.fetchCommits(EMPTY_OBJ_ID, EMPTY_OBJ_ID)).hasSize(2).containsOnlyNulls();

    String otherMessage = "other";

    ObjId commitId1 = requireNonNull(commitLogic.doCommit(stdCommit().build(), emptyList())).id();
    ObjId commitId2 =
        requireNonNull(commitLogic.doCommit(stdCommit().message(otherMessage).build(), emptyList()))
            .id();

    soft.assertThat(commitLogic.fetchCommits(commitId1, commitId2))
        .extracting(Obj::id, CommitObj::message)
        .containsExactly(tuple(commitId1, STD_MESSAGE), tuple(commitId2, otherMessage));

    soft.assertThat(commitLogic.fetchCommits(EMPTY_OBJ_ID, commitId2))
        .extracting(o -> o != null ? tuple(o.id(), o.message()) : null)
        .containsExactly(null, tuple(commitId2, otherMessage));

    soft.assertThat(commitLogic.fetchCommits(commitId1, EMPTY_OBJ_ID))
        .extracting(o -> o != null ? tuple(o.id(), o.message()) : null)
        .containsExactly(tuple(commitId1, STD_MESSAGE), null);
  }

  @Test
  public void commonAncestor() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    ObjId root = requireNonNull(commitLogic.doCommit(stdCommit().build(), emptyList())).id();

    ObjId commonAncestor = root;
    for (int i = 0; i < 5; i++) {
      commonAncestor =
          requireNonNull(
                  commitLogic.doCommit(
                      stdCommit().parentCommitId(commonAncestor).message("Commit #" + i).build(),
                      emptyList()))
              .id();
    }

    ObjId[] branches = new ObjId[5];
    Arrays.fill(branches, commonAncestor);
    for (int branch = 0; branch < 5; branch++) {
      for (int i = 0; i < 5; i++) {
        branches[branch] =
            requireNonNull(
                    commitLogic.doCommit(
                        stdCommit()
                            .parentCommitId(branches[branch])
                            .message("Branch " + branch + " commit #" + i)
                            .build(),
                        emptyList()))
                .id();
      }
    }

    for (ObjId branch : branches) {
      soft.assertThat(commitLogic.findCommonAncestor(branch, root)).isEqualTo(root);
    }

    for (int i = 0; i < branches.length; i++) {
      requireNonNull(branches[i]);
      soft.assertThat(commitLogic.findCommonAncestor(branches[i], branches[i]))
          .isEqualTo(branches[i]);
      for (int j = i + 1; j < branches.length; j++) {
        soft.assertThat(commitLogic.findCommonAncestor(branches[i], branches[j]))
            .isEqualTo(commonAncestor);
      }
    }
  }

  @Test
  public void noCommonAncestor() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    soft.assertThatThrownBy(() -> commitLogic.findCommonAncestor(EMPTY_OBJ_ID, EMPTY_OBJ_ID))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);

    ObjId branch1commit1 =
        requireNonNull(
                commitLogic.doCommit(stdCommit().message("branch1commit1").build(), emptyList()))
            .id();
    ObjId branch1commit2 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit().message("branch1commit2").parentCommitId(branch1commit1).build(),
                    emptyList()))
            .id();
    ObjId branch1commit3 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit().message("branch1commit3").parentCommitId(branch1commit2).build(),
                    emptyList()))
            .id();
    ObjId branch2commit1 =
        requireNonNull(
                commitLogic.doCommit(stdCommit().message("branch2commit1").build(), emptyList()))
            .id();
    ObjId branch2commit2 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit().message("branch2commit2").parentCommitId(branch2commit1).build(),
                    emptyList()))
            .id();
    ObjId branch2commit3 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit().message("branch2commit3").parentCommitId(branch2commit2).build(),
                    emptyList()))
            .id();

    soft.assertThatThrownBy(() -> commitLogic.findCommonAncestor(branch1commit1, branch2commit1))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
    soft.assertThatThrownBy(() -> commitLogic.findCommonAncestor(branch1commit2, branch2commit1))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
    soft.assertThatThrownBy(() -> commitLogic.findCommonAncestor(branch1commit3, branch2commit1))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
    soft.assertThatThrownBy(() -> commitLogic.findCommonAncestor(branch1commit1, branch2commit2))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
    soft.assertThatThrownBy(() -> commitLogic.findCommonAncestor(branch1commit1, branch2commit3))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
    soft.assertThatThrownBy(() -> commitLogic.findCommonAncestor(branch1commit3, branch2commit3))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);

    soft.assertThatThrownBy(
            () ->
                commitLogic.findCommonAncestor(branch1commit1, objIdFromString("1111111111111111")))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessage("Commit '1111111111111111' not found");

    soft.assertThatThrownBy(
            () ->
                commitLogic.findCommonAncestor(objIdFromString("1111111111111111"), branch2commit1))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessage("Commit '1111111111111111' not found");

    soft.assertThatThrownBy(
            () -> commitLogic.findCommonAncestor(EMPTY_OBJ_ID, objIdFromString("1111111111111111")))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessage("Commit '1111111111111111' not found");

    soft.assertThatThrownBy(
            () -> commitLogic.findCommonAncestor(objIdFromString("1111111111111111"), EMPTY_OBJ_ID))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessage("Commit '1111111111111111' not found");
  }

  @Test
  public void commitLog() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    soft.assertThatThrownBy(
            () -> newArrayList(commitLogic.commitLog(commitLogQuery(randomObjId()))))
        .isInstanceOf(NoSuchElementException.class);

    soft.assertThat(newArrayList(commitLogic.commitLog(commitLogQuery(EMPTY_OBJ_ID)))).isEmpty();

    ObjId tip = requireNonNull(commitLogic.doCommit(stdCommit().build(), emptyList())).id();

    List<ObjId> tail = new ArrayList<>();
    tail.add(EMPTY_OBJ_ID);

    List<Tuple> expected = new ArrayList<>();
    expected.add(tuple(new ArrayList<>(tail), STD_MESSAGE, 1L));

    soft.assertThat(newArrayList(commitLogic.commitLog(commitLogQuery(tip))))
        .hasSize(1)
        .extracting(CommitObj::tail, CommitObj::message, CommitObj::seq)
        .containsExactlyElementsOf(expected);

    for (int i = 0; i < 100; i++) {
      tail.add(0, tip);
      if (tail.size() > persist.config().parentsPerCommit()) {
        tail.remove(persist.config().parentsPerCommit());
      }

      String msg = "commit #" + i;
      tip =
          requireNonNull(
                  commitLogic.doCommit(
                      stdCommit().parentCommitId(tip).message(msg).build(), emptyList()))
              .id();

      expected.add(0, tuple(new ArrayList<>(tail), msg, 2L + i));
    }

    soft.assertThat(newArrayList(commitLogic.commitLog(commitLogQuery(tip))))
        .hasSize(101)
        .extracting(CommitObj::tail, CommitObj::message, CommitObj::seq)
        .containsExactlyElementsOf(expected);

    PagedResult<CommitObj, ObjId> iter = commitLogic.commitLog(commitLogQuery(tip));
    soft.assertThat(iter.tokenForKey(null)).isEqualTo(emptyPagingToken());
    for (int i = 0; iter.hasNext(); i++) {
      CommitObj commit = iter.next();
      PagingToken token = iter.tokenForKey(commit.id());
      ObjId idFromToken = ObjId.objIdFromBytes(token.token());

      soft.assertThat(newArrayList(commitLogic.commitLog(commitLogQuery(token, tip, null))))
          .extracting(CommitObj::tail, CommitObj::message, CommitObj::seq)
          .containsExactlyElementsOf(expected.subList(i, expected.size()));

      soft.assertThat(newArrayList(commitLogic.commitLog(commitLogQuery(token, idFromToken, null))))
          .extracting(CommitObj::tail, CommitObj::message, CommitObj::seq)
          .containsExactlyElementsOf(expected.subList(i, expected.size()));

      soft.assertThat(newArrayList(commitLogic.commitLog(commitLogQuery(idFromToken))))
          .extracting(CommitObj::tail, CommitObj::message, CommitObj::seq)
          .containsExactlyElementsOf(expected.subList(i, expected.size()));

      soft.assertThat(
              newArrayList(commitLogic.commitLog(commitLogQuery(token, idFromToken, idFromToken))))
          .extracting(CommitObj::tail, CommitObj::message, CommitObj::seq)
          .containsExactlyElementsOf(expected.subList(i, i + 1));

      soft.assertThat(
              newArrayList(commitLogic.commitLog(commitLogQuery(null, idFromToken, idFromToken))))
          .extracting(CommitObj::tail, CommitObj::message, CommitObj::seq)
          .containsExactlyElementsOf(expected.subList(i, i + 1));

      soft.assertAll();
    }
  }

  @Test
  public void commitIdLog() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    soft.assertThatThrownBy(
            () -> newArrayList(commitLogic.commitIdLog(commitLogQuery(randomObjId()))))
        .isInstanceOf(NoSuchElementException.class);

    soft.assertThat(newArrayList(commitLogic.commitIdLog(commitLogQuery(EMPTY_OBJ_ID)))).isEmpty();

    ObjId tip = requireNonNull(commitLogic.doCommit(stdCommit().build(), emptyList())).id();

    List<ObjId> expected = new ArrayList<>();
    expected.add(tip);

    soft.assertThat(newArrayList(commitLogic.commitIdLog(commitLogQuery(tip))))
        .hasSize(1)
        .containsExactlyElementsOf(expected);

    for (int i = 0; i < 100; i++) {
      String msg = "commit #" + i;
      tip =
          requireNonNull(
                  commitLogic.doCommit(
                      stdCommit().parentCommitId(tip).message(msg).build(), emptyList()))
              .id();

      expected.add(0, tip);
    }

    soft.assertThat(newArrayList(commitLogic.commitIdLog(commitLogQuery(tip))))
        .hasSize(101)
        .containsExactlyElementsOf(expected);

    PagedResult<ObjId, ObjId> iter = commitLogic.commitIdLog(commitLogQuery(tip));
    soft.assertThat(iter.tokenForKey(null)).isEqualTo(emptyPagingToken());
    for (int i = 0; iter.hasNext(); i++) {
      ObjId id = iter.next();

      PagingToken token = iter.tokenForKey(id);
      ObjId idFromToken = ObjId.objIdFromBytes(token.token());

      soft.assertThat(newArrayList(commitLogic.commitIdLog(commitLogQuery(token, tip, null))))
          .containsExactlyElementsOf(expected.subList(i, expected.size()));

      soft.assertThat(
              newArrayList(commitLogic.commitIdLog(commitLogQuery(token, idFromToken, null))))
          .containsExactlyElementsOf(expected.subList(i, expected.size()));

      soft.assertThat(newArrayList(commitLogic.commitIdLog(commitLogQuery(idFromToken))))
          .containsExactlyElementsOf(expected.subList(i, expected.size()));

      soft.assertThat(
              newArrayList(
                  commitLogic.commitIdLog(commitLogQuery(token, idFromToken, idFromToken))))
          .containsExactlyElementsOf(expected.subList(i, i + 1));

      soft.assertThat(
              newArrayList(commitLogic.commitIdLog(commitLogQuery(null, idFromToken, idFromToken))))
          .containsExactlyElementsOf(expected.subList(i, i + 1));

      soft.assertAll();
    }
  }

  static Stream<Arguments> diff() {
    ObjId id1 = randomObjId();
    ObjId id2 = randomObjId();
    UUID cid1 = randomUUID();
    return Stream.of(
        arguments(
            stdCommit().message("commit 1"), stdCommit().message("commit 2"), emptyList(), null),
        // single ADD in "from"
        arguments(
            stdCommit().addAdds(commitAdd(KEY_ONE, 0, id1, null, cid1)),
            stdCommit(),
            singletonList(diffEntry(KEY_ONE, id1, 0, cid1, null, 0, null)),
            null),
        // single ADD in "from" w/ filter
        arguments(
            stdCommit().addAdds(commitAdd(KEY_ONE, 0, id1, null, cid1)),
            stdCommit(),
            emptyList(),
            (Predicate<StoreKey>) k -> !KEY_ONE.equals(k)),
        // single ADD in "to"
        arguments(
            stdCommit(),
            stdCommit().addAdds(commitAdd(KEY_ONE, 0, id1, null, cid1)),
            singletonList(diffEntry(KEY_ONE, null, 0, null, id1, 0, cid1)),
            null),
        // ADD in "from" + "to" / same key
        arguments(
            stdCommit().addAdds(commitAdd(KEY_ONE, 1, id1, null, cid1)),
            stdCommit().addAdds(commitAdd(KEY_ONE, 2, id2, null, cid1)),
            singletonList(diffEntry(KEY_ONE, id1, 1, cid1, id2, 2, cid1)),
            null),
        // ADD in "from" + "to" / same key w/ filter
        arguments(
            stdCommit().addAdds(commitAdd(KEY_ONE, 1, id1, null, cid1)),
            stdCommit().addAdds(commitAdd(KEY_ONE, 2, id2, null, cid1)),
            emptyList(),
            (Predicate<StoreKey>) k -> !KEY_ONE.equals(k)),
        // ADD in "from" + "to" / key in commit 1 "lower"
        arguments(
            stdCommit().addAdds(commitAdd(KEY_AAA, 1, id1, null, cid1)),
            stdCommit().addAdds(commitAdd(KEY_ONE, 2, id2, null, cid1)),
            asList(
                diffEntry(KEY_AAA, id1, 1, cid1, null, 0, null),
                diffEntry(KEY_ONE, null, 0, null, id2, 2, cid1)),
            null),
        // ADD in "from" + "to" / key in commit 1 "lower" w/ filter
        arguments(
            stdCommit().addAdds(commitAdd(KEY_AAA, 1, id1, null, cid1)),
            stdCommit().addAdds(commitAdd(KEY_ONE, 2, id2, null, cid1)),
            singletonList(diffEntry(KEY_ONE, null, 0, null, id2, 2, cid1)),
            (Predicate<StoreKey>) k -> !KEY_AAA.equals(k)),
        // ADD in "from" + "to" / key in commit 2 "lower"
        arguments(
            stdCommit().addAdds(commitAdd(KEY_ONE, 1, id1, null, cid1)),
            stdCommit().addAdds(commitAdd(KEY_AAA, 2, id2, null, cid1)),
            asList(
                diffEntry(KEY_AAA, null, 0, null, id2, 2, cid1),
                diffEntry(KEY_ONE, id1, 1, cid1, null, 0, null)),
            null));
  }

  @ParameterizedTest
  @MethodSource("diff")
  public void diff(
      CreateCommit.Builder commitBuilder1,
      CreateCommit.Builder commitBuilder2,
      List<DiffEntry> diff,
      Predicate<StoreKey> filter)
      throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    ObjId commitId1 =
        requireNonNull(commitLogic.doCommit(commitBuilder1.build(), emptyList())).id();
    ObjId commitId2 =
        requireNonNull(commitLogic.doCommit(commitBuilder2.build(), emptyList())).id();
    CommitObj commit1 = commitLogic.fetchCommit(commitId1);
    CommitObj commit2 = commitLogic.fetchCommit(commitId2);

    soft.assertThat(commitLogic.diff(diffQuery(commit1, commit2, false, filter)))
        .toIterable()
        .containsExactlyElementsOf(diff);

    soft.assertThat(
            commitLogic.diff(diffQuery(null, commit1, commit2, null, key("000"), false, filter)))
        .toIterable()
        .isEmpty();

    soft.assertThat(
            commitLogic.diff(diffQuery(null, commit1, commit2, key("zzz"), null, false, filter)))
        .toIterable()
        .isEmpty();

    soft.assertThat(
            commitLogic.diff(
                diffQuery(null, commit1, commit2, key("bbb"), key("nnn"), false, filter)))
        .toIterable()
        .isEmpty();

    soft.assertThat(
            commitLogic.diff(diffQuery(null, commit1, commit2, null, key("bbb"), false, filter)))
        .toIterable()
        .containsExactlyElementsOf(
            diff.stream().filter(e -> e.key().equals(KEY_AAA)).collect(Collectors.toList()));

    soft.assertThat(
            commitLogic.diff(
                diffQuery(null, commit1, commit2, key("a"), key("bbb"), false, filter)))
        .toIterable()
        .containsExactlyElementsOf(
            diff.stream().filter(e -> e.key().equals(KEY_AAA)).collect(Collectors.toList()));

    soft.assertThat(
            commitLogic.diff(diffQuery(null, commit1, commit2, key("o"), null, false, filter)))
        .toIterable()
        .containsExactlyElementsOf(
            diff.stream().filter(e -> e.key().equals(KEY_ONE)).collect(Collectors.toList()));

    soft.assertThat(
            commitLogic.diff(diffQuery(null, commit1, commit2, key("o"), key("p"), false, filter)))
        .toIterable()
        .containsExactlyElementsOf(
            diff.stream().filter(e -> e.key().equals(KEY_ONE)).collect(Collectors.toList()));
  }

  @Test
  public void diffWithPaging() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    ObjId idAaa = randomObjId();
    ObjId idBbb = randomObjId();
    ObjId idCcc = randomObjId();
    ObjId idDdd = randomObjId();
    ObjId idEee = randomObjId();
    ObjId idFff = randomObjId();
    UUID cidAaa = randomUUID();
    UUID cidBbb = randomUUID();
    UUID cidCcc = randomUUID();
    UUID cidDdd = randomUUID();
    UUID cidEee = randomUUID();
    UUID cidFff = randomUUID();

    ObjId commitId1 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit()
                        .addAdds(commitAdd(KEY_AAA, 0, idAaa, null, cidAaa))
                        .addAdds(commitAdd(key("ccc"), 0, idCcc, null, cidCcc))
                        .addAdds(commitAdd(key("eee"), 0, idEee, null, cidEee))
                        .build(),
                    emptyList()))
            .id();
    ObjId commitId2 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit()
                        .addAdds(commitAdd(key("bbb"), 0, idBbb, null, cidBbb))
                        .addAdds(commitAdd(key("ddd"), 0, idDdd, null, cidDdd))
                        .addAdds(commitAdd(key("fff"), 0, idFff, null, cidFff))
                        .build(),
                    emptyList()))
            .id();
    CommitObj commit1 = commitLogic.fetchCommit(commitId1);
    CommitObj commit2 = commitLogic.fetchCommit(commitId2);

    List<DiffEntry> diffs =
        asList(
            diffEntry(KEY_AAA, idAaa, 0, cidAaa, null, 0, null),
            diffEntry(key("bbb"), null, 0, null, idBbb, 0, cidBbb),
            diffEntry(key("ccc"), idCcc, 0, cidCcc, null, 0, null),
            diffEntry(key("ddd"), null, 0, null, idDdd, 0, cidDdd),
            diffEntry(key("eee"), idEee, 0, cidEee, null, 0, null),
            diffEntry(key("fff"), null, 0, null, idFff, 0, cidFff));

    soft.assertThat(commitLogic.diff(diffQuery(commit1, commit2, false, null)))
        .toIterable()
        .containsExactlyElementsOf(diffs);

    PagedResult<DiffEntry, StoreKey> iter =
        commitLogic.diff(diffQuery(commit1, commit2, false, null));
    soft.assertThat(iter.tokenForKey(null)).isEqualTo(emptyPagingToken());
    for (int offset = 0; iter.hasNext(); offset++) {
      DiffEntry entry = iter.next();
      soft.assertThat(entry).isEqualTo(diffs.get(offset));

      PagingToken token = iter.tokenForKey(entry.key());
      soft.assertThat(token).isNotEqualTo(emptyPagingToken());
      soft.assertThat(commitLogic.diff(diffQuery(token, commit1, commit2, null, null, false, null)))
          .toIterable()
          .containsExactlyElementsOf(diffs.subList(offset, diffs.size()));
    }
  }

  @Test
  public void diffToCreateCommit() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    ObjId idAaa = randomObjId();
    ObjId idBbb = randomObjId();
    ObjId idCcc = randomObjId();
    ObjId idCcc2 = randomObjId();
    ObjId idDdd = randomObjId();
    ObjId idEee = randomObjId();
    ObjId commitId1 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit()
                        .addAdds(commitAdd(KEY_AAA, 0, idAaa, null, null))
                        .addAdds(commitAdd(key("bbb"), 0, idBbb, null, null))
                        .addAdds(commitAdd(key("ccc"), 0, idCcc, null, null))
                        .build(),
                    emptyList()))
            .id();
    ObjId commitId2 =
        requireNonNull(
                commitLogic.doCommit(
                    stdCommit()
                        .parentCommitId(commitId1)
                        .addAdds(commitAdd(key("ccc"), 0, idCcc2, idCcc, null))
                        .addAdds(commitAdd(key("ddd"), 0, idDdd, null, null))
                        .addAdds(commitAdd(key("eee"), 0, idEee, null, null))
                        .addRemoves(commitRemove(key("bbb"), 0, idBbb, null))
                        .build(),
                    emptyList()))
            .id();
    CommitObj commit1 = commitLogic.fetchCommit(commitId1);
    CommitObj commit2 = commitLogic.fetchCommit(commitId2);

    CreateCommit.Builder createCommit = stdCommit();
    soft.assertThat(
            commitLogic.diffToCreateCommit(
                commitLogic.diff(diffQuery(commit1, commit2, false, null)), createCommit))
        .isSameAs(createCommit);

    soft.assertThat(createCommit.build())
        .extracting(CreateCommit::adds, CreateCommit::removes)
        .containsExactly(
            asList(
                commitAdd(key("ccc"), 0, idCcc2, idCcc, null),
                commitAdd(key("ddd"), 0, idDdd, null, null),
                commitAdd(key("eee"), 0, idEee, null, null)),
            singletonList(commitRemove(key("bbb"), 0, idBbb, null)));
  }

  @Test
  public void headCommit() throws Exception {
    ReferenceLogic refLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);
    ObjId tip =
        requireNonNull(
                commitLogic.doCommit(
                    newCommitBuilder()
                        .headers(EMPTY_COMMIT_HEADERS)
                        .message("msg foo")
                        .parentCommitId(EMPTY_OBJ_ID)
                        .build(),
                    emptyList()))
            .id();
    Reference ref = refLogic.createReference("foo", tip, randomObjId());
    soft.assertThat(commitLogic.headCommit(ref))
        .extracting(CommitObj::message)
        .isEqualTo("msg foo");

    Reference refEmpty = refLogic.createReference("empty", EMPTY_OBJ_ID, randomObjId());
    soft.assertThat(commitLogic.headCommit(refEmpty)).isNull();

    Reference refNotFound = refLogic.createReference("not-found", randomObjId(), null);
    soft.assertThatThrownBy(() -> commitLogic.headCommit(refNotFound))
        .isInstanceOf(ObjNotFoundException.class);
  }

  @Test
  void commitWithModifiedExpectedValue() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);

    StoreKey key = key("a");
    ObjId correctValue = randomObjId();
    ObjId updateValue = randomObjId();
    ObjId otherExpectedValue = randomObjId();

    ObjId tip =
        requireNonNull(
                commitLogic.doCommit(
                    newCommitBuilder()
                        .headers(EMPTY_COMMIT_HEADERS)
                        .message("msg foo")
                        .parentCommitId(EMPTY_OBJ_ID)
                        .addAdds(commitAdd(key, 0, correctValue, null, null))
                        .build(),
                    emptyList()))
            .id();

    // Put the correct expected value into the Commit-ADD operation, but "break" it in the callback
    // by adding a wrong expected value-ID.
    soft.assertThatThrownBy(
            () ->
                commitLogic.buildCommitObj(
                    stdCommit()
                        .parentCommitId(tip)
                        .addAdds(commitAdd(key, 0, updateValue, correctValue, null))
                        .build(),
                    c -> CONFLICT,
                    (k, v) -> {},
                    (action, storeKey, currentId) -> otherExpectedValue,
                    NO_VALUE_REPLACEMENT))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: VALUE_DIFFERS:a")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts, list(CommitConflict.class))
        .hasSize(1)
        .first()
        .extracting(
            CommitConflict::conflictType,
            CommitConflict::key,
            c -> requireNonNull(c.existing()).value(),
            c -> requireNonNull(c.op()).value())
        .containsExactly(VALUE_DIFFERS, key, correctValue, updateValue);

    // Put a wrong expected value into the Commit-ADD operation, but "fix" it in the callback.
    CommitObj commit =
        commitLogic.buildCommitObj(
            stdCommit()
                .parentCommitId(tip)
                .addAdds(commitAdd(key, 0, updateValue, otherExpectedValue, null))
                .build(),
            c -> CONFLICT,
            (k, v) -> {},
            (action, storeKey, currentId) -> correctValue,
            NO_VALUE_REPLACEMENT);

    soft.assertThat(indexesLogic.buildCompleteIndex(commit, Optional.empty()).get(key))
        .isNotNull()
        .extracting(StoreIndexElement::content)
        .extracting(CommitOp::value)
        .isEqualTo(updateValue);
  }

  @Test
  void commitWithModifiedCommittedValue() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);

    StoreKey key = key("a");
    ObjId initialValue = randomObjId();
    ObjId unusedUpdateValue = randomObjId();
    ObjId updateValue = randomObjId();

    ObjId tip =
        requireNonNull(
                commitLogic.doCommit(
                    newCommitBuilder()
                        .headers(EMPTY_COMMIT_HEADERS)
                        .message("msg foo")
                        .parentCommitId(EMPTY_OBJ_ID)
                        .addAdds(commitAdd(key, 0, initialValue, null, null))
                        .build(),
                    emptyList()))
            .id();

    CommitObj commit =
        commitLogic.buildCommitObj(
            stdCommit()
                .parentCommitId(tip)
                .addAdds(commitAdd(key, 0, unusedUpdateValue, initialValue, null))
                .build(),
            c -> CONFLICT,
            (k, v) -> {},
            NO_VALUE_REPLACEMENT,
            (action, storeKey, commitId) -> updateValue);

    soft.assertThat(indexesLogic.buildCompleteIndex(commit, Optional.empty()).get(key))
        .isNotNull()
        .extracting(StoreIndexElement::content)
        .extracting(CommitOp::value)
        .isEqualTo(updateValue);
  }

  static Stream<Arguments> commitsAndBranches() {
    return Stream.of(Arguments.of(5, 5), Arguments.of(11, 3));
  }

  @ParameterizedTest
  @MethodSource("commitsAndBranches")
  void identifyReferencedAndUnreferencedHeads(int numBranches, int numCommits) throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    Set<ObjId> forkPoints = new HashSet<>();
    Set<ObjId> heads = new HashSet<>();

    prepareReferences(commitLogic, numCommits, numBranches, heads::add, forkPoints::add);

    HeadsAndForkPoints headsAndForkPoints = commitLogic.identifyAllHeadsAndForkPoints(100, e -> {});

    soft.assertThat(headsAndForkPoints.getHeads()).containsExactlyInAnyOrderElementsOf(heads);
    soft.assertThat(headsAndForkPoints.getForkPoints())
        .containsExactlyInAnyOrderElementsOf(forkPoints);
  }

  private void prepareReferences(
      CommitLogic commitLogic,
      int numCommits,
      int numBranches,
      Consumer<ObjId> addHead,
      Consumer<ObjId> addForkPoint)
      throws Exception {
    ObjId root = addCommits(commitLogic, numCommits, EMPTY_OBJ_ID, 0);
    addForkPoint.accept(root);

    for (int branchNum = 1; branchNum <= numBranches; branchNum++) {
      ObjId head = addCommits(commitLogic, numCommits, root, branchNum);
      addHead.accept(head);
    }
  }

  private ObjId addCommits(CommitLogic commitLogic, int numCommits, ObjId head, int branchNum)
      throws Exception {
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      CommitObj commit =
          commitLogic.doCommit(
              newCommitBuilder()
                  .headers(EMPTY_COMMIT_HEADERS)
                  .parentCommitId(head)
                  .message(
                      "commit #" + commitNum + " of " + numCommits + " - unifier: " + branchNum)
                  .build(),
              emptyList());
      checkState(commit != null);
      head = commit.id();
    }

    return head;
  }

  //

  /**
   * Create a couple of commits that use reference indexes (spilled out to external objects),
   * corrupt those in various ways.
   */
  @Test
  void corruptWithReferenceIndex(
      @NessieStoreConfig(name = CONFIG_MAX_INCREMENTAL_INDEX_SIZE, value = "1024")
          @NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "1024")
          @NessiePersist
          Persist persist)
      throws Exception {

    ReferenceLogic referenceLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);
    ConsistencyLogic consistencyLogic = consistencyLogic(persist);

    ObjId parentCommitId = EMPTY_OBJ_ID;
    Reference branch = referenceLogic.createReference("refs/heads/corrupt", parentCommitId, null);

    BiFunction<Integer, Integer, StoreKey> contentKey =
        (c, k) -> StoreKey.key("commit-" + c + "-key-" + k);

    Deque<CommitObj> commits = new ArrayDeque<>();
    // Using 'Tuple's here, because the 'CommitObj's don't need to be equal (and we are only
    // interested in whether the commit is not null)
    Deque<Tuple> expected = new ArrayDeque<>();
    expected.add(tuple(parentCommitId, false, true, true));
    int numCommits = 10;
    int keysPerCommit = 50;
    ContentValueObj lastContent = null;
    for (int c = 0; c < numCommits; c++) {
      CreateCommit.Builder commit =
          newCommitBuilder()
              .headers(EMPTY_COMMIT_HEADERS)
              .message("commit " + c)
              .parentCommitId(parentCommitId);
      List<Obj> objects = new ArrayList<>();
      for (int k = 0; k < keysPerCommit; k++) {
        UUID contentId = randomUUID();
        ContentValueObj value =
            contentValue(contentId.toString(), 0, ByteString.copyFromUtf8("value-" + c + "-" + k));
        objects.add(value);
        lastContent = value;
        commit.addAdds(commitAdd(contentKey.apply(c, k), 0, value.id(), null, contentId));
      }
      CommitObj newCommit = requireNonNull(commitLogic.doCommit(commit.build(), objects));
      commits.addFirst(newCommit);
      branch = persist.updateReferencePointer(branch, newCommit.id());
      expected.addFirst(tuple(newCommit.id(), true, true, true));
      parentCommitId = newCommit.id();
    }

    List<ConsistencyLogic.CommitStatus> commitStatuses = new ArrayList<>();
    consistencyLogic.checkReference(branch, commitStatuses::add);
    soft.assertThat(commitStatuses)
        .map(
            cs ->
                tuple(
                    cs.id(),
                    cs.commit() != null,
                    cs.indexObjectsAvailable(),
                    cs.contentObjectsAvailable()))
        .containsExactlyElementsOf(expected);

    // Delete a content object of latest commit --> contents inconsistent

    persist.deleteObj(lastContent.id());

    Tuple curr = expected.removeFirst();
    expected.addFirst(tuple(curr.toList().get(0), curr.toList().get(1) != null, true, false));

    commitStatuses = new ArrayList<>();
    consistencyLogic.checkReference(branch, commitStatuses::add);
    soft.assertThat(commitStatuses)
        .map(
            cs ->
                tuple(
                    cs.id(),
                    cs.commit() != null,
                    cs.indexObjectsAvailable(),
                    cs.contentObjectsAvailable()))
        .containsExactlyElementsOf(expected);

    // Delete reference index object of latest commit --> no more index and no more content objects

    CommitObj latestCommit = requireNonNull(commitLogic.fetchCommit(commits.getFirst().id()));
    soft.assertThat(latestCommit).extracting(CommitObj::referenceIndex).isNotNull();
    persist.deleteObj(requireNonNull(latestCommit.referenceIndex()));

    curr = expected.removeFirst();
    expected.addFirst(tuple(curr.toList().get(0), curr.toList().get(1) != null, false, false));

    commitStatuses = new ArrayList<>();
    consistencyLogic.checkReference(branch, commitStatuses::add);
    soft.assertThat(commitStatuses)
        .map(
            cs ->
                tuple(
                    cs.id(),
                    cs.commit() != null,
                    cs.indexObjectsAvailable(),
                    cs.contentObjectsAvailable()))
        .containsExactlyElementsOf(expected);

    // Delete commit object of latest commit --> commit no longer available

    persist.deleteObj(latestCommit.id());

    curr = expected.removeFirst();
    expected.addFirst(tuple(curr.toList().get(0), false, false, false));
    commitStatuses = new ArrayList<>();
    consistencyLogic.checkReference(branch, commitStatuses::add);
    soft.assertThat(commitStatuses)
        .map(
            cs ->
                tuple(
                    cs.id(),
                    cs.commit() != null,
                    cs.indexObjectsAvailable(),
                    cs.contentObjectsAvailable()))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void corruptIntRefsIndex(
      @NessieStoreConfig(name = CONFIG_MAX_INCREMENTAL_INDEX_SIZE, value = "1024")
          @NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "1024")
          @NessiePersist
          Persist persist)
      throws Exception {
    ReferenceLogic referenceLogic = referenceLogic(persist);
    ConsistencyLogic consistencyLogic = consistencyLogic(persist);

    List<Reference> createdReferences = new ArrayList<>();
    createdReferences.add(persist.fetchReference("refs/heads/main"));

    Reference refRefs;
    CommitObj refRefsHead;

    // Create references until there's a spilled-out index
    ObjId refsUntilRefIndex;
    for (int i = 0; ; i++) {
      Reference ref =
          referenceLogic.createReference(
              "refs/heads/ref-"
                  + i
                  + "-a-1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
              EMPTY_OBJ_ID,
              null);
      createdReferences.add(ref);

      refRefs = persist.fetchReference(InternalRef.REF_REFS.name());
      refRefsHead =
          persist.fetchTypedObj(
              requireNonNull(refRefs, "internal refs missing").pointer(),
              StandardObjType.COMMIT,
              CommitObj.class);
      if (refRefsHead.referenceIndex() != null) {
        refsUntilRefIndex = refRefsHead.referenceIndex();
        break;
      }
    }

    List<Reference> lostReferences = new ArrayList<>();

    // Create more references until the spilled-out index changes
    for (int i = 0; ; i++) {
      Reference ref =
          referenceLogic.createReference(
              "refs/heads/ref-"
                  + i
                  + "-b-1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
              EMPTY_OBJ_ID,
              null);

      refRefs = persist.fetchReference(InternalRef.REF_REFS.name());
      refRefsHead =
          persist.fetchTypedObj(
              requireNonNull(refRefs, "internal refs missing").pointer(),
              StandardObjType.COMMIT,
              CommitObj.class);
      if (!Objects.equals(refsUntilRefIndex, refRefsHead.referenceIndex())) {
        lostReferences.add(ref);
        break;
      }
      createdReferences.add(ref);
    }

    List<Reference> allReferences =
        Stream.concat(createdReferences.stream(), lostReferences.stream())
            .collect(Collectors.toList());

    // everything's consistent, but assert

    List<ConsistencyLogic.CommitStatus> consistency = new ArrayList<>();
    consistencyLogic.checkReference(refRefs, consistency::add);
    soft.assertThat(consistency)
        .map(
            cs ->
                tuple(
                    cs.commit() != null, cs.indexObjectsAvailable(), cs.contentObjectsAvailable()))
        .allMatch(t -> t.equals(tuple(true, true, true)));

    soft.assertThat(Lists.newArrayList(referenceLogic.queryReferences(referencesQuery("refs/"))))
        .containsExactlyInAnyOrderElementsOf(allReferences);

    // Delete the reference-index object
    persist.deleteObj(requireNonNull(refRefsHead.referenceIndex()));

    soft.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                referenceLogic.queryReferences(referencesQuery("refs/")).forEachRemaining(x -> {}))
        .withMessageContaining("references a reference index, which does not exist");

    consistency = new ArrayList<>();
    consistencyLogic.checkReference(refRefs, consistency::add);
    soft.assertThat(consistency.get(0))
        .extracting(
            cs -> cs.commit() != null,
            ConsistencyLogic.CommitStatus::indexObjectsAvailable,
            ConsistencyLogic.CommitStatus::contentObjectsAvailable)
        .containsExactly(true, false, false);
    soft.assertThat(consistency.subList(1, consistency.size()))
        .map(
            cs ->
                tuple(
                    cs.commit() != null, cs.indexObjectsAvailable(), cs.contentObjectsAvailable()))
        .allMatch(t -> t.equals(tuple(true, true, true)));

    // Delete the references HEAD commit
    persist.deleteObj(refRefsHead.id());

    soft.assertThatRuntimeException()
        .isThrownBy(
            () ->
                referenceLogic.queryReferences(referencesQuery("refs/")).forEachRemaining(x -> {}))
        .withMessageEndingWith("Object with ID %s not found", refRefsHead.id());

    consistency = new ArrayList<>();
    consistencyLogic.checkReference(refRefs, consistency::add);
    soft.assertThat(consistency.get(0))
        .extracting(
            cs -> cs.commit() != null,
            ConsistencyLogic.CommitStatus::indexObjectsAvailable,
            ConsistencyLogic.CommitStatus::contentObjectsAvailable)
        .containsExactly(false, false, false);
    soft.assertThat(consistency.subList(1, consistency.size()))
        .map(
            cs ->
                tuple(
                    cs.commit() != null, cs.indexObjectsAvailable(), cs.contentObjectsAvailable()))
        .allMatch(t -> t.equals(tuple(true, true, true)));

    // re-assign internal int/refs reference
    persist.updateReferencePointer(refRefs, consistency.get(1).id());

    soft.assertThat(Lists.newArrayList(referenceLogic.queryReferences(referencesQuery("refs/"))))
        .containsExactlyInAnyOrderElementsOf(createdReferences);

    for (Reference lostReference : lostReferences) {
      // The existing "references recovery logic" detects that the `Reference` already exists in the
      // 'refs' table, so it "only" adds the missing index entry and commits it to 'int/refs'.
      soft.assertThatThrownBy(
              () ->
                  referenceLogic.createReference(
                      lostReference.name(), lostReference.pointer(), null))
          .isInstanceOf(RefAlreadyExistsException.class);
    }

    soft.assertThat(Lists.newArrayList(referenceLogic.queryReferences(referencesQuery("refs/"))))
        .containsExactlyInAnyOrderElementsOf(allReferences);
  }
}
