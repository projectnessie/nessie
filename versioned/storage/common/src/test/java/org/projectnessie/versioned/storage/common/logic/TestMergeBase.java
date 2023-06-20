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
package org.projectnessie.versioned.storage.common.logic;

import static org.projectnessie.versioned.storage.common.logic.CommitLogicImpl.NO_COMMON_ANCESTOR_IN_PARENTS_OF;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.ALL_FLAGS;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.CANDIDATE;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.COMMIT_A;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.COMMIT_B;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMergeBase {
  @InjectSoftAssertions protected SoftAssertions soft;
  private MockRepo repo;

  @Test
  void noCommits() {
    soft.assertThatThrownBy(
            () ->
                MergeBase.builder()
                    .targetCommitId(EMPTY_OBJ_ID)
                    .fromCommitId(EMPTY_OBJ_ID)
                    .loadCommit(x -> null)
                    .build()
                    .identifyMergeBase())
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
  }

  @Test
  void sameCommits() {
    CommitObj a = repo.add(initialCommit());
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(a.id())
                .fromCommitId(a.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a.id());
  }

  /**
   * Unrelated branches case. <code><pre>
   * ----B-----D
   *
   * ----A-----C
   * </pre></code>
   */
  @Test
  void noMergeBase() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(initialCommit());
    CommitObj c = repo.add(buildCommit("c", a));
    CommitObj d = repo.add(buildCommit("d", b));

    soft.assertThatThrownBy(
            () ->
                MergeBase.builder()
                    .loadCommit(repo::loadCommit)
                    .targetCommitId(c.id())
                    .fromCommitId(d.id())
                    .build()
                    .identifyMergeBase())
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
    soft.assertThatThrownBy(
            () ->
                MergeBase.builder()
                    .loadCommit(repo::loadCommit)
                    .respectMergeParents(false)
                    .targetCommitId(c.id())
                    .fromCommitId(d.id())
                    .build()
                    .identifyMergeBase())
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageStartingWith(NO_COMMON_ANCESTOR_IN_PARENTS_OF);
  }

  /**
   * Test a simple merge base case. <code><pre>
   *       ----B
   *      /
   * ----A-----C
   * </pre></code>
   *
   * <p>Best merge-base of {@code B} onto {@code C} is {@code A}.
   */
  @Test
  void simpleCase() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj c = repo.add(buildCommit("c", a));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(c.id())
                .fromCommitId(b.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a.id());
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(c.id())
                .fromCommitId(b.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a.id());
  }

  /**
   * Merge-again case. <code><pre>
   *       ----B---------E
   *      /          \
   * ----A------C-----D
   * </pre></code>
   *
   * <p>Best merge-base of {@code E} onto {@code D} is {@code B}.
   */
  @Test
  void doubleMerge() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj c = repo.add(buildCommit("c", a));
    CommitObj d = repo.add(buildCommit("d", c).addSecondaryParents(b.id()));
    CommitObj e = repo.add(buildCommit("e", b));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(d.id())
                .fromCommitId(e.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(b.id());
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(d.id())
                .fromCommitId(e.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a.id());
  }

  /**
   * Merge-again case. <code><pre>
   *       ----B----D-------F
   *      /             \
   * ----A------C--------E
   * </pre></code>
   *
   * <p>Best merge-base of {@code F} onto {@code E} is {@code D}.
   */
  @Test
  void doubleMerge2() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj c = repo.add(buildCommit("c", a));
    CommitObj d = repo.add(buildCommit("d", b));
    CommitObj e = repo.add(buildCommit("e", c).addSecondaryParents(d.id()));
    CommitObj f = repo.add(buildCommit("f", d));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(e.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(d.id());
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(e.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a.id());
  }

  /**
   * Merge-again case. <code><pre>
   *              ----E----G---------I
   *            /       /           /
   *       ----B----D----------H----
   *      /               /
   * ----A------C--------F
   * </pre></code>
   *
   * <p>Best merge-base of {@code I} onto {@code F} is {@code F}.
   */
  @Test
  void multiMerge1() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj c = repo.add(buildCommit("c", a));
    CommitObj d = repo.add(buildCommit("d", b));
    CommitObj e = repo.add(buildCommit("e", b));
    CommitObj f = repo.add(buildCommit("f", c));
    CommitObj g = repo.add(buildCommit("g", e).addSecondaryParents(d.id()));
    CommitObj h = repo.add(buildCommit("h", d).addSecondaryParents(f.id()));
    CommitObj i = repo.add(buildCommit("i", g).addSecondaryParents(h.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(f.id())
                .fromCommitId(i.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(f.id());
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(f.id())
                .fromCommitId(i.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a.id());
  }

  /**
   * Merge-again case. <code><pre>
   * A ----- C - E - F - G
   *  \- B -/           /
   *   \    \--------- / ---\
   *    \- D ---------/------ H - J
   * </pre></code>
   *
   * <p>Best merge-base of {@code J} onto {@code G} is {@code D}.
   */
  @Test
  void multiMerge2() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj c = repo.add(buildCommit("c", a).addSecondaryParents(b.id()));
    CommitObj d = repo.add(buildCommit("d", a));
    CommitObj e = repo.add(buildCommit("e", c));
    CommitObj f = repo.add(buildCommit("f", e));
    CommitObj g = repo.add(buildCommit("g", f).addSecondaryParents(d.id()));
    CommitObj h = repo.add(buildCommit("h", d).addSecondaryParents(b.id()));
    CommitObj j = repo.add(buildCommit("j", h));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(g.id())
                .fromCommitId(j.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(d.id());
  }

  /**
   * Cross-merge case. <code><pre>
   *        ----B--------D----F
   *       /       \ /
   *      /        / \
   * ----A------C--------E----G
   * </pre></code>
   *
   * <p>Merge-base outcome for {@code F} onto {@code G} is either {@code B} or {@code C}.
   */
  @Test
  void afterCrossMerge() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj c = repo.add(buildCommit("c", a));
    CommitObj d = repo.add(buildCommit("d", b).addSecondaryParents(c.id()));
    CommitObj e = repo.add(buildCommit("e", c).addSecondaryParents(b.id()));
    CommitObj f = repo.add(buildCommit("f", d));
    CommitObj g = repo.add(buildCommit("g", e));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(g.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(b.id());
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(f.id())
                .fromCommitId(g.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(c.id());
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(g.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a.id());
  }

  /**
   * Merge two previous merges. <code><pre>
   *        B-----------------H
   *       /                 /
   *      / ------------F   /
   *     | /           /   /
   *     |/           /   /
   * ----A---C---D---E---G
   * </pre></code>
   *
   * <p>Best merge-base of {@code H} onto {@code F} is {@code E}.
   */
  @Test
  void nestedBranches() {
    CommitObj a = repo.add(initialCommit("a"));
    CommitObj c = repo.add(buildCommit("c", a));
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj d = repo.add(buildCommit("d", c));
    CommitObj e = repo.add(buildCommit("e", d));
    CommitObj g = repo.add(buildCommit("g", e));
    CommitObj f = repo.add(buildCommit("f", a).addSecondaryParents(e.id()));
    CommitObj h = repo.add(buildCommit("h", b).addSecondaryParents(g.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(f.id())
                .fromCommitId(h.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(e.id());
  }

  /**
   * Merge two previous merges. <code><pre>
   *      ---- B ---- E ---- G ---- I ---- K
   *     /      /             /
   *    /      /             /
   *   A ---- C ---- D ---- F ---- H ---- J
   * </pre></code>
   *
   * <p>Best merge-base of {@code K} onto {@code J} is {@code F}.
   */
  @Test
  void featureBranch() {
    CommitObj a = repo.add(initialCommit("a"));
    CommitObj b = repo.add(buildCommit("b", a));
    CommitObj c = repo.add(buildCommit("c", a));
    CommitObj d = repo.add(buildCommit("d", c));
    CommitObj e = repo.add(buildCommit("e", b).addSecondaryParents(c.id()));
    CommitObj f = repo.add(buildCommit("f", d));
    CommitObj g = repo.add(buildCommit("g", e));
    CommitObj h = repo.add(buildCommit("h", f));
    CommitObj i = repo.add(buildCommit("i", g).addSecondaryParents(f.id()));
    CommitObj j = repo.add(buildCommit("j", h));
    CommitObj k = repo.add(buildCommit("k", i));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(j.id())
                .fromCommitId(k.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(f.id());
  }

  @Test
  void shallowCommitFlags() {
    ShallowCommit commit = new ShallowCommit(randomObjId(), new ObjId[] {randomObjId()}, 1L);

    soft.assertThat(commit.isAnyFlagSet(ALL_FLAGS)).isFalse();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE)).isFalse();

    commit.setCandidate();

    soft.assertThat(commit.isAnyFlagSet(ALL_FLAGS)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE)).isTrue();

    commit.clearCandidate();

    soft.assertThat(commit.flags()).isEqualTo(0);

    int multiple = CANDIDATE | COMMIT_A | COMMIT_B;
    commit.setCandidate();
    commit.setCommitA();
    commit.setCommitB();

    soft.assertThat(commit.isAnyFlagSet(multiple)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE | COMMIT_A)).isTrue();

    commit.clearCandidate();

    soft.assertThat(commit.isAnyFlagSet(multiple)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(COMMIT_A)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(COMMIT_A | COMMIT_B)).isTrue();
  }

  @BeforeEach
  void setupRepo() {
    repo = new MockRepo();
  }

  static class MockRepo {
    final Map<ObjId, CommitObj> commits = new HashMap<>();

    CommitObj add(CommitObj.Builder c) {
      int num = commits.size();
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.putInt(num);
      bb.flip();

      c.created(num).id(ObjId.objIdFromByteBuffer(bb));
      CommitObj commit = c.build();
      commits.put(commit.id(), commit);
      return commit;
    }

    public CommitObj loadCommit(ObjId id) {
      return commits.get(id);
    }
  }

  static CommitObj.Builder initialCommit() {
    return initialCommit("initial");
  }

  static CommitObj.Builder initialCommit(String name) {
    return buildCommit(name, null).addTail(EMPTY_OBJ_ID);
  }

  static CommitObj.Builder buildCommit(String msg, CommitObj parent) {
    CommitObj.Builder commit =
        commitBuilder()
            .headers(newCommitHeaders().build())
            .message(msg)
            .incrementalIndex(ByteString.empty());
    if (parent != null) {
      commit.seq(parent.seq() + 1).addTail(parent.id());
    } else {
      commit.seq(0);
    }
    return commit;
  }
}
