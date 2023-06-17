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
        .isEqualTo(a);
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
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(b.id()));

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
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(c.id())
                .fromCommitId(b.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a);
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(c.id())
                .fromCommitId(b.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a);
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
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(c.id()).addSecondaryParents(b.id()));
    CommitObj e = repo.add(buildCommit("e").addTail(b.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(d.id())
                .fromCommitId(e.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(b);
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(d.id())
                .fromCommitId(e.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a);
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
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(b.id()));
    CommitObj e = repo.add(buildCommit("e").addTail(c.id()).addSecondaryParents(d.id()));
    CommitObj f = repo.add(buildCommit("f").addTail(d.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(e.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(d);
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(e.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a);
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
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(b.id()));
    CommitObj e = repo.add(buildCommit("e").addTail(b.id()));
    CommitObj f = repo.add(buildCommit("f").addTail(c.id()));
    CommitObj g = repo.add(buildCommit("g").addTail(e.id()).addSecondaryParents(d.id()));
    CommitObj h = repo.add(buildCommit("h").addTail(d.id()).addSecondaryParents(f.id()));
    CommitObj i = repo.add(buildCommit("i").addTail(g.id()).addSecondaryParents(h.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(f.id())
                .fromCommitId(i.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(f);
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(f.id())
                .fromCommitId(i.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a);
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
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()).addSecondaryParents(b.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(a.id()));
    CommitObj e = repo.add(buildCommit("e").addTail(c.id()));
    CommitObj f = repo.add(buildCommit("f").addTail(e.id()));
    CommitObj g = repo.add(buildCommit("g").addTail(f.id()).addSecondaryParents(d.id()));
    CommitObj h = repo.add(buildCommit("h").addTail(d.id()).addSecondaryParents(b.id()));
    CommitObj j = repo.add(buildCommit("j").addTail(h.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(g.id())
                .fromCommitId(j.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(b);
  }

  /**
   * Cross-merge case. <code><pre>
   *        ----B--------D----F
   *       /       \ /
   *      /        / \
   * ----A------C--------E----G
   * </pre></code>
   *
   * <p>Merge-base outcome for {@code F} onto {@code G} is {@code B}. In theory, it could also be
   * {@code C}, but the logic/implementation ensures that it's always {@code B}.
   */
  @Test
  void afterCrossMerge() {
    CommitObj a = repo.add(initialCommit());
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(b.id()).addSecondaryParents(c.id()));
    CommitObj e = repo.add(buildCommit("e").addTail(c.id()).addSecondaryParents(b.id()));
    CommitObj f = repo.add(buildCommit("f").addTail(d.id()));
    CommitObj g = repo.add(buildCommit("g").addTail(e.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(g.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(b);
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(f.id())
                .fromCommitId(g.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(b);
    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .respectMergeParents(false)
                .targetCommitId(g.id())
                .fromCommitId(f.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(a);
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
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(c.id()));
    CommitObj e = repo.add(buildCommit("e").addTail(d.id()));
    CommitObj g = repo.add(buildCommit("g").addTail(e.id()));
    CommitObj f = repo.add(buildCommit("f").addTail(a.id()).addSecondaryParents(e.id()));
    CommitObj h = repo.add(buildCommit("h").addTail(b.id()).addSecondaryParents(g.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(f.id())
                .fromCommitId(h.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(e);
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
    CommitObj b = repo.add(buildCommit("b").addTail(a.id()));
    CommitObj c = repo.add(buildCommit("c").addTail(a.id()));
    CommitObj d = repo.add(buildCommit("d").addTail(c.id()));
    CommitObj e = repo.add(buildCommit("e").addTail(b.id()).addSecondaryParents(c.id()));
    CommitObj f = repo.add(buildCommit("f").addTail(d.id()));
    CommitObj g = repo.add(buildCommit("g").addTail(e.id()));
    CommitObj h = repo.add(buildCommit("h").addTail(f.id()));
    CommitObj i = repo.add(buildCommit("i").addTail(g.id()).addSecondaryParents(f.id()));
    CommitObj j = repo.add(buildCommit("j").addTail(h.id()));
    CommitObj k = repo.add(buildCommit("k").addTail(i.id()));

    soft.assertThat(
            MergeBase.builder()
                .loadCommit(repo::loadCommit)
                .targetCommitId(j.id())
                .fromCommitId(k.id())
                .build()
                .identifyMergeBase())
        .isEqualTo(f);
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
      int seq = commits.size();
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.putInt(seq);
      bb.flip();

      c.created(seq).seq(seq).id(ObjId.objIdFromByteBuffer(bb));
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
    return buildCommit(name).addTail(EMPTY_OBJ_ID);
  }

  static CommitObj.Builder buildCommit(String msg) {
    return commitBuilder()
        .headers(newCommitHeaders().build())
        .message(msg)
        .incrementalIndex(ByteString.empty());
  }
}
