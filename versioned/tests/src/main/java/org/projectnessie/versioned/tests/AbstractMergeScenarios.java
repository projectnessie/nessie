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
package org.projectnessie.versioned.tests;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableMergeOp;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;

@SuppressWarnings("unused")
@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractMergeScenarios extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractMergeScenarios(VersionStore store) {
    super(store);
  }

  /**
   * Unrelated branches case. <code><pre>
   * ----B-----D      b2
   *
   * ----A-----C      b1
   * </pre></code>
   */
  @Test
  void noMergeBase() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2");
    Commit c = buildCommit().commitTo("b1");
    Commit d = buildCommit().commitTo("b2");

    soft.assertThatThrownBy(() -> buildMerge().from("b1").merge("b2"))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessageStartingWith("No common ancestor");
  }

  /**
   * Test a simple merge base case. <code><pre>
   *       ----B      b2
   *      /
   * ----A-----C      b1
   * </pre></code>
   *
   * <p>Best merge-base of {@code B} onto {@code C} is {@code A}.
   */
  @Test
  void simpleCase() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit c = buildCommit().commitTo("b1");

    buildMerge().from("b2").assertMergeBase("b1", a);
  }

  /**
   * Merge-again case. <code><pre>
   *       ----B---------E      b2
   *      /          \
   * ----A------C-----D         b1
   * </pre></code>
   *
   * <p>Best merge-base of {@code E} onto {@code D} is {@code B}.
   */
  @Test
  void doubleMerge() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit c = buildCommit().commitTo("b1");
    Commit d = buildMerge().from("b2").merge("b1");
    Commit e = buildCommit().commitTo("b2");

    buildMerge().from("b1").assertMergeBase("b2", b);
  }

  /**
   * Merge-again case. <code><pre>
   *       ----B----D-------F      b2
   *      /             \
   * ----A------C--------E         b1
   * </pre></code>
   *
   * <p>Best merge-base of {@code F} onto {@code E} is {@code D}.
   */
  @Test
  void doubleMerge2() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit c = buildCommit().commitTo("b1");
    Commit d = buildCommit().commitTo("b2");
    Commit e = buildMerge().from("b2").merge("b1");
    Commit f = buildCommit().commitTo("b2");

    buildMerge().from("b1").assertMergeBase("b2", d);
  }

  /**
   * Merge-again case. <code><pre>
   *              ----E----G---------I      b3
   *            /       /           /
   *       ----B----D----------H----        b2
   *      /               /
   * ----A------C--------F                  b1
   * </pre></code>
   *
   * <p>Best merge-base of {@code I} onto {@code F} is {@code F}.
   */
  @Test
  void multiMerge1() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit c = buildCommit().commitTo("b1");
    Commit d = buildCommit().commitTo("b2");
    Commit e = buildCommit().commitToNewBranch("b3", "b2", b);
    Commit f = buildCommit().commitTo("b1");
    Commit g = buildMerge().from("b2").merge("b3");
    Commit h = buildMerge().from("b1").merge("b2");
    Commit i = buildMerge().from("b1").merge("b3");

    buildMerge().from("b3").assertMergeBase("b1", f);
  }

  /**
   * Merge-again case. <code><pre>
   * A ----- C - E - F - G               b1
   *  \- B -/           /                b2
   *   \    \--------- / ---\
   *    \- D ---------/------ H - J      b3
   * </pre></code>
   *
   * <p>Best merge-base of {@code J} onto {@code G} is {@code D}.
   */
  @Test
  void multiMerge2() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit c = buildMerge().from("b2").merge("b1");
    Commit d = buildCommit().commitToNewBranch("b3", "b1", a);
    Commit e = buildCommit().commitTo("b1");
    Commit f = buildCommit().commitTo("b1");
    Commit g = buildMerge().from("b3").merge("b1");
    Commit h = buildMerge().from("b2").merge("b3");
    Commit j = buildCommit().commitTo("b3");

    buildMerge().from("b3").assertMergeBase("b1", d);
  }

  /**
   * Cross-merge case. <code><pre>
   *        ----B--------D----F      b2
   *       /       \ /
   *      /        / \
   * ----A------C--------E----G      b1
   * </pre></code>
   *
   * <p>Merge-base outcome for {@code F} onto {@code G} is either {@code B} or {@code C}.
   */
  @Test
  void afterCrossMerge() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit c = buildCommit().commitTo("b1");
    Commit d = buildMerge().from("b1", c).merge("b2");
    Commit e = buildMerge().from("b2", b).merge("b1");
    Commit f = buildCommit().commitTo("b2");
    Commit g = buildCommit().commitTo("b1");

    buildMerge().dryRun().from("b2").assertMergeBase("b1", b);
    buildMerge().dryRun().from("b1").assertMergeBase("b2", c);
  }

  /**
   * Merge two previous merges. <code><pre>
   *        B-----------------I      b3
   *       /                 /
   *      / F-----------G   /        b2
   *     | /           /   /
   *     |/           /   /
   * ----A---C---D---E---H           b1
   * </pre></code>
   *
   * <p>Best merge-base of {@code I} onto {@code G} is {@code E}.
   */
  @Test
  void nestedBranches() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b3", "b1", a);
    Commit c = buildCommit().commitTo("b1");
    Commit d = buildCommit().commitTo("b1");
    Commit e = buildCommit().commitTo("b1");
    Commit f = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit g = buildMerge().from("b1", e).merge("b2");
    Commit h = buildCommit().commitTo("b1");
    Commit i = buildMerge().from("b1").merge("b3");

    buildMerge().dryRun().from("b3").assertMergeBase("b2", e);
  }

  /**
   * Merge two previous merges. <code><pre>
   *      ---- B ---- E ---- G ---- I ---- K      b2
   *     /      /             /
   *    /      /             /
   *   A ---- C ---- D ---- F ---- H ---- J       b1
   * </pre></code>
   *
   * <p>Best merge-base of {@code K} onto {@code J} is {@code F}.
   */
  @Test
  void featureBranch() throws VersionStoreException {
    Commit a = buildCommit().commitToNewBranch("b1");
    Commit b = buildCommit().commitToNewBranch("b2", "b1", a);
    Commit c = buildCommit().commitTo("b1");
    Commit d = buildCommit().commitTo("b1");
    Commit e = buildMerge().from("b1", c).merge("b2");
    Commit f = buildCommit().commitTo("b1");
    Commit g = buildCommit().commitTo("b2");
    Commit h = buildCommit().commitTo("b1");
    Commit i = buildMerge().from("b1", f).merge("b2");
    Commit j = buildCommit().commitTo("b1");
    Commit k = buildCommit().commitTo("b2");

    buildMerge().from("b2").assertMergeBase("b1", f);
  }

  private MergeBuilder buildMerge() {
    return new MergeBuilder();
  }

  private CommitBuilder buildCommit() {
    return new CommitBuilder();
  }

  private String commitMessage() {
    char c = (char) ('a' + commitCount.getAndIncrement());
    return Character.toString(c);
  }

  private final class MergeBuilder {
    final ImmutableMergeOp.Builder merge = VersionStore.MergeOp.builder();

    @CanIgnoreReturnValue
    MergeBuilder from(String from) {
      Hash head = requireNonNull(branches.get(from), "Branch " + from + " not created");
      merge.fromRef(BranchName.of(from)).fromHash(head);
      return this;
    }

    @CanIgnoreReturnValue
    MergeBuilder from(String from, Commit fromCommit) {
      merge.fromRef(BranchName.of(from)).fromHash(fromCommit.getHash());
      return this;
    }

    @CanIgnoreReturnValue
    MergeBuilder dryRun() {
      merge.dryRun(true);
      return this;
    }

    Commit merge(String target) throws VersionStoreException {
      return doMerge(target).getCreatedCommits().get(0);
    }

    Hash mergeReturnMergeBase(String target) throws VersionStoreException {
      return doMerge(target).getCommonAncestor();
    }

    void assertMergeBase(String target, Commit expected) throws VersionStoreException {
      Hash mergeBase = mergeReturnMergeBase(target);
      String expectedCommitMessage = commits.get(expected.getHash());
      String receivedCommitMessage = commits.get(mergeBase);
      soft.assertThat(mergeBase)
          .describedAs(
              "Expected commit '%s', but got commit '%s' for merge from %s onto %s (expected hash %s, got hash %s)",
              expectedCommitMessage,
              receivedCommitMessage,
              merge.build().fromRef().getName(),
              target,
              expected.getHash(),
              mergeBase)
          .isEqualTo(expected.getHash());
    }

    MergeResult doMerge(String target) throws VersionStoreException {
      Hash head = requireNonNull(branches.get(target), "Branch " + target + " not created");
      VersionStore.MergeOp op =
          merge.toBranch(BranchName.of(target)).expectedHash(Optional.of(head)).build();
      MergeResult result = store().merge(op);
      if (!op.dryRun()) {
        head = result.getCreatedCommits().get(0).getHash();
        branches.put(target, head);
        commits.put(head, commitMessage());
      }
      return result;
    }
  }

  final Map<String, Hash> branches = new HashMap<>();
  final Map<Hash, String> commits = new HashMap<>();
  final Map<ContentKey, String> tableIds = new HashMap<>();
  final AtomicInteger commitCount = new AtomicInteger();
  final AtomicLong unique = new AtomicLong();

  @BeforeEach
  void setup() {
    tableIds.clear();
    commits.clear();
    commitCount.set(0);
    unique.set(0);
  }

  private final class CommitBuilder {
    final CommitMeta.Builder meta;
    final List<Operation> operations = new ArrayList<>();

    CommitBuilder() {
      meta = CommitMeta.builder();
    }

    @CanIgnoreReturnValue
    CommitBuilder createTable(String name) {
      operations.add(
          Put.of(
              ContentKey.fromPathString(name),
              IcebergTable.of(name, unique.incrementAndGet(), 1, 2, 3)));
      return this;
    }

    @CanIgnoreReturnValue
    CommitBuilder update(String name) {
      ContentKey key = ContentKey.fromPathString(name);
      String cid = requireNonNull(tableIds.get(key), "Table " + name + " not yet created");
      operations.add(Put.of(key, IcebergTable.of(name, unique.incrementAndGet(), 1, 2, 3, cid)));
      return this;
    }

    @CanIgnoreReturnValue
    CommitBuilder delete(String name) {
      ContentKey key = ContentKey.fromPathString(name);
      requireNonNull(tableIds.get(key), "Table " + name + " not yet created");
      operations.add(Delete.of(key));
      return this;
    }

    Commit commitToNewBranch(String branch) throws VersionStoreException {
      return commitToNewBranch(branch, null, null);
    }

    Commit commitToNewBranch(String branch, String from, Commit fromCommit)
        throws VersionStoreException {
      if (branches.containsKey(branch)) {
        throw new IllegalStateException("Branch " + branch + " already created");
      }

      Hash origin = null;
      if (from != null) {
        origin = fromCommit.getHash();
        requireNonNull(branches.get(from), "Branch " + from + " not yet created");
      }

      Hash head = store().create(BranchName.of(branch), Optional.ofNullable(origin)).getHash();
      branches.put(branch, head);

      return commitTo(branch);
    }

    Commit commitTo(String branch) throws VersionStoreException {
      String msg = commitMessage();
      meta.message(msg);

      if (operations.isEmpty()) {
        createTable("table created on " + msg);
      }

      Hash head = branches.get(branch);
      requireNonNull(head, "Branch " + branch + " not created");
      CommitResult result =
          store()
              .commit(
                  BranchName.of(branch),
                  Optional.of(head),
                  meta.build(),
                  operations,
                  v -> {},
                  tableIds::put);

      head = result.getCommitHash();
      commits.put(head, msg);
      branches.put(branch, head);

      return result.getCommit();
    }
  }
}
