/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.HeadsAndForkPoints;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.ReferencedAndUnreferencedHeads;
import org.projectnessie.versioned.persist.adapter.ReferencesUtil;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

/** Verifies handling of repo-description in the database-adapters. */
public abstract class AbstractCommitLogScan {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractCommitLogScan(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  static Stream<Arguments> commitsAndBranches() {
    return Stream.of(Arguments.of(5, 5), Arguments.of(11, 3));
  }

  @ParameterizedTest
  @MethodSource("commitsAndBranches")
  void scanCommits(int numBranches, int numCommits) throws Exception {
    IntFunction<BranchName> branch = branchNum -> BranchName.of("scanCommits-" + branchNum);

    Set<Hash> commits = new HashSet<>();

    // Create a bunch of references and commits, and then drop some created references
    prepareReferences(
        numCommits, numBranches, branch, (h, r) -> {}, h -> {}, (r, h) -> {}, commits::add);

    try (Stream<CommitLogEntry> entries = databaseAdapter.scanAllCommitLogEntries()) {
      assertThat(entries).map(CommitLogEntry::getHash).containsExactlyInAnyOrderElementsOf(commits);
    }
  }

  @ParameterizedTest
  @MethodSource("commitsAndBranches")
  void identifyReferencedAndUnreferencedHeads(
      int numBranches,
      int numCommits,
      @NessieDbAdapter
          @NessieDbAdapterConfigItem(name = "assumed.wall.clock.drift.micros", value = "0")
          AbstractDatabaseAdapter<?, ?> databaseAdapter)
      throws Exception {
    IntFunction<BranchName> branch = branchNum -> BranchName.of("collectAllHeads-" + branchNum);

    Set<Hash> deletedHeads = new HashSet<>();
    Map<Hash, Set<NamedRef>> liveHeads = new HashMap<>();
    Map<NamedRef, Hash> refHeads = new HashMap<>();

    BiConsumer<Hash, NamedRef> addLive =
        (head, ref) -> liveHeads.computeIfAbsent(head, x -> new HashSet<>()).add(ref);

    prepareReferences(
        numCommits, numBranches, branch, addLive, deletedHeads::add, refHeads::put, h -> {});

    //

    ReferencesUtil referencesUtil = ReferencesUtil.forDatabaseAdapter(databaseAdapter);

    HeadsAndForkPoints headsAndForkPoints =
        referencesUtil.identifyAllHeadsAndForkPoints(100, e -> {});

    ReferencedAndUnreferencedHeads refAndUnref =
        referencesUtil.identifyReferencedAndUnreferencedHeads(headsAndForkPoints);

    assertThat(refAndUnref.getUnreferencedHeads()).isEqualTo(deletedHeads);
    assertThat(refAndUnref.getReferencedHeads()).isEqualTo(liveHeads);

    // Add more commits to some of the remaining branches - simulate that commits were added in
    // the meantime.
    for (int branchNum = 0; branchNum < numBranches; branchNum++) {
      if ((branchNum & 3) == 1) {
        BranchName branchName = branch.apply(branchNum);
        Hash head = refHeads.get(branchName);
        liveHeads.get(head).remove(branchName);
        if (liveHeads.get(head).isEmpty()) {
          liveHeads.remove(head);
        }
        head = addCommits(numCommits, branchName, head, h -> {});
        liveHeads.computeIfAbsent(head, x -> new HashSet<>()).add(branchName);
        refHeads.put(branchName, head);
      }
    }

    refAndUnref = referencesUtil.identifyReferencedAndUnreferencedHeads(headsAndForkPoints);

    assertThat(refAndUnref.getUnreferencedHeads()).isEqualTo(deletedHeads);
    assertThat(refAndUnref.getReferencedHeads()).isEqualTo(liveHeads);
  }

  private void prepareReferences(
      int numCommits,
      int numBranches,
      IntFunction<BranchName> branch,
      BiConsumer<Hash, NamedRef> addLiveHead,
      Consumer<Hash> addDeletedHead,
      BiConsumer<NamedRef, Hash> setRefHead,
      Consumer<Hash> committed)
      throws Exception {
    BranchName mainBranch = BranchName.of("main");
    Hash root = addCommits(numCommits, mainBranch, databaseAdapter.noAncestorHash(), committed);
    addLiveHead.accept(root, mainBranch);

    for (int branchNum = 0; branchNum < numBranches; branchNum++) {
      BranchName branchName = branch.apply(branchNum);
      Hash head = databaseAdapter.create(branchName, root).getHash();
      head = addCommits(numCommits, branchName, head, committed);
      setRefHead.accept(branchName, head);

      // Add a tag + a branch at commits "somewhere" on another ref's commit-log sequence.
      // I.e. the HEAD of these (the tag + the branch) are neither a "head" nor a "fork point".
      try (Stream<CommitLogEntry> l = databaseAdapter.commitLog(head)) {
        Hash tagHead =
            l.skip(1).limit(1).findFirst().orElseThrow(IllegalStateException::new).getHash();
        TagName tagName = TagName.of("tag-" + branchName.getName());
        databaseAdapter.create(tagName, tagHead);
        addLiveHead.accept(tagHead, tagName);

        BranchName otherBranch = BranchName.of("other-" + branchName.getName());
        databaseAdapter.create(otherBranch, tagHead);
        addLiveHead.accept(tagHead, otherBranch);
      }

      if ((branchNum & 1) == 0) {
        addDeletedHead.accept(head);
        databaseAdapter.delete(branchName, Optional.of(head));
        assertThatThrownBy(
                () -> databaseAdapter.namedRef(branchName.getName(), GetNamedRefsParams.DEFAULT))
            .isInstanceOf(ReferenceNotFoundException.class);
      } else {
        addLiveHead.accept(head, branchName);
        assertThatCode(
                () -> databaseAdapter.namedRef(branchName.getName(), GetNamedRefsParams.DEFAULT))
            .doesNotThrowAnyException();
      }
    }
  }

  private Hash addCommits(
      int numCommits, BranchName branchName, Hash head, Consumer<Hash> committed)
      throws ReferenceConflictException, ReferenceNotFoundException {
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      ContentKey key = ContentKey.of("many-commits-" + numCommits);
      ContentId cid = ContentId.of("cid-" + branchName.getName() + "-" + commitNum);
      OnRefOnly c =
          OnRefOnly.onRef("value for #" + commitNum + " in " + branchName.getName(), cid.getId());
      byte payload = (byte) payloadForContent(c);

      head =
          databaseAdapter
              .commit(
                  ImmutableCommitParams.builder()
                      .toBranch(branchName)
                      .expectedHead(Optional.of(head))
                      .commitMetaSerialized(
                          ByteString.copyFromUtf8(
                              "commit #"
                                  + commitNum
                                  + " in "
                                  + branchName.getName()
                                  + " of "
                                  + numCommits))
                      .addPuts(
                          KeyWithBytes.of(
                              key,
                              cid,
                              payload,
                              DefaultStoreWorker.instance().toStoreOnReferenceState(c)))
                      .build())
              .getCommitHash();
      committed.accept(head);
    }
    return head;
  }
}
