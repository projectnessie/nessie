/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;
import static org.projectnessie.versioned.storage.cleanup.Cleanup.createCleanup;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_INCREMENTAL_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_SERIALIZED_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitType.NORMAL;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessieStoreConfig(name = CONFIG_MAX_INCREMENTAL_INDEX_SIZE, value = "5000")
@NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "5000")
public class TestCutHistory {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  private CommitObj commit(ObjId parentId, String name, CommitObj... mergeParents)
      throws CommitConflictException, ObjNotFoundException {
    var commitLogic = commitLogic(persist);
    CreateCommit.Builder builder =
        newCommitBuilder()
            .commitType(NORMAL)
            .parentCommitId(parentId)
            .message("test commit: " + name)
            .headers(CommitHeaders.EMPTY_COMMIT_HEADERS);

    for (CommitObj mergeParent : mergeParents) {
      builder.addSecondaryParents(mergeParent.id());
    }

    return commitLogic.doCommit(builder.build(), List.of());
  }

  @Test
  void scanFindsTailOverlaps() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    // Cut point: C1
    //               /- E4
    //           /- E2 ------\
    //          /     /- E1 - E3
    //  root - C1 - C2
    //  \       \- B0 - ... B24
    //   \- D1 -D2
    AtomicInteger baseCommitCount = new AtomicInteger();
    try (CloseableIterator<Obj> it = persist.scanAllObjects(Collections.singleton(COMMIT))) {
      it.forEachRemaining(c -> baseCommitCount.incrementAndGet());
    }

    var root = commit(EMPTY_OBJ_ID, "root");
    var c1 = commit(root.id(), "c1");
    var c2 = commit(c1.id(), "c2");
    var e1 = commit(c2.id(), "e1");
    var e2 = commit(c1.id(), "e2");
    var e3 = commit(e1.id(), "e3", e2);
    var e4 = commit(e2.id(), "e4");
    var d1 = commit(root.id(), "d1");
    var d2 = commit(d1.id(), "d2");
    List<ObjId> b = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      b.add(commit(i > 0 ? b.get(i - 1) : c1.id(), "b" + i).id());
    }
    soft.assertThat(b.size()).isGreaterThan(persist.config().parentsPerCommit());

    var cleanup = createCleanup(CleanupParams.builder().build());
    CutHistoryParams ctx = cleanup.buildCutHistoryParams(persist, c1.id());
    CutHistory cutHistory = cleanup.createCutHistory(ctx);
    CutHistoryScanResult result = cutHistory.identifyAffectedCommits();

    // 34 test commits from "root" to B24, plus auxiliary commits for reference and repo objects
    soft.assertThat(result.numScannedObjs()).isEqualTo(34 + baseCommitCount.get());

    soft.assertThat(result.affectedCommitIds())
        .doesNotContain(c1.id(), root.id(), d1.id(), d2.id());
    soft.assertThat(result.affectedCommitIds())
        .contains(c2.id(), e1.id(), e2.id(), e3.id(), e4.id());

    // Commits referencing the cut point as their last parent do not need to be rewritten
    soft.assertThat(result.affectedCommitIds())
        .containsAll(b.subList(0, persist.config().parentsPerCommit() - 1));
    soft.assertThat(result.affectedCommitIds())
        .hasSize(5 + persist.config().parentsPerCommit() - 1);
  }

  @Test
  void dryRun() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    // Cut point: C1
    //  root - C1 - C2
    var root = commit(EMPTY_OBJ_ID, "root");
    var c1 = commit(root.id(), "c1");
    var c2 = commit(c1.id(), "c2");

    var cleanup = createCleanup(CleanupParams.builder().dryRun(true).build());
    var ctx = cleanup.buildCutHistoryParams(persist, c1.id());
    var cutHistory = cleanup.createCutHistory(ctx);
    var scanResult = cutHistory.identifyAffectedCommits();
    var cutResult = cutHistory.cutHistory(scanResult);
    soft.assertThat(cutResult.failures()).isEmpty();
    soft.assertThat(cutResult.rewrittenCommitIds()).isEmpty();
    soft.assertThat(cutResult.wasHistoryCut()).isFalse();

    CommitLogic commitLogic = commitLogic(persist);
    soft.assertThat(commitLogic.fetchCommit(root.id())).isEqualTo(root);
    soft.assertThat(commitLogic.fetchCommit(c1.id())).isEqualTo(c1);
    soft.assertThat(commitLogic.fetchCommit(c2.id())).isEqualTo(c2);
  }

  @Test
  void rewrittenCommitMessage() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var root = commit(EMPTY_OBJ_ID, "root");
    var c1 = commit(root.id(), "c1");

    var cleanup = createCleanup(CleanupParams.builder().build());
    var ctx = cleanup.buildCutHistoryParams(persist, c1.id());
    var cutHistory = cleanup.createCutHistory(ctx);
    var scanResult = cutHistory.identifyAffectedCommits();
    var cutResult = cutHistory.cutHistory(scanResult);
    soft.assertThat(cutResult.failures()).isEmpty();

    var commitLogic = commitLogic(persist);
    var c1new = commitLogic.fetchCommit(c1.id());
    soft.assertThat(requireNonNull(c1new).message())
        .matches(".*\\[updated to remove parents on .*].*");
  }

  @Test
  void idempotency() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var ids = new ArrayList<ObjId>();
    var root = commit(EMPTY_OBJ_ID, "root");
    ids.add(root.id());
    var c1 = commit(root.id(), "c1");
    ids.add(c1.id());
    var head = c1.id();
    for (int i = 0; i < persist.config().parentsPerCommit() + 2; i++) {
      var c = commit(head, "t" + i);
      ids.add(c.id());
      head = c.id();
    }

    var cleanup = createCleanup(CleanupParams.builder().build());
    var ctx = cleanup.buildCutHistoryParams(persist, c1.id());
    var cutHistory = cleanup.createCutHistory(ctx);
    var scanResult = cutHistory.identifyAffectedCommits();
    soft.assertThat(scanResult.cutPoint()).isEqualTo(c1.id());
    soft.assertThat(scanResult.affectedCommitIds()).isNotEmpty();

    var cutResult = cutHistory.cutHistory(scanResult);
    soft.assertThat(cutResult.wasHistoryCut()).isTrue();
    soft.assertThat(cutResult.failures()).isEmpty();
    soft.assertThat(cutResult.input()).isEqualTo(scanResult);
    soft.assertThat(cutResult.rewrittenCommitIds()).containsAll(scanResult.affectedCommitIds());
    soft.assertThat(cutResult.rewrittenCommitIds()).contains(c1.id());

    var commitLogic = commitLogic(persist);
    var commits = new ArrayList<CommitObj>();
    for (ObjId id : ids) {
      commits.add(requireNonNull(commitLogic.fetchCommit(id)));
    }

    // re-invocation does not make any changes
    cutResult = cutHistory.cutHistory(scanResult);
    soft.assertThat(cutResult.wasHistoryCut()).isFalse();
    soft.assertThat(cutResult.rewrittenCommitIds()).isEmpty();
    soft.assertThat(cutResult.failures()).isEmpty();

    soft.assertThat(persist.fetchObjs(ids.toArray(new ObjId[0])))
        .containsExactlyInAnyOrderElementsOf(commits);
  }

  private IcebergTable table(String key) {
    return IcebergTable.of(key, 1, 2, 3, 4);
  }

  private Hash commit(VersionStore store, String ref, Collection<String> keys) throws Exception {
    ReferenceInfo<CommitMeta> main = store.getNamedRef(ref, GetNamedRefsParams.DEFAULT);
    CommitResult result =
        store.commit(
            BranchName.of(main.getNamedRef().getName()),
            Optional.of(main.getHash()),
            CommitMeta.fromMessage("test: " + keys),
            keys.stream()
                .map(k -> Operation.Put.of(ContentKey.of(k), table(k)))
                .collect(Collectors.toList()));

    return result.getCommit().getHash();
  }

  @ParameterizedTest
  @CsvSource({
    // Note: numExtraKeys and expectIndexStripes work in conjunction with config overrides at the
    // class level
    "1, false, false",
    "1, false, true",
    "10, false, false",
    "30, false, true",
    "100, true, false",
    "100, true, true",
  })
  void cutPreservesContents(int numExtraKeys, boolean expectIndexStripes, boolean withPurge)
      throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    // Cut point: C1
    //          /- E1 - E2 -\     /- F1
    //  root - C0 --------- C1 - C2 - B1... Bnn
    //                       \- D1 - D2
    var main =
        requireNonNull(repositoryLogic(persist).fetchRepositoryDescription()).defaultBranchName();
    var store = new VersionStoreImpl(persist);
    var root = commit(store, main, List.of("root"));

    var c0Keys =
        IntStream.rangeClosed(1, numExtraKeys).mapToObj(i -> "k" + i).collect(Collectors.toSet());
    var c0 = commit(store, main, c0Keys); // force index stripes to spill out

    CommitObj commitC0 = commitLogic(persist).fetchCommit(hashToObjId(c0));
    soft.assertThat(requireNonNull(commitC0).referenceIndexStripes().isEmpty())
        .isNotEqualTo(expectIndexStripes);

    var branchE = store.create(BranchName.of("e"), Optional.of(c0)).getNamedRef().getName();
    var e1 = commit(store, branchE, List.of("e1"));
    var e2 = commit(store, branchE, List.of("e2"));
    store.delete(BranchName.of(branchE), e2);

    var c1 =
        store
            .merge(
                VersionStore.MergeOp.builder()
                    .expectedHash(Optional.of(c0))
                    .fromRef(BranchName.of(branchE))
                    .fromHash(e2)
                    .toBranch(BranchName.of(main))
                    .build())
            .getResultantTargetHash();

    var c2 = commit(store, main, List.of("c2"));

    var branchD = store.create(BranchName.of("d"), Optional.ofNullable(c1)).getNamedRef().getName();
    var d1 = commit(store, branchD, List.of("d1"));
    var d2 = commit(store, branchD, List.of("d2"));

    var branchF = store.create(BranchName.of("f"), Optional.ofNullable(c2)).getNamedRef().getName();
    var f1 = commit(store, branchF, List.of("f1"));

    var keysB = new ArrayList<String>();
    var hashesB = new ArrayList<Hash>();
    for (int i = 0; i < numExtraKeys; i++) {
      String k = "b" + i;
      keysB.add(k);
      hashesB.add(commit(store, main, List.of(k)));
    }

    CommitObj commitBLast =
        commitLogic(persist).fetchCommit(hashToObjId(hashesB.get(hashesB.size() - 1)));
    soft.assertThat(requireNonNull(commitBLast).referenceIndexStripes().isEmpty())
        .isNotEqualTo(expectIndexStripes);

    var cleanup = createCleanup(CleanupParams.builder().build());
    var ctx = cleanup.buildCutHistoryParams(persist, hashToObjId(requireNonNull(c1)));
    var cutHistory = cleanup.createCutHistory(ctx);
    var identifiedCommits = cutHistory.identifyAffectedCommits();
    var cutResult = cutHistory.cutHistory(identifiedCommits);
    soft.assertThat(cutResult.wasHistoryCut()).isTrue();
    soft.assertThat(cutResult.failures()).isEmpty();
    soft.assertThat(cutResult.input()).isEqualTo(identifiedCommits);
    soft.assertThat(cutResult.rewrittenCommitIds())
        .containsAll(identifiedCommits.affectedCommitIds());
    soft.assertThat(cutResult.rewrittenCommitIds()).contains(hashToObjId(requireNonNull(c1)));

    var commits = store.getCommits(c1, true);
    soft.assertThat(commits).hasNext();
    soft.assertThat(commits.next())
        .extracting(Commit::getParentHash, c -> c.getCommitMeta().getParentCommitHashes())
        .containsExactly(objIdToHash(EMPTY_OBJ_ID), List.of(EMPTY_OBJ_ID.toString()));

    if (withPurge) {
      var referencedObjectsContext =
          cleanup.buildReferencedObjectsContext(persist, persist.config().currentTimeMicros());
      var referencedObjectsResolver =
          cleanup.createReferencedObjectsResolver(referencedObjectsContext);
      var resolveResult = referencedObjectsResolver.resolve();
      var purgeObjects = cleanup.createPurgeObjects(resolveResult.purgeObjectsContext());
      var purgeResult = purgeObjects.purge();
      soft.assertThat(purgeResult.stats().numPurgedObjs()).isGreaterThan(0);
    }

    if (!withPurge) {
      validateContent(store, root, List.of(), "root");
      validateContent(store, c0, c0Keys, "root");
      validateContent(store, e1, c0Keys, "root", "e1");
      validateContent(store, e2, c0Keys, "root", "e1", "e2");
    }

    validateContent(store, c1, c0Keys, "root", "e1", "e2");
    validateContent(store, c1, c0Keys, "root", "e1", "e2");
    validateContent(store, d1, c0Keys, "root", "e1", "e2", "d1");
    validateContent(store, d2, c0Keys, "root", "e1", "e2", "d1", "d2");
    validateContent(store, c2, c0Keys, "root", "e1", "e2", "c2");
    validateContent(store, f1, c0Keys, "root", "e1", "e2", "c2", "f1");

    for (int i = 0; i < hashesB.size(); i++) {
      Set<String> keys = new HashSet<>(c0Keys);
      keys.addAll(Arrays.asList("root", "e1", "e2", "c2"));
      keys.addAll(keysB.subList(0, i + 1));
      validateContent(store, hashesB.get(i), keys);
    }
  }

  private void validateContent(
      VersionStore store, Hash hash, Collection<String> keys, String... extraKeys)
      throws ReferenceNotFoundException {
    Set<String> allKeys = new HashSet<>(keys);
    allKeys.addAll(Arrays.asList(extraKeys));

    Set<String> loadedKeys = new HashSet<>();
    Set<String> loadedLocations = new HashSet<>();
    PaginationIterator<KeyEntry> it = store.getKeys(hash, null, true, NO_KEY_RESTRICTIONS);
    while (it.hasNext()) {
      KeyEntry e = it.next();
      loadedKeys.add(e.getKey().contentKey().toString());
      loadedLocations.add(((IcebergTable) requireNonNull(e.getContent())).getMetadataLocation());
    }
    soft.assertThat(loadedKeys).containsExactlyInAnyOrderElementsOf(allKeys);
    soft.assertThat(loadedLocations).containsExactlyInAnyOrderElementsOf(allKeys);
  }
}
