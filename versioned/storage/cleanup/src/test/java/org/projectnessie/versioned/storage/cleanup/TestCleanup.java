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
import static java.util.UUID.randomUUID;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.cleanup.Cleanup.createCleanup;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_INCREMENTAL_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_SERIALIZED_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitType.NORMAL;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.store.DefaultStoreWorker;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessieStoreConfig(name = CONFIG_MAX_INCREMENTAL_INDEX_SIZE, value = "10000")
@NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "10000")
@NessieStoreConfig(name = CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT, value = "10")
public class TestCleanup {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  @Test
  void mustRestartWithBiggerFilterThrown() {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var maxObjReferenced = persist.config().currentTimeMicros();

    var cleanupParams = CleanupParams.builder().expectedObjCount(1).build();
    var cleanup = createCleanup(cleanupParams);
    var referencedObjectsContext = cleanup.buildReferencedObjectsContext(persist, maxObjReferenced);
    var referencedObjectsResolver =
        cleanup.createReferencedObjectsResolver(referencedObjectsContext);

    soft.assertThatThrownBy(referencedObjectsResolver::resolve)
        .isInstanceOf(MustRestartWithBiggerFilterException.class);

    var newCleanupParams = cleanupParams.withIncreasedExpectedObjCount();

    soft.assertThat(cleanupParams.expectedObjCount())
        .isLessThan(newCleanupParams.expectedObjCount());
    soft.assertThat(
            CleanupParams.builder()
                .from(cleanupParams)
                .expectedObjCount(newCleanupParams.expectedObjCount())
                .build())
        .isEqualTo(newCleanupParams);

    cleanup = createCleanup(newCleanupParams);
    referencedObjectsContext = cleanup.buildReferencedObjectsContext(persist, maxObjReferenced);
    referencedObjectsResolver = cleanup.createReferencedObjectsResolver(referencedObjectsContext);

    soft.assertThatCode(referencedObjectsResolver::resolve).doesNotThrowAnyException();
  }

  @Test
  void estimatedHeapPressure() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var maxObjReferenced = persist.config().currentTimeMicros();

    var cleanup = createCleanup(CleanupParams.builder().build());
    var referencedObjectsContext = cleanup.buildReferencedObjectsContext(persist, maxObjReferenced);
    var referencedObjectsResolver =
        cleanup.createReferencedObjectsResolver(referencedObjectsContext);

    soft.assertThat(referencedObjectsResolver.estimatedHeapPressure()).isGreaterThan(1L);

    var resolveResult = referencedObjectsResolver.resolve();
    var purge = cleanup.createPurgeObjects(resolveResult.purgeObjectsContext());

    soft.assertThat(purge.estimatedHeapPressure())
        .isGreaterThan(1L)
        .isLessThan(referencedObjectsResolver.estimatedHeapPressure());
  }

  @Test
  void againstEmptyRepository() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommitChainHeads,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numQueuedObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // refs
            3L,
            // HEADs ("main" has EMPTY_OBJ_ID)
            2L,
            // commits
            3L,
            // unique commits
            3L,
            // queued objs
            2L,
            // objs
            2L);
    soft.assertThat(resolveAndPurge.purgeResult().stats())
        .extracting(PurgeStats::failure, PurgeStats::numScannedObjs, PurgeStats::numPurgedObjs)
        .containsExactly(Optional.empty(), 5L, 0L);
  }

  @Test
  void purgeDeleteRefObjs() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var referenceLogic = referenceLogic(persist);
    var commitLogic = commitLogic(persist);

    for (int i = 0; i < 10; i++) {
      referenceLogic.createReference("kept-" + i, EMPTY_OBJ_ID, null);
    }
    for (int i = 0; i < 10; i++) {
      referenceLogic.createReference("deleted-" + i, EMPTY_OBJ_ID, null);
    }

    var resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommitChainHeads,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numQueuedObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // 3 references (empty repo) + 20 created references
            3L + 20L,
            // 2 queued commits (2 internal refs, "main" + all created refs hava EMPTY_OBJ_ID
            2L,
            // 3 commits (empty repo) + 20 created references CommitObjs
            3L + 20L,
            3L + 20L,
            // 2 objs (empty repo) + 20 created RefObj's
            2L + 20L,
            2L + 20L);
    soft.assertThat(resolveAndPurge.purgeResult().stats())
        .extracting(PurgeStats::failure, PurgeStats::numScannedObjs, PurgeStats::numPurgedObjs)
        .containsExactly(
            Optional.empty(),
            // 5 (empty repo) + 20 CommitObj + 20 RefObj + 10 CommitObj
            5L + 20L + 20L,
            // Nothing to delete
            0L);

    for (int i = 0; i < 10; i++) {
      referenceLogic.deleteReference("deleted-" + i, EMPTY_OBJ_ID);
    }

    resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommitChainHeads,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numQueuedObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // 3 references (empty repo) + 20 created references
            3L + 10L,
            // 2 queued commits (2 internal refs, "main" + all created refs hava EMPTY_OBJ_ID
            2L,
            // 3 commits (empty repo) + 20 created references CommitObjs +  10 deleted references
            // CommitObjs
            3L + 20L + 10L,
            3L + 20L + 10L,
            // 2 objs (empty repo) + 20 created RefObj's
            2L + 20L,
            2L + 20L);
    soft.assertThat(resolveAndPurge.purgeResult().stats())
        .extracting(PurgeStats::failure, PurgeStats::numScannedObjs, PurgeStats::numPurgedObjs)
        .containsExactly(
            Optional.empty(),
            // 5 (empty repo) + 20 CommitObj + 20 RefObj + 10 CommitObj
            5L + 20L + 20L + 10L,
            // RefObj's are NOT deleted, because those are referenced via the `int/refs` commit log
            // chain
            0L);

    // Shorten the "int/refs" history / make RefObj's eligible for cleanup

    var refRefs = requireNonNull(persist.fetchReference(InternalRef.REF_REFS.name()));
    var newRefRefs = referenceLogic.rewriteCommitLog(refRefs, (num, commit) -> true);
    soft.assertThat(newRefRefs.pointer()).isNotEqualTo(refRefs.pointer());
    var refRefsHead = requireNonNull(commitLogic.fetchCommit(newRefRefs.pointer()));
    soft.assertThat(refRefsHead.directParent()).isEqualTo(EMPTY_OBJ_ID);

    resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommitChainHeads,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numQueuedObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // 3 references (empty repo) + 20 created references
            3L + 10L,
            // 2 queued commits (2 internal refs, "main" + all created refs hava EMPTY_OBJ_ID
            2L,
            // 2 CommitObjs (one less than "empty repo": the commit to create the "main" reference
            // has been "squashed")
            2L,
            2L,
            // 2 objs (empty repo) + 10 "existing" RefObj's
            2L + 10L,
            2L + 10L);
    soft.assertThat(resolveAndPurge.purgeResult().stats())
        .extracting(PurgeStats::failure, PurgeStats::numScannedObjs, PurgeStats::numPurgedObjs)
        .containsExactly(
            Optional.empty(),
            // 5 (empty repo) + 20 CommitObj + 20 RefObj + 10 CommitObj + 1 re-written CommitObj
            5L + 20L + 20L + 10L + 1L,
            // RefObj's are deleted, because those are referenced via the `int/refs` commit log
            // chain, CommitObj's from the create/delete reference operations:
            // 10 RefObj's + 30 CommitObj + 2 CommitObj
            10L + 30L + 2L);
  }

  @Test
  void againstEmptyRepositoryWithGarbage() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var referenceLogic = referenceLogic(persist);
    var commitLogic = commitLogic(persist);

    var unreferenced = new ArrayList<ObjId>();
    var keptUnreferenced = new ArrayList<ObjId>();
    var referencedCommits = new ArrayList<ObjId>();
    var referenced = new ArrayList<ObjId>();
    var contents = 0;

    for (int i = 0; i < 25; i++) {
      var obj =
          stringData("foo/bar", Compression.NONE, null, List.of(), copyFromUtf8("string " + i));
      soft.assertThat(persist.storeObj(obj)).isTrue();
      unreferenced.add(obj.id());
    }
    for (int i = 0; i < 25; i++) {
      var cid = randomUUID();
      var obj =
          contentValue(
              cid.toString(),
              127,
              DefaultStoreWorker.instance()
                  .toStoreOnReferenceState(onRef("dummy " + i, cid.toString())));
      soft.assertThat(persist.storeObj(obj)).isTrue();
      unreferenced.add(obj.id());
    }

    // 10 new references
    // 10 new RefObj
    for (int i = 0; i < 10; i++) {
      var head = EMPTY_OBJ_ID;
      for (int i1 = 0; i1 < 20; i1++) {
        var cid1 = randomUUID();
        var cid2 = randomUUID();
        var obj1 =
            contentValue(
                cid1.toString(),
                127,
                DefaultStoreWorker.instance()
                    .toStoreOnReferenceState(onRef("obj " + i + " " + i1 + " 1", cid1.toString())));
        var obj2 =
            contentValue(
                cid2.toString(),
                127,
                DefaultStoreWorker.instance()
                    .toStoreOnReferenceState(onRef("obj " + i + " " + i1 + " 2", cid2.toString())));
        var commit =
            commitLogic.doCommit(
                newCommitBuilder()
                    .commitType(NORMAL)
                    .parentCommitId(head)
                    .addAdds(
                        commitAdd(
                            key("store", "key", Integer.toString(i), Integer.toString(i1), "1"),
                            42,
                            obj1.id(),
                            null,
                            cid1))
                    .addAdds(
                        commitAdd(
                            key("store", "key", Integer.toString(i), Integer.toString(i1), "2"),
                            42,
                            obj2.id(),
                            null,
                            cid2))
                    .headers(newCommitHeaders().add("created", "foo-" + i + "-" + i1).build())
                    .message("commit " + i1 + " on " + i)
                    .build(),
                List.of(obj1, obj2));
        head = requireNonNull(commit).id();

        referencedCommits.add(head);
        referenced.add(obj1.id());
        referenced.add(obj2.id());
        contents += 2;
      }

      var extendedInfo =
          stringData("ref/foo", Compression.NONE, null, List.of(), copyFromUtf8("ext-info " + i));
      soft.assertThat(persist.storeObj(extendedInfo)).isTrue();
      referenced.add(extendedInfo.id());

      referenceLogic.createReference("refs/heads/myref-" + i, head, extendedInfo.id());
    }

    var maxObjReferenced = persist.config().currentTimeMicros();

    // Unreferenced, but newer than 'maxObjReferenced'
    for (int i = 100; i < 125; i++) {
      var obj =
          stringData("foo/bar", Compression.NONE, null, List.of(), copyFromUtf8("string " + i));
      soft.assertThat(persist.storeObj(obj)).isTrue();
      keptUnreferenced.add(obj.id());
    }
    for (int i = 100; i < 125; i++) {
      var obj = contentValue("cid-" + i, 42, copyFromUtf8("string " + i));
      soft.assertThat(persist.storeObj(obj)).isTrue();
      keptUnreferenced.add(obj.id());
    }

    var resolveAndPurge = resolveAndPurge(maxObjReferenced);

    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommitChainHeads,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numQueuedObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // refs
            3L + 10L,
            // heads ("main" has EMPTY_OBJ_ID)
            2L + 10L,
            // commits
            3L + 10L + referencedCommits.size(),
            // unique commits
            3L + 10L + referencedCommits.size(),
            // objects + non-existing UniqueObj
            2L + referenced.size() + contents,
            2L + referenced.size());

    soft.assertThat(resolveAndPurge.purgeResult().stats())
        .extracting(PurgeStats::failure, PurgeStats::numScannedObjs, PurgeStats::numPurgedObjs)
        .containsExactly(
            Optional.empty(), 5L + 100L + 20L + referencedCommits.size() + referenced.size(), 50L);

    soft.assertThat(persist.fetchObjsIfExist(unreferenced.toArray(new ObjId[0])))
        .containsOnlyNulls();
    soft.assertThat(persist.fetchObjsIfExist(keptUnreferenced.toArray(new ObjId[0])))
        .doesNotContainNull();
    soft.assertThat(persist.fetchObjsIfExist(referenced.toArray(new ObjId[0])))
        .doesNotContainNull();
  }

  @Test
  void withSecondaryParents() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var referenceLogic = referenceLogic(persist);
    var commitLogic = commitLogic(persist);

    var secondaryHead = buildNewCommitChain(commitLogic, "secondary");
    var referenceHead = buildNewCommitChain(commitLogic, "main");

    var mergeCommit =
        commitLogic.doCommit(
            newCommitBuilder()
                .commitType(NORMAL)
                .parentCommitId(referenceHead)
                .addSecondaryParents(secondaryHead)
                .message("merge commit")
                .headers(newCommitHeaders().add("created", "foo merge").build())
                .build(),
            List.of());

    referenceLogic.createReference("refs/heads/my-merge-1", requireNonNull(mergeCommit).id(), null);
    referenceLogic.createReference("refs/heads/my-merge-2", requireNonNull(mergeCommit).id(), null);

    var maxObjReferenced = persist.config().currentTimeMicros();
    var resolveAndPurge = resolveAndPurge(maxObjReferenced);

    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommitChainHeads,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numQueuedObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // references
            3L + 1L + 1L,
            // commit heads (all refs HEADs + secondary parent + incl duplicates & EMPTY_OBJ_ID)
            3L + 2L,
            // commits (internals + 2x create-ref + 5+5 + 1)
            3L + 2L + 5L + 5L + 1L,
            3L + 2L + 5L + 5L + 1L,
            // objects (internals, 2x RefObj + 5+5 contents + 10 non-existing UniqueObj
            2L + 2L + 5L + 5L + 10L,
            2L + 2L + 5L + 5L);

    soft.assertThat(resolveAndPurge.purgeResult().stats())
        .extracting(PurgeStats::failure, PurgeStats::numScannedObjs, PurgeStats::numPurgedObjs)
        .containsExactly(Optional.empty(), 5L + 13L + 12L, 0L);
  }

  @Test
  void withReferenceIndexStripes() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var referenceLogic = referenceLogic(persist);
    var commitLogic = commitLogic(persist);

    // Note: the 200 value works in conjunction with config overrides at the class level
    var root = commit(commitLogic, EMPTY_OBJ_ID, 200, "withIndexStripes-root-", "main");
    var c1 = commit(commitLogic, root.id(), "withIndexStripes", "c1", "main");

    referenceLogic.createReference("refs/heads/withIndexStripes", c1.id(), null);

    var indexStripes = c1.referenceIndexStripes();
    soft.assertThat(indexStripes).hasSizeGreaterThan(1);

    var resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.purgeResult().stats().failure()).isEmpty();

    var filter = resolveAndPurge.resolveResult().purgeObjectsContext().referencedObjects();
    for (IndexStripe indexStripe : indexStripes) {
      soft.assertThat(indexStripe).matches(s -> filter.isProbablyReferenced(s.segment()));
    }
  }

  @Test
  void withReferenceIndexSegments() throws Exception {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var referenceLogic = referenceLogic(persist);
    var commitLogic = commitLogic(persist);

    // Note: the 1000 value works in conjunction with config overrides at the class level
    var root = commit(commitLogic, EMPTY_OBJ_ID, 1000, "withIndexSegments-root-", "main");
    var c1 = commit(commitLogic, root.id(), "withIndexSegments", "c1", "main");

    var indexId = c1.referenceIndex();
    var indexObj = persist.fetchObj(requireNonNull(indexId));
    soft.assertThat(indexObj).isInstanceOf(IndexSegmentsObj.class);
    var segments = (IndexSegmentsObj) indexObj;
    soft.assertThat(segments.stripes()).hasSizeGreaterThan(1);

    referenceLogic.createReference("refs/heads/withIndexSegments", c1.id(), null);

    var resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.purgeResult().stats().failure()).isEmpty();

    var filter = resolveAndPurge.resolveResult().purgeObjectsContext().referencedObjects();
    soft.assertThat(indexId).extracting(filter::isProbablyReferenced).isEqualTo(true);
    soft.assertThat(segments.stripes()).allMatch(s -> filter.isProbablyReferenced(s.segment()));
  }

  private ObjId buildNewCommitChain(CommitLogic commitLogic, String discrim) throws Exception {
    var head = EMPTY_OBJ_ID;
    for (int i = 0; i < 5; i++) {
      var commit =
          commit(commitLogic, head, "obj " + i + " " + discrim, Integer.toString(i), discrim);
      head = requireNonNull(commit).id();
    }
    return head;
  }

  private CommitObj commit(
      CommitLogic commitLogic, ObjId parent, String onRefState, String key, String discrim)
      throws CommitConflictException, ObjNotFoundException {
    var builder =
        newCommitBuilder()
            .commitType(NORMAL)
            .parentCommitId(parent)
            .headers(newCommitHeaders().add("created", "foo-" + key + "-" + discrim).build())
            .message("commit " + key + " " + discrim);
    var contentValue = addValue(builder, onRefState, key, discrim);
    return commitLogic.doCommit(builder.build(), List.of(contentValue));
  }

  private CommitObj commit(
      CommitLogic commitLogic, ObjId parent, int numKeys, String keyPrefix, String discrim)
      throws CommitConflictException, ObjNotFoundException {
    var builder =
        newCommitBuilder()
            .commitType(NORMAL)
            .parentCommitId(parent)
            .headers(newCommitHeaders().add("created", numKeys + "-" + discrim).build())
            .message("commit multiple: " + numKeys + " " + discrim);
    List<Obj> values = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      values.add(addValue(builder, "obj " + keyPrefix + i, keyPrefix + i, discrim));
    }
    return commitLogic.doCommit(builder.build(), values);
  }

  private ContentValueObj addValue(
      CreateCommit.Builder builder, String onRefState, String key, String discrim) {
    var cid = randomUUID();
    var contentValue =
        contentValue(
            cid.toString(),
            127,
            DefaultStoreWorker.instance()
                .toStoreOnReferenceState(onRef(onRefState, cid.toString())));
    builder.addAdds(commitAdd(key("store", "key", key, discrim), 42, contentValue.id(), null, cid));
    return contentValue;
  }

  ResolvePurgeResult resolveAndPurge(long maxObjReferenced) throws Exception {
    var cleanup = createCleanup(CleanupParams.builder().build());
    var referencedObjectsContext = cleanup.buildReferencedObjectsContext(persist, maxObjReferenced);
    var referencedObjectsResolver =
        cleanup.createReferencedObjectsResolver(referencedObjectsContext);
    var resolveResult = referencedObjectsResolver.resolve();
    var purgeObjects = cleanup.createPurgeObjects(resolveResult.purgeObjectsContext());
    var purgeResult = purgeObjects.purge();

    return ImmutableResolvePurgeResult.of(resolveResult, purgeResult);
  }

  @NessieImmutable
  interface ResolvePurgeResult {
    ResolveResult resolveResult();

    PurgeResult purgeResult();
  }
}
