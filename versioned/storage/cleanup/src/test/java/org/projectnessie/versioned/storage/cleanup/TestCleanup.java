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

import static java.util.UUID.randomUUID;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.cleanup.Cleanup.createCleanup;
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
import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.store.DefaultStoreWorker;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestCleanup {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  @Test
  void againstEmptyRepository() {
    soft.assertThat(repositoryLogic(persist).repositoryExists()).isTrue();

    var resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numPendingObjs,
            ResolveStats::numObjs)
        .containsExactly(Optional.empty(), 3L, 3L, 3L, 2L, 2L);
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
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numPendingObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // 3 references (empty repo) + 20 created references
            3L + 20L,
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
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numPendingObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // 3 references (empty repo) + 20 created references
            3L + 10L,
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

    var refRefs = persist.fetchReference(InternalRef.REF_REFS.name());
    var newRefRefs = referenceLogic.rewriteCommitLog(refRefs, (num, commit) -> true);
    soft.assertThat(newRefRefs.pointer()).isNotEqualTo(refRefs.pointer());
    var refRefsHead = commitLogic.fetchCommit(newRefRefs.pointer());
    soft.assertThat(refRefsHead.directParent()).isEqualTo(EMPTY_OBJ_ID);

    resolveAndPurge = resolveAndPurge(persist.config().currentTimeMicros());
    soft.assertThat(resolveAndPurge.resolveResult().stats())
        .extracting(
            ResolveStats::failure,
            ResolveStats::numReferences,
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numPendingObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            // 3 references (empty repo) + 20 created references
            3L + 10L,
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
    var refs = new ArrayList<Reference>();
    var keptUnreferenced = new ArrayList<ObjId>();
    var referencedCommits = new ArrayList<ObjId>();
    var referenced = new ArrayList<ObjId>();

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
        head = commit.id();

        referencedCommits.add(head);
        referenced.add(obj1.id());
        referenced.add(obj2.id());
      }

      var extendedInfo =
          stringData("ref/foo", Compression.NONE, null, List.of(), copyFromUtf8("ext-info " + i));
      soft.assertThat(persist.storeObj(extendedInfo)).isTrue();
      referenced.add(extendedInfo.id());

      var ref = referenceLogic.createReference("refs/heads/myref-" + i, head, extendedInfo.id());
      refs.add(ref);
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
            ResolveStats::numCommits,
            ResolveStats::numUniqueCommits,
            ResolveStats::numPendingObjs,
            ResolveStats::numObjs)
        .containsExactly(
            Optional.empty(),
            3L + 10L,
            3L + 10L + referencedCommits.size(),
            3L + 10L + referencedCommits.size(),
            2L + referenced.size(),
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

  ResolvePurgeResult resolveAndPurge(long maxObjReferenced) {
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
