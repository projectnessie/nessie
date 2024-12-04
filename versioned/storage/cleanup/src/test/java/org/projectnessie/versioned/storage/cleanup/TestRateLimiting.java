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
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.store.DefaultStoreWorker;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestRateLimiting {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  @Test
  void productionImplementations() {
    soft.assertThat(RateLimit.create(0)).extracting(RateLimit::toString).isEqualTo("unlimited");
    soft.assertThat(RateLimit.create(-42)).extracting(RateLimit::toString).isEqualTo("unlimited");
    soft.assertThat(RateLimit.create(42)).extracting(RateLimit::toString).isEqualTo("up to 42");

    soft.assertThatCode(() -> RateLimit.create(0).acquire()).doesNotThrowAnyException();
    soft.assertThatCode(() -> RateLimit.create(42).acquire()).doesNotThrowAnyException();
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

    var acquires = new AtomicLong[4];

    var cleanup =
        createCleanup(
            CleanupParams.builder()
                .resolveCommitRatePerSecond(1)
                .resolveObjRatePerSecond(2)
                .purgeScanObjRatePerSecond(3)
                .purgeDeleteObjRatePerSecond(4)
                .rateLimitFactory(
                    i -> {
                      var a = acquires[i - 1] = new AtomicLong();
                      return a::incrementAndGet;
                    })
                .build());
    var referencedObjectsContext = cleanup.buildReferencedObjectsContext(persist, maxObjReferenced);
    var referencedObjectsResolver =
        cleanup.createReferencedObjectsResolver(referencedObjectsContext);
    var resolveResult = referencedObjectsResolver.resolve();

    soft.assertThat(acquires)
        .extracting(l -> l != null ? l.get() : -1L)
        .containsExactlyInAnyOrder(
            3L + 10L + referencedCommits.size(), 2L + referenced.size(), -1L, -1L);
    soft.assertThat(resolveResult.stats())
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
            // objects
            2L + referenced.size() + contents,
            2L + referenced.size());

    var purgeObjects = cleanup.createPurgeObjects(resolveResult.purgeObjectsContext());
    var purgeResult = purgeObjects.purge();

    soft.assertThat(acquires)
        .extracting(AtomicLong::get)
        .containsExactlyInAnyOrder(
            3L + 10L + referencedCommits.size(),
            2L + referenced.size(),
            5L + 100L + 20L + referencedCommits.size() + referenced.size(),
            50L);
    soft.assertThat(purgeResult.stats())
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
}
