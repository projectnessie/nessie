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
package org.projectnessie.versioned.storage.versionstore;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_RETRIES;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentHistoryEntry;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.commontests.AbstractVersionStoreTests;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;

public class TestVersionStoreImpl extends AbstractVersionStoreTests {

  @InjectSoftAssertions protected SoftAssertions soft;

  @Override
  protected VersionStore store() {
    return ValidatingVersionStoreImpl.of(soft, persist);
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void contentHistory() throws Exception {
    var store = new VersionStoreImpl(persist);

    var branch = BranchName.of("branchContentHistory");
    store.create(branch, Optional.empty()).getHash();

    var key = ContentKey.of("history-table");
    var keyOther = ContentKey.of("other-table");
    var keyRenamed = ContentKey.of("renamed-table");

    var hashCreate =
        store.commit(
            branch,
            Optional.empty(),
            fromMessage("create"),
            List.of(Operation.Put.of(key, IcebergTable.of("meta1", 1, 0, 0, 0))));
    var contentCreate = store.getValue(branch, key, false).content();
    store.commit(
        branch,
        Optional.empty(),
        fromMessage("update 1"),
        List.of(
            Operation.Put.of(key, IcebergTable.of("meta2", 2, 0, 0, 0, contentCreate.getId()))));
    var contentUpdate1 = store.getValue(branch, key, false).content();
    var hashCreateOther =
        store.commit(
            branch,
            Optional.empty(),
            fromMessage("create-other"),
            List.of(Operation.Put.of(keyOther, IcebergTable.of("other1", 11, 0, 0, 0))));
    var hashUpdate2 =
        store.commit(
            branch,
            Optional.empty(),
            fromMessage("update 2"),
            List.of(
                Operation.Put.of(
                    key, IcebergTable.of("meta3", 3, 0, 0, 0, contentUpdate1.getId()))));
    var contentUpdate2 = store.getValue(branch, key, false).content();
    var hashRename1 =
        store.commit(
            branch,
            Optional.empty(),
            fromMessage("rename 1"),
            List.of(
                Operation.Delete.of(key),
                Operation.Put.of(
                    keyRenamed, IcebergTable.of("meta4", 4, 0, 0, 0, contentUpdate1.getId()))));
    var contentRename1 = store.getValue(branch, keyRenamed, false).content();
    store.commit(
        branch,
        Optional.empty(),
        fromMessage("update 3"),
        List.of(
            Operation.Put.of(
                keyRenamed, IcebergTable.of("meta5", 5, 0, 0, 0, contentRename1.getId()))));
    var contentUpdate3 = store.getValue(branch, keyRenamed, false).content();
    var hashRename2 =
        store.commit(
            branch,
            Optional.empty(),
            fromMessage("rename 2"),
            List.of(Operation.Delete.of(keyRenamed), Operation.Put.of(key, contentUpdate3)));
    var contentRename2 = store.getValue(branch, key, false).content();
    soft.assertThat(contentRename2).isEqualTo(contentUpdate3);
    var hashUpdate4 =
        store.commit(
            branch,
            Optional.empty(),
            fromMessage("update 4"),
            List.of(
                Operation.Put.of(
                    key, IcebergTable.of("meta6", 6, 0, 0, 0, contentRename2.getId()))));
    var contentUpdate4 = store.getValue(branch, key, false).content();

    soft.assertThat(Lists.newArrayList(store.getContentChanges(branch, key)))
        .extracting(
            ContentHistoryEntry::getKey,
            e -> e.getCommitMeta().getMessage(),
            e -> Hash.of(e.getCommitMeta().getHash()),
            ContentHistoryEntry::getContent)
        .containsExactly(
            tuple(key, "update 4", hashUpdate4.getCommitHash(), contentUpdate4),
            // "rename 2" is seen as the 1st content change before "update 4"
            tuple(key, "rename 2", hashRename2.getCommitHash(), contentRename2),
            // "update 3" has the same content value as "rename 2", so it is not returned in the
            // iterator
            tuple(keyRenamed, "rename 1", hashRename1.getCommitHash(), contentRename1),
            tuple(key, "update 2", hashUpdate2.getCommitHash(), contentUpdate2),
            tuple(key, "create-other", hashCreateOther.getCommitHash(), contentUpdate1),
            // "create" is the only commit that has the initial change
            tuple(key, "create", hashCreate.getCommitHash(), contentCreate));
  }

  @Test
  public void commitWithInfiniteConcurrentConflict(
      @NessieStoreConfig(name = CONFIG_COMMIT_RETRIES, value = "3")
          @NessieStoreConfig(name = CONFIG_COMMIT_TIMEOUT_MILLIS, value = "999999999")
          @NessiePersist
          Persist persist)
      throws Exception {
    VersionStore store = new VersionStoreImpl(persist);

    BranchName branch = BranchName.of("branch1");
    Hash branch1 = store.create(branch, Optional.empty()).getHash();

    AtomicInteger intercepted = new AtomicInteger();

    Persist tested =
        new PersistDelegate(persist) {
          @Nonnull
          @Override
          public Reference updateReferencePointer(
              @Nonnull Reference reference, @Nonnull ObjId newPointer)
              throws RefNotFoundException, RefConditionFailedException {

            int num = intercepted.incrementAndGet();
            // Intercept the reference-pointer-bump and inject a concurrent commit
            // here
            try {
              store.commit(
                  branch,
                  Optional.of(branch1),
                  fromMessage("conflicting pointer bump"),
                  singletonList(
                      Put.of(
                          ContentKey.of("other-key-" + num),
                          IcebergTable.of("meta", 42, 43, 44, 45))));
            } catch (ReferenceNotFoundException | ReferenceConflictException e) {
              throw new RuntimeException(e);
            }

            return super.updateReferencePointer(reference, newPointer);
          }
        };

    VersionStore storeTested = new VersionStoreImpl(tested);
    assertThatThrownBy(
            () ->
                storeTested.commit(
                    branch,
                    Optional.of(branch1),
                    fromMessage("commit foo"),
                    singletonList(
                        Put.of(
                            ContentKey.of("some-key"), IcebergTable.of("meta", 42, 43, 44, 45)))))
        .isInstanceOf(ReferenceRetryFailureException.class)
        .hasMessageStartingWith(
            "The commit operation could not be performed after 3 retries within the configured commit timeout after ");
  }

  @Test
  public void commitWithSingleConcurrentConflict() throws Exception {
    VersionStore store = new VersionStoreImpl(persist);

    BranchName branch = BranchName.of("branch1");
    Hash branch1 = store.create(branch, Optional.empty()).getHash();

    AtomicBoolean intercepted = new AtomicBoolean();

    Persist tested =
        new PersistDelegate(persist) {
          @Nonnull
          @Override
          public Reference updateReferencePointer(
              @Nonnull Reference reference, @Nonnull ObjId newPointer)
              throws RefNotFoundException, RefConditionFailedException {

            if (intercepted.compareAndSet(false, true)) {
              // Intercept the reference-pointer-bump and inject a concurrent commit
              // here
              try {
                store.commit(
                    branch,
                    Optional.of(branch1),
                    fromMessage("conflicting pointer bump"),
                    singletonList(
                        Put.of(
                            ContentKey.of("other-key"), IcebergTable.of("meta", 42, 43, 44, 45))));
              } catch (ReferenceNotFoundException | ReferenceConflictException e) {
                throw new RuntimeException(e);
              }
            }

            return super.updateReferencePointer(reference, newPointer);
          }
        };

    VersionStore storeTested = new VersionStoreImpl(tested);
    storeTested.commit(
        branch,
        Optional.of(branch1),
        fromMessage("commit foo"),
        singletonList(Put.of(ContentKey.of("some-key"), IcebergTable.of("meta", 42, 43, 44, 45))));
  }

  @ParameterizedTest
  @MethodSource
  public void commitWithDatabaseTimeout(
      @SuppressWarnings("unused") String name,
      BiFunction<AtomicBoolean, Persist, Persist> delegateFactory)
      throws Exception {
    VersionStore store = new VersionStoreImpl(persist);

    BranchName branch = BranchName.of("branch1");
    Hash branch1 = store.create(branch, Optional.empty()).getHash();

    AtomicBoolean called = new AtomicBoolean();
    Persist tested = delegateFactory.apply(called, persist);

    VersionStore storeTested = new VersionStoreImpl(tested);
    storeTested.commit(
        branch,
        Optional.of(branch1),
        fromMessage("commit foo"),
        singletonList(Put.of(ContentKey.of("some-key"), IcebergTable.of("meta", 42, 43, 44, 45))));

    soft.assertThat(called).isTrue();
  }

  public static Stream<Arguments> commitWithDatabaseTimeout() {
    return Stream.of(
        arguments(
            "updateReferencePointer-success-but-timeout",
            (BiFunction<AtomicBoolean, Persist, Persist>)
                (called, p) ->
                    new PersistDelegate(p) {
                      @Nonnull
                      @Override
                      public Reference updateReferencePointer(
                          @Nonnull Reference reference, @Nonnull ObjId newPointer)
                          throws RefNotFoundException, RefConditionFailedException {
                        if (called.compareAndSet(false, true)) {
                          super.updateReferencePointer(reference, newPointer);
                          throw new UnknownOperationResultException("test", new RuntimeException());
                        } else {
                          return super.updateReferencePointer(reference, newPointer);
                        }
                      }
                    }),
        arguments(
            "storeObj-success-but-timeout",
            (BiFunction<AtomicBoolean, Persist, Persist>)
                (called, p) ->
                    new PersistDelegate(p) {
                      @Override
                      public boolean storeObj(@Nonnull Obj obj) throws ObjTooLargeException {
                        if (called.compareAndSet(false, true)) {
                          super.storeObj(obj);
                          throw new UnknownOperationResultException("test", new RuntimeException());
                        } else {
                          return super.storeObj(obj);
                        }
                      }
                    }),
        arguments(
            "storeObjs-success-but-timeout",
            (BiFunction<AtomicBoolean, Persist, Persist>)
                (called, p) ->
                    new PersistDelegate(p) {
                      @Nonnull
                      @Override
                      public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
                        if (called.compareAndSet(false, true)) {
                          super.storeObjs(objs);
                          throw new UnknownOperationResultException("test", new RuntimeException());
                        } else {
                          return super.storeObjs(objs);
                        }
                      }
                    }),
        //
        arguments(
            "updateReferencePointer-no-success",
            (BiFunction<AtomicBoolean, Persist, Persist>)
                (called, p) ->
                    new PersistDelegate(p) {
                      @Nonnull
                      @Override
                      public Reference updateReferencePointer(
                          @Nonnull Reference reference, @Nonnull ObjId newPointer)
                          throws RefNotFoundException, RefConditionFailedException {
                        if (called.compareAndSet(false, true)) {
                          throw new UnknownOperationResultException("test", new RuntimeException());
                        } else {
                          return super.updateReferencePointer(reference, newPointer);
                        }
                      }
                    }),
        arguments(
            "storeObj-no-success",
            (BiFunction<AtomicBoolean, Persist, Persist>)
                (called, p) ->
                    new PersistDelegate(p) {
                      @Override
                      public boolean storeObj(@Nonnull Obj obj) throws ObjTooLargeException {
                        if (called.compareAndSet(false, true)) {
                          throw new UnknownOperationResultException("test", new RuntimeException());
                        } else {
                          return super.storeObj(obj);
                        }
                      }
                    }),
        arguments(
            "storeObjs-no-success",
            (BiFunction<AtomicBoolean, Persist, Persist>)
                (called, p) ->
                    new PersistDelegate(p) {
                      @Nonnull
                      @Override
                      public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
                        if (called.compareAndSet(false, true)) {
                          throw new UnknownOperationResultException("test", new RuntimeException());
                        } else {
                          return super.storeObjs(objs);
                        }
                      }
                    })
        //
        );
  }
}
