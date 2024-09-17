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
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_RETRIES;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
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
