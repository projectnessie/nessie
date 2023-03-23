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
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_RETRIES;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.commontests.AbstractVersionStoreTests;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;

public class TestVersionStoreImpl extends AbstractVersionStoreTests {

  @Test
  public void commitWithInfiniteConcurrentConflict(
      @NessieStoreConfig(name = CONFIG_COMMIT_RETRIES, value = "3")
          @NessieStoreConfig(name = CONFIG_COMMIT_TIMEOUT_MILLIS, value = "999999999")
          @NessiePersist
          Persist persist)
      throws Exception {
    VersionStore store = new VersionStoreImpl(persist);

    BranchName branch = BranchName.of("branch1");
    Hash branch1 = store.create(branch, Optional.empty());

    AtomicInteger intercepted = new AtomicInteger();

    Persist tested =
        new PersistDelegate(persist) {
          @Nonnull
          @jakarta.annotation.Nonnull
          @Override
          public Reference updateReferencePointer(
              @Nonnull @jakarta.annotation.Nonnull Reference reference,
              @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
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
    Hash branch1 = store.create(branch, Optional.empty());

    AtomicBoolean intercepted = new AtomicBoolean();

    Persist tested =
        new PersistDelegate(persist) {
          @Nonnull
          @jakarta.annotation.Nonnull
          @Override
          public Reference updateReferencePointer(
              @Nonnull @jakarta.annotation.Nonnull Reference reference,
              @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
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

  static class PersistDelegate extends BasePersistDelegate implements Persist {

    PersistDelegate(Persist p) {
      super(p);
    }

    @Override
    public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
      delegate.deleteObj(id);
    }

    @Override
    public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
      delegate.deleteObjs(ids);
    }

    @Override
    public void upsertObj(@Nonnull @jakarta.annotation.Nonnull Obj obj)
        throws ObjTooLargeException {
      delegate.upsertObj(obj);
    }

    @Override
    public void upsertObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
        throws ObjTooLargeException {
      delegate.upsertObjs(objs);
    }
  }

  abstract static class BasePersistDelegate implements Persist {
    final Persist delegate;

    BasePersistDelegate(Persist delegate) {
      this.delegate = delegate;
    }

    @Override
    public int hardObjectSizeLimit() {
      return delegate.hardObjectSizeLimit();
    }

    @Override
    public int effectiveIndexSegmentSizeLimit() {
      return delegate.effectiveIndexSegmentSizeLimit();
    }

    @Override
    public int effectiveIncrementalIndexSizeLimit() {
      return delegate.effectiveIncrementalIndexSizeLimit();
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public String name() {
      return delegate.name();
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public StoreConfig config() {
      return delegate.config();
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public Reference addReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
        throws RefAlreadyExistsException {
      return delegate.addReference(reference);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public Reference markReferenceAsDeleted(
        @Nonnull @jakarta.annotation.Nonnull Reference reference)
        throws RefNotFoundException, RefConditionFailedException {
      return delegate.markReferenceAsDeleted(reference);
    }

    @Override
    public void purgeReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
        throws RefNotFoundException, RefConditionFailedException {
      delegate.purgeReference(reference);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public Reference updateReferencePointer(
        @Nonnull @jakarta.annotation.Nonnull Reference reference,
        @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
        throws RefNotFoundException, RefConditionFailedException {
      return delegate.updateReferencePointer(reference, newPointer);
    }

    @Override
    public Reference fetchReference(@Nonnull @jakarta.annotation.Nonnull String name) {
      return delegate.fetchReference(name);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public Reference[] fetchReferences(@Nonnull @jakarta.annotation.Nonnull String[] names) {
      return delegate.fetchReferences(names);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public Obj fetchObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) throws ObjNotFoundException {
      return delegate.fetchObj(id);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public <T extends Obj> T fetchTypedObj(
        @Nonnull @jakarta.annotation.Nonnull ObjId id, ObjType type, Class<T> typeClass)
        throws ObjNotFoundException {
      return delegate.fetchTypedObj(id, type, typeClass);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public ObjType fetchObjType(@Nonnull @jakarta.annotation.Nonnull ObjId id)
        throws ObjNotFoundException {
      return delegate.fetchObjType(id);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public Obj[] fetchObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids)
        throws ObjNotFoundException {
      return delegate.fetchObjs(ids);
    }

    @Override
    public boolean storeObj(@Nonnull @jakarta.annotation.Nonnull Obj obj)
        throws ObjTooLargeException {
      return delegate.storeObj(obj);
    }

    @Override
    public boolean storeObj(
        @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
        throws ObjTooLargeException {
      return delegate.storeObj(obj, ignoreSoftSizeRestrictions);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public boolean[] storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
        throws ObjTooLargeException {
      return delegate.storeObjs(objs);
    }

    @Override
    public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
      delegate.deleteObj(id);
    }

    @Override
    public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
      delegate.deleteObjs(ids);
    }

    @Override
    @Nonnull
    @jakarta.annotation.Nonnull
    public CloseableIterator<Obj> scanAllObjects(
        @Nonnull @jakarta.annotation.Nonnull Set<ObjType> returnedObjTypes) {
      return delegate.scanAllObjects(returnedObjTypes);
    }

    @Override
    public void erase() {
      delegate.erase();
    }
  }
}
