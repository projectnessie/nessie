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
package org.projectnessie.versioned.storage.versionstore;

import jakarta.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.tests.StorageAssertions;
import org.projectnessie.versioned.tests.ValidatingVersionStore;

public class ValidatingVersionStoreImpl extends VersionStoreImpl implements ValidatingVersionStore {

  private final SoftAssertions soft;
  private final ValidatingPersist persist;

  private ValidatingVersionStoreImpl(SoftAssertions soft, ValidatingPersist persist) {
    super(persist);
    this.soft = soft;
    this.persist = persist;
  }

  public static VersionStore of(SoftAssertions soft, Persist persist) {
    return new ValidatingVersionStoreImpl(soft, new ValidatingPersist(persist));
  }

  @Override
  public StorageAssertions storageCheckpoint() {
    return new StorageCheckpoint();
  }

  private static class ValidatingPersist extends PersistDelegate {
    private final AtomicInteger writeCounter = new AtomicInteger();
    private final AtomicReference<RuntimeException> lastWrite = new AtomicReference<>();

    ValidatingPersist(Persist p) {
      super(p);
    }

    private void recordWrite() {
      writeCounter.incrementAndGet();
      lastWrite.set(new RuntimeException("write #" + writeCounter.incrementAndGet()));
    }

    @Nonnull
    @Override
    public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
      recordWrite();
      return super.addReference(reference);
    }

    @Nonnull
    @Override
    public Reference markReferenceAsDeleted(@Nonnull Reference reference)
        throws RefNotFoundException, RefConditionFailedException {
      recordWrite();
      return super.markReferenceAsDeleted(reference);
    }

    @Override
    public void purgeReference(@Nonnull Reference reference)
        throws RefNotFoundException, RefConditionFailedException {
      recordWrite();
      super.purgeReference(reference);
    }

    @Nonnull
    @Override
    public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
        throws RefNotFoundException, RefConditionFailedException {
      recordWrite();
      return super.updateReferencePointer(reference, newPointer);
    }

    @Override
    public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
      recordWrite();
      super.upsertObj(obj);
    }

    @Override
    public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
      recordWrite();
      super.upsertObjs(objs);
    }

    @Override
    public boolean storeObj(@Nonnull Obj obj) throws ObjTooLargeException {
      recordWrite();
      return super.storeObj(obj);
    }

    @Override
    public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
        throws ObjTooLargeException {
      recordWrite();
      return super.storeObj(obj, ignoreSoftSizeRestrictions);
    }

    @Nonnull
    @Override
    public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
      recordWrite();
      return super.storeObjs(objs);
    }
  }

  private class StorageCheckpoint extends StorageAssertions {
    private final Throwable initial = persist.lastWrite.get();

    @Override
    public void assertNoWrites() {
      RuntimeException current = persist.lastWrite.get();
      soft.assertThatCode(
              () -> {
                if (current != initial) {
                  throw current;
                }
              })
          .describedAs("Expected no writes at the storage layer")
          .doesNotThrowAnyException();
    }
  }
}
