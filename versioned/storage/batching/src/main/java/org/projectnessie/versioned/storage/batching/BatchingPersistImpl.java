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
package org.projectnessie.versioned.storage.batching;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.ValidatingPersist;

final class BatchingPersistImpl implements BatchingPersist, ValidatingPersist {
  private final WriteBatching batching;

  private final Map<ObjId, Obj> pendingUpserts = new HashMap<>();
  private final Map<ObjId, Obj> pendingStores = new HashMap<>();

  private final ReentrantReadWriteLock lock;

  BatchingPersistImpl(WriteBatching batching) {
    checkArgument(batching.optimistic(), "Non-optimistic mode is not supported");
    this.batching = batching;
    this.lock = new ReentrantReadWriteLock();
  }

  @VisibleForTesting
  Map<ObjId, Obj> pendingUpserts() {
    return pendingUpserts;
  }

  @VisibleForTesting
  Map<ObjId, Obj> pendingStores() {
    return pendingStores;
  }

  @Override
  public void flush() {
    if (batching.batchSize() > 0) {
      writeLock();
      try {
        if (!pendingStores.isEmpty()) {
          delegate().storeObjs(pendingStores.values().toArray(new Obj[0]));
          pendingStores.clear();
        }
        if (!pendingUpserts.isEmpty()) {
          delegate().upsertObjs(pendingUpserts.values().toArray(new Obj[0]));
          pendingUpserts.clear();
        }
      } catch (ObjTooLargeException e) {
        throw new RuntimeException(e);
      } finally {
        writeUnlock();
      }
    }
  }

  private Persist delegate() {
    return batching.persist();
  }

  private void readLock() {
    lock.readLock().lock();
  }

  private void readUnlock() {
    lock.readLock().unlock();
  }

  private void writeUnlock() {
    lock.writeLock().unlock();
  }

  private void writeLock() {
    lock.writeLock().lock();
  }

  private void maybeFlush() {
    if (batching.batchSize() > 0) {
      if (pendingStores.size() > batching.batchSize()
          || pendingUpserts.size() > batching.batchSize()) {
        flush();
      }
    }
  }

  @Override
  public boolean storeObj(
      @Nonnull @javax.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    if (!ignoreSoftSizeRestrictions) {
      verifySoftRestrictions(obj);
    }
    writeLock();
    try {
      pendingStores.putIfAbsent(obj.id(), obj);
      maybeFlush();
    } finally {
      writeUnlock();
    }
    return true;
  }

  @Override
  public void upsertObj(@Nonnull @javax.annotation.Nonnull Obj obj) throws ObjTooLargeException {
    verifySoftRestrictions(obj);
    writeLock();
    try {
      pendingUpserts.put(obj.id(), obj);
      maybeFlush();
    } finally {
      writeUnlock();
    }
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public boolean[] storeObjs(@Nonnull @javax.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    writeLock();
    try {
      for (Obj obj : objs) {
        if (obj != null) {
          storeObj(obj);
        }
      }
    } finally {
      writeUnlock();
    }
    boolean[] r = new boolean[objs.length];
    Arrays.fill(r, true);
    return r;
  }

  @Override
  public void upsertObjs(@Nonnull @javax.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    writeLock();
    try {
      for (Obj obj : objs) {
        if (obj != null) {
          upsertObj(obj);
        }
      }
    } finally {
      writeUnlock();
    }
  }

  private Obj pendingObj(ObjId id) {
    Obj r = pendingUpserts.get(id);
    if (r == null) {
      r = pendingStores.get(id);
    }
    return r;
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull @javax.annotation.Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass)
      throws ObjNotFoundException {
    readLock();
    try {
      Obj r = pendingObj(id);
      if (r != null) {
        if (type != null && !r.type().equals(type)) {
          throw new ObjNotFoundException(id);
        }
        @SuppressWarnings("unchecked")
        T o = (T) r;
        return o;
      }
    } finally {
      readUnlock();
    }
    return delegate().fetchTypedObj(id, type, typeClass);
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public ObjType fetchObjType(@Nonnull @javax.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    readLock();
    try {
      Obj r = pendingObj(id);
      if (r != null) {
        return r.type();
      }
    } finally {
      readUnlock();
    }
    return delegate().fetchObjType(id);
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public <T extends Obj> T[] fetchTypedObjs(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {

    ObjId[] backendIds = null;
    @SuppressWarnings("unchecked")
    T[] r = (T[]) Array.newInstance(typeClass, ids.length);

    backendIds = fetchObjsPre(ids, r, backendIds);

    if (backendIds == null) {
      return r;
    }

    T[] backendResult = delegate().fetchTypedObjs(backendIds, type, typeClass);
    return fetchObjsPost(backendResult, r);
  }

  private <T extends Obj> ObjId[] fetchObjsPre(ObjId[] ids, T[] r, ObjId[] backendIds) {
    readLock();
    try {
      for (int i = 0; i < ids.length; i++) {
        ObjId id = ids[i];
        if (id == null) {
          continue;
        }
        Obj o = pendingObj(id);
        if (o != null) {
          @SuppressWarnings("unchecked")
          T typed = (T) o;
          r[i] = typed;
        } else {
          if (backendIds == null) {
            backendIds = new ObjId[ids.length];
          }
          backendIds[i] = id;
        }
      }
    } finally {
      readUnlock();
    }
    return backendIds;
  }

  private static <T extends Obj> T[] fetchObjsPost(T[] backendResult, T[] r) {
    for (int i = 0; i < backendResult.length; i++) {
      T o = backendResult[i];
      if (o != null) {
        r[i] = o;
      }
    }
    return r;
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {

    ObjId[] backendIds = null;
    @SuppressWarnings("unchecked")
    T[] r = (T[]) Array.newInstance(typeClass, ids.length);

    backendIds = fetchObjsPre(ids, r, backendIds);

    if (backendIds == null) {
      return r;
    }

    T[] backendResult = delegate().fetchTypedObjsIfExist(backendIds, type, typeClass);
    return fetchObjsPost(backendResult, r);
  }

  @Override
  public void deleteObj(@Nonnull @javax.annotation.Nonnull ObjId id) {
    writeLock();
    try {
      delegate().deleteObj(id);
      pendingStores.remove(id);
      pendingUpserts.remove(id);
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void deleteObjs(@Nonnull @javax.annotation.Nonnull ObjId[] ids) {
    writeLock();
    try {
      for (ObjId id : ids) {
        if (id != null) {
          deleteObj(id);
        }
      }
    } finally {
      writeUnlock();
    }
  }

  @Override
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean updateConditional(
      @Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void erase() {
    writeLock();
    try {
      pendingStores.clear();
      pendingUpserts.clear();
      delegate().erase();
    } finally {
      writeUnlock();
    }
  }

  @Override
  public int hardObjectSizeLimit() {
    return delegate().hardObjectSizeLimit();
  }

  @Override
  public int effectiveIndexSegmentSizeLimit() {
    return delegate().effectiveIndexSegmentSizeLimit();
  }

  @Override
  public int effectiveIncrementalIndexSizeLimit() {
    return delegate().effectiveIncrementalIndexSizeLimit();
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public String name() {
    return delegate().name();
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public StoreConfig config() {
    return delegate().config();
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public Reference addReference(@Nonnull @javax.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
    return delegate().addReference(reference);
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public Reference markReferenceAsDeleted(@Nonnull @javax.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    return delegate().markReferenceAsDeleted(reference);
  }

  @Override
  public void purgeReference(@Nonnull @javax.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    delegate().purgeReference(reference);
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public Reference updateReferencePointer(
      @Nonnull @javax.annotation.Nonnull Reference reference,
      @Nonnull @javax.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    return delegate().updateReferencePointer(reference, newPointer);
  }

  @Override
  @Nullable
  @javax.annotation.Nullable
  public Reference fetchReference(@Nonnull @javax.annotation.Nonnull String name) {
    return delegate().fetchReference(name);
  }

  @Override
  @Nullable
  @javax.annotation.Nullable
  public Reference fetchReferenceForUpdate(@Nonnull @javax.annotation.Nonnull String name) {
    return delegate().fetchReferenceForUpdate(name);
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public Reference[] fetchReferences(@Nonnull @javax.annotation.Nonnull String[] names) {
    return delegate().fetchReferences(names);
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public Reference[] fetchReferencesForUpdate(@Nonnull @javax.annotation.Nonnull String[] names) {
    return delegate().fetchReferencesForUpdate(names);
  }

  @Override
  @Nonnull
  @javax.annotation.Nonnull
  public CloseableIterator<Obj> scanAllObjects(
      @Nonnull @javax.annotation.Nonnull Set<ObjType> returnedObjTypes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCaching() {
    return delegate().isCaching();
  }
}
