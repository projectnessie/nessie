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
package org.projectnessie.versioned.storage.cache;

import static org.projectnessie.versioned.storage.cache.CacheBackend.NON_EXISTENT_REFERENCE_SENTINEL;

import jakarta.annotation.Nonnull;
import java.util.Set;
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
import org.projectnessie.versioned.storage.common.persist.UpdateableObj;

class CachingPersistImpl implements Persist {

  final Persist persist;
  final ObjCache cache;

  CachingPersistImpl(Persist persist, ObjCache cache) {
    this.persist = persist;
    this.cache = cache;
  }

  @Override
  @Nonnull
  public Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException {
    Obj o = cache.get(id);
    if (o != null) {
      return o;
    }
    try {
      o = persist.fetchObj(id);
      cache.put(o);
      return o;
    } catch (ObjNotFoundException e) {
      cache.remove(id);
      throw e;
    }
  }

  @Override
  public Obj getImmediate(@Nonnull ObjId id) {
    return cache.get(id);
  }

  @Override
  @Nonnull
  public <T extends Obj> T fetchTypedObj(@Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    Obj o = cache.get(id);
    if (o != null) {
      if (!o.type().equals(type)) {
        throw new ObjNotFoundException(id);
      }
    } else {
      o = persist.fetchTypedObj(id, type, typeClass);
      cache.put(o);
    }
    @SuppressWarnings("unchecked")
    T r = (T) o;
    return r;
  }

  @Override
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    Obj o = cache.get(id);
    if (o != null) {
      return o.type();
    }
    // 'fetchObjType' is used to validate the object-type for objects that are not available.
    // It's not worth to eagerly fetch the whole object and add it to the cache.
    return persist.fetchObjType(id);
  }

  @Override
  @Nonnull
  public Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException {
    ObjId[] backendIds = null;
    Obj[] r = new Obj[ids.length];

    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id == null) {
        continue;
      }
      Obj o = cache.get(id);
      if (o != null) {
        r[i] = o;
      } else {
        if (backendIds == null) {
          backendIds = new ObjId[ids.length];
        }
        backendIds[i] = id;
      }
    }

    if (backendIds == null) {
      return r;
    }

    Obj[] backendResult = persist.fetchObjs(backendIds);
    for (int i = 0; i < backendResult.length; i++) {
      Obj o = backendResult[i];
      if (o != null) {
        r[i] = o;
        cache.put(o);
      }
    }
    return r;
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    if (persist.storeObj(obj, ignoreSoftSizeRestrictions)) {
      cache.put(obj);
      return true;
    }
    return false;
  }

  @Override
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    boolean[] stored = persist.storeObjs(objs);
    for (int i = 0; i < stored.length; i++) {
      if (stored[i]) {
        cache.put(objs[i]);
      }
    }
    return stored;
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    try {
      persist.upsertObj(obj);
    } finally {
      cache.remove(obj.id());
    }
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    try {
      persist.upsertObjs(objs);
    } finally {
      for (Obj obj : objs) {
        if (obj != null) {
          cache.remove(obj.id());
        }
      }
    }
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    try {
      persist.deleteObj(id);
    } finally {
      cache.remove(id);
    }
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    try {
      persist.deleteObjs(ids);
    } finally {
      for (ObjId id : ids) {
        if (id != null) {
          cache.remove(id);
        }
      }
    }
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    try {
      return persist.deleteConditional(obj);
    } finally {
      cache.remove(obj.id());
    }
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    if (persist.updateConditional(expected, newValue)) {
      cache.put(newValue);
      return true;
    } else {
      cache.remove(expected.id());
      return false;
    }
  }

  @Override
  public void erase() {
    try {
      persist.erase();
    } finally {
      cache.clear();
    }
  }

  @Override
  @Nonnull
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return persist.scanAllObjects(returnedObjTypes);
  }

  // plain delegates...

  @Override
  public int hardObjectSizeLimit() {
    return persist.hardObjectSizeLimit();
  }

  @Override
  public int effectiveIndexSegmentSizeLimit() {
    return persist.effectiveIndexSegmentSizeLimit();
  }

  @Override
  public int effectiveIncrementalIndexSizeLimit() {
    return persist.effectiveIncrementalIndexSizeLimit();
  }

  @Override
  @Nonnull
  public StoreConfig config() {
    return persist.config();
  }

  @Override
  @Nonnull
  public String name() {
    return persist.name();
  }

  // References

  @Override
  @Nonnull
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    Reference r = null;
    try {
      return r = persist.addReference(reference);
    } finally {
      if (r != null) {
        cache.putReference(r);
      } else {
        cache.removeReference(reference.name());
      }
    }
  }

  @Override
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    Reference r = null;
    try {
      return r = persist.markReferenceAsDeleted(reference);
    } finally {
      if (r != null) {
        cache.putReference(r);
      } else {
        cache.removeReference(reference.name());
      }
    }
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      persist.purgeReference(reference);
    } finally {
      cache.removeReference(reference.name());
    }
  }

  @Override
  @Nonnull
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    Reference r = null;
    try {
      return r = persist.updateReferencePointer(reference, newPointer);
    } finally {
      if (r != null) {
        cache.putReference(r);
      } else {
        cache.removeReference(reference.name());
      }
    }
  }

  @Override
  public Reference fetchReference(@Nonnull String name) {
    return fetchReferenceInternal(name, false);
  }

  @Override
  public Reference fetchReferenceForUpdate(@Nonnull String name) {
    return fetchReferenceInternal(name, true);
  }

  private Reference fetchReferenceInternal(@Nonnull String name, boolean bypassCache) {
    Reference r = null;
    if (!bypassCache) {
      r = cache.getReference(name);
      if (r == NON_EXISTENT_REFERENCE_SENTINEL) {
        return null;
      }
    }

    if (r == null) {
      r = persist.fetchReferenceForUpdate(name);
      if (r == null) {
        cache.putNegative(name);
      } else {
        cache.putReference(r);
      }
    }
    return r;
  }

  @Override
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    return fetchReferencesInternal(names, false);
  }

  @Override
  @Nonnull
  public Reference[] fetchReferencesForUpdate(@Nonnull String[] names) {
    return fetchReferencesInternal(names, true);
  }

  private Reference[] fetchReferencesInternal(@Nonnull String[] names, boolean bypassCache) {
    Reference[] r = new Reference[names.length];

    String[] backend = null;
    if (!bypassCache) {
      for (int i = 0; i < names.length; i++) {
        String name = names[i];
        if (name != null) {
          Reference cr = cache.getReference(name);
          if (cr != null) {
            if (cr != NON_EXISTENT_REFERENCE_SENTINEL) {
              r[i] = cr;
            }
          } else {
            if (backend == null) {
              backend = new String[names.length];
            }
            backend[i] = name;
          }
        }
      }
    } else {
      backend = names;
    }

    if (backend != null) {
      Reference[] br = persist.fetchReferencesForUpdate(backend);
      for (int i = 0; i < br.length; i++) {
        String name = backend[i];
        if (name != null) {
          Reference ref = br[i];
          if (ref != null) {
            r[i] = ref;
            cache.putReference(ref);
          } else {
            cache.putNegative(name);
          }
        }
      }
    }

    return r;
  }
}
