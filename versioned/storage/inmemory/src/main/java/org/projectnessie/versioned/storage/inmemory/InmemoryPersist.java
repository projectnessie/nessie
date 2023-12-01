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
package org.projectnessie.versioned.storage.inmemory;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;

import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
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
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.ValidatingPersist;

class InmemoryPersist implements ValidatingPersist {

  private final InmemoryBackend inmemory;
  private final StoreConfig config;

  InmemoryPersist(InmemoryBackend inmemory, StoreConfig config) {
    this.inmemory = inmemory;
    this.config = config;
  }

  private String compositeKeyRepo() {
    return InmemoryBackend.compositeKeyRepo(config.repositoryId());
  }

  private String compositeKey(String id) {
    checkArgument(!id.isEmpty());
    return config.repositoryId() + ':' + id;
  }

  private String compositeKey(ObjId id) {
    return compositeKey(id.toString());
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return InmemoryBackendFactory.NAME;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public StoreConfig config() {
    return config;
  }

  @Override
  public Reference fetchReference(@Nonnull @jakarta.annotation.Nonnull String name) {
    return inmemory.references.get(compositeKey(name));
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference[] fetchReferences(@Nonnull @jakarta.annotation.Nonnull String[] names) {
    Reference[] r = new Reference[names.length];
    for (int i = 0; i < names.length; i++) {
      String name = names[i];
      if (name != null) {
        r[i] = fetchReference(name);
      }
    }
    return r;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference addReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    Reference ex = inmemory.references.putIfAbsent(compositeKey(reference.name()), reference);
    if (ex != null) {
      throw new RefAlreadyExistsException(ex);
    }
    return reference;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference markReferenceAsDeleted(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    Reference[] result = new Reference[1];

    Reference asDeleted = reference.withDeleted(true);

    inmemory.references.computeIfPresent(
        compositeKey(reference.name()),
        (k, r) -> {
          result[0] = r;
          return r.pointer().equals(reference.pointer()) && !reference.deleted() ? asDeleted : r;
        });

    Reference r = result[0];
    if (r == null) {
      throw new RefNotFoundException(reference);
    }
    if (!r.pointer().equals(reference.pointer()) || r.deleted()) {
      throw new RefConditionFailedException(r);
    }
    return asDeleted;
  }

  @Override
  public void purgeReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    Reference[] result = new Reference[1];
    inmemory.references.computeIfPresent(
        compositeKey(reference.name()),
        (k, r) -> {
          result[0] = r;
          return r.pointer().equals(reference.pointer()) && r.deleted() ? null : r;
        });

    Reference r = result[0];
    if (r == null) {
      throw new RefNotFoundException(reference);
    }
    if (!r.pointer().equals(reference.pointer()) || !r.deleted()) {
      throw new RefConditionFailedException(r);
    }
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference updateReferencePointer(
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    Reference asUpdated = reference.forNewPointer(newPointer, config);

    Reference[] result = new Reference[2];
    Reference c =
        inmemory.references.computeIfPresent(
            compositeKey(reference.name()),
            (k, r) -> {
              if (!r.deleted() && r.equals(reference)) {
                result[0] = r;
                r = asUpdated;
              } else {
                result[1] = r;
              }
              return r;
            });

    if (c == null) {
      throw new RefNotFoundException(reference);
    }
    Reference r = result[0];
    if (r != null) {
      return asUpdated;
    }
    throw new RefConditionFailedException(result[1]);
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Obj fetchObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) throws ObjNotFoundException {
    Obj obj = inmemory.objects.get(compositeKey(id));
    if (obj == null) {
      throw new ObjNotFoundException(id);
    }
    return obj;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull @jakarta.annotation.Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    Obj obj = inmemory.objects.get(compositeKey(id));
    if (obj == null || !obj.type().equals(type)) {
      throw new ObjNotFoundException(id);
    }
    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public ObjType fetchObjType(@Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    Obj obj = inmemory.objects.get(compositeKey(id));
    if (obj == null) {
      throw new ObjNotFoundException(id);
    }
    return obj.type();
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Obj[] fetchObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids)
      throws ObjNotFoundException {
    Obj[] r = new Obj[ids.length];
    List<ObjId> notFound = null;
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id == null) {
        continue;
      }
      try {
        r[i] = fetchObj(id);
      } catch (ObjNotFoundException e) {
        if (notFound == null) {
          notFound = new ArrayList<>();
        }
        notFound.addAll(e.objIds());
      }
    }
    if (notFound != null) {
      throw new ObjNotFoundException(notFound);
    }
    return r;
  }

  @Override
  public boolean storeObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    checkArgument(obj.id() != null, "Obj to store must have a non-null ID");

    if (!ignoreSoftSizeRestrictions) {
      verifySoftRestrictions(obj);
    }

    Obj ex = inmemory.objects.putIfAbsent(compositeKey(obj.id()), obj);
    return ex == null;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public boolean[] storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    boolean[] r = new boolean[objs.length];
    for (int i = 0; i < objs.length; i++) {
      if (objs[i] != null) {
        r[i] = storeObj(objs[i]);
      }
    }
    return r;
  }

  @Override
  public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    inmemory.objects.remove(compositeKey(id));
  }

  @Override
  public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
    for (ObjId id : ids) {
      if (id != null) {
        deleteObj(id);
      }
    }
  }

  @Override
  public void upsertObj(@Nonnull @jakarta.annotation.Nonnull Obj obj) throws ObjTooLargeException {
    verifySoftRestrictions(obj);
    inmemory.objects.put(compositeKey(obj.id()), obj);
  }

  @Override
  public void upsertObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    for (Obj obj : objs) {
      if (obj != null) {
        upsertObj(obj);
      }
    }
  }

  @Override
  public void erase() {
    inmemory.eraseRepositories(singleton(config().repositoryId()));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(
      @Nonnull @jakarta.annotation.Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes::contains);
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Predicate<ObjType> filter;

    ScanAllObjectsIterator(Predicate<ObjType> filter) {
      this.filter = filter;
    }

    final String prefix = compositeKeyRepo();

    Iterator<Map.Entry<String, Obj>> iter = inmemory.objects.entrySet().iterator();

    @Override
    protected Obj computeNext() {
      while (true) {
        if (!iter.hasNext()) {
          return endOfData();
        }

        Map.Entry<String, Obj> entry = iter.next();

        String k = entry.getKey();
        if (!k.startsWith(prefix)) {
          continue;
        }

        Obj o = entry.getValue();
        if (filter.test(o.type())) {
          return o;
        }
      }
    }

    @Override
    public void close() {}
  }
}
