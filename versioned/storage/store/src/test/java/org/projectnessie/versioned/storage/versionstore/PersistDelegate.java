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

import java.util.Set;
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
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

public class PersistDelegate implements Persist {
  final Persist delegate;

  PersistDelegate(Persist delegate) {
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
  public Reference markReferenceAsDeleted(@Nonnull @jakarta.annotation.Nonnull Reference reference)
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
  public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    delegate.deleteObj(id);
  }

  @Override
  public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
    delegate.deleteObjs(ids);
  }

  @Override
  public void storeObj(@Nonnull @jakarta.annotation.Nonnull Obj obj) throws ObjTooLargeException {
    delegate.storeObj(obj);
  }

  @Override
  public void storeObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    delegate.storeObj(obj, ignoreSoftSizeRestrictions);
  }

  @Override
  public void storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    delegate.storeObjs(objs);
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
