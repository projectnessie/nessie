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
import java.util.Set;
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
  public String name() {
    return delegate.name();
  }

  @Override
  @Nonnull
  public StoreConfig config() {
    return delegate.config();
  }

  @Override
  @Nonnull
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    return delegate.addReference(reference);
  }

  @Override
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    return delegate.markReferenceAsDeleted(reference);
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    delegate.purgeReference(reference);
  }

  @Override
  @Nonnull
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    return delegate.updateReferencePointer(reference, newPointer);
  }

  @Override
  public Reference fetchReference(@Nonnull String name) {
    return delegate.fetchReference(name);
  }

  @Override
  public Reference fetchReferenceForUpdate(@Nonnull String name) {
    return delegate.fetchReferenceForUpdate(name);
  }

  @Override
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    return delegate.fetchReferences(names);
  }

  @Override
  @Nonnull
  public Reference[] fetchReferencesForUpdate(@Nonnull String[] names) {
    return delegate.fetchReferencesForUpdate(names);
  }

  @Override
  @Nonnull
  public Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException {
    return delegate.fetchObj(id);
  }

  @Override
  public Obj getImmediate(@Nonnull ObjId id) {
    return delegate.getImmediate(id);
  }

  @Override
  @Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    return delegate.fetchTypedObj(id, type, typeClass);
  }

  @Override
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    return delegate.fetchObjType(id);
  }

  @Override
  @Nonnull
  public Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException {
    return delegate.fetchObjs(ids);
  }

  @Override
  public Obj[] fetchObjsIfExist(@Nonnull ObjId[] ids) {
    return delegate.fetchObjsIfExist(ids);
  }

  @Override
  @Nonnull
  public <T extends Obj> T[] fetchTypedObjs(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    return delegate.fetchTypedObjs(ids, type, typeClass);
  }

  @Override
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
    return delegate.fetchTypedObjsIfExist(ids, type, typeClass);
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj) throws ObjTooLargeException {
    return delegate.storeObj(obj);
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return delegate.storeObj(obj, ignoreSoftSizeRestrictions);
  }

  @Override
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    return delegate.storeObjs(objs);
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    delegate.deleteObj(id);
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    delegate.deleteObjs(ids);
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    delegate.upsertObj(obj);
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    delegate.upsertObjs(objs);
  }

  @Override
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    return delegate.deleteWithReferenced(obj);
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    return delegate.deleteConditional(obj);
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    return delegate.updateConditional(expected, newValue);
  }

  @Override
  @Nonnull
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return delegate.scanAllObjects(returnedObjTypes);
  }

  @Override
  public void erase() {
    delegate.erase();
  }
}
