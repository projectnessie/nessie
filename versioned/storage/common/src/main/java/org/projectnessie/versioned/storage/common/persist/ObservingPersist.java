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
package org.projectnessie.versioned.storage.common.persist;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;

public class ObservingPersist implements Persist {
  private final Persist delegate;

  private static final String PREFIX = "nessie.storage.persist";

  public ObservingPersist(Persist delegate) {
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

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    return delegate.addReference(reference);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    return delegate.markReferenceAsDeleted(reference);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    delegate.purgeReference(reference);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    return delegate.updateReferencePointer(reference, newPointer);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nullable
  public Reference fetchReference(@Nonnull String name) {
    return delegate.fetchReference(name);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    return delegate.fetchReferences(names);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nullable
  public Reference fetchReferenceForUpdate(@Nonnull String name) {
    return delegate.fetchReferenceForUpdate(name);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Reference[] fetchReferencesForUpdate(@Nonnull String[] names) {
    return delegate.fetchReferencesForUpdate(names);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException {
    return delegate.fetchObj(id);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public Obj getImmediate(@Nonnull ObjId id) {
    return delegate.getImmediate(id);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    return delegate.fetchTypedObj(id, type, typeClass);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    return delegate.fetchObjType(id);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException {
    return delegate.fetchObjs(ids);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public Obj[] fetchObjsIfExist(@Nonnull ObjId[] ids) {
    return delegate.fetchObjsIfExist(ids);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public <T extends Obj> T[] fetchTypedObjs(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    return delegate.fetchTypedObjs(ids, type, typeClass);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
    return delegate.fetchTypedObjsIfExist(ids, type, typeClass);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public boolean storeObj(@Nonnull Obj obj) throws ObjTooLargeException {
    return delegate.storeObj(obj);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return delegate.storeObj(obj, ignoreSoftSizeRestrictions);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    return delegate.storeObjs(objs);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void deleteObj(@Nonnull ObjId id) {
    delegate.deleteObj(id);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void deleteObjs(@Nonnull ObjId[] ids) {
    delegate.deleteObjs(ids);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    delegate.upsertObj(obj);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    delegate.upsertObjs(objs);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    return delegate.deleteWithReferenced(obj);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    return delegate.deleteConditional(obj);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    return delegate.updateConditional(expected, newValue);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @Nonnull
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return delegate.scanAllObjects(returnedObjTypes);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void erase() {
    delegate.erase();
  }

  @Override
  public boolean isCaching() {
    return delegate.isCaching();
  }
}
