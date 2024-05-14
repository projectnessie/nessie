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
package org.projectnessie.versioned.storage.common.persist;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;

/**
 * Low-level storage layer interface providing <em>low-level</em> functionality to manage references
 * and objects.
 *
 * <p>References should be managed (created, deleted, re-assigned) via {@link ReferenceLogic}.
 */
public interface Persist {

  default int hardObjectSizeLimit() {
    return Integer.MAX_VALUE;
  }

  default int effectiveIndexSegmentSizeLimit() {
    return Math.min(config().maxSerializedIndexSize(), hardObjectSizeLimit() / 2);
  }

  default int effectiveIncrementalIndexSizeLimit() {
    return Math.min(config().maxIncrementalIndexSize(), hardObjectSizeLimit() / 2);
  }

  @Nonnull
  String name();

  @Nonnull
  StoreConfig config();

  // References

  /**
   * Low-level, atomically persists the given reference.
   *
   * <p><em>Do not use this function from service implementations, use {@link ReferenceLogic}
   * instead!</em>
   *
   * @throws RefAlreadyExistsException if a reference with the same name already exists
   */
  @Nonnull
  Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException;

  /**
   * Low-level, atomically marks the given reference as deleted, if it exists.
   *
   * <p><em>Do not use this function from service implementations, use {@link ReferenceLogic}
   * instead!</em>
   *
   * @return if the reference exists and if the current persisted reference is not marked as {@link
   *     Reference#deleted()} and equal to {@code reference}, return the reference object marked as
   *     {@link Reference#deleted()}. Returns {@code null} otherwise.
   * @throws RefNotFoundException if a reference with the same name does not exist
   * @throws RefConditionFailedException if the existing reference is already deleted its pointer is
   *     different
   */
  @Nonnull
  Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException;

  /**
   * Low-level, atomically deletes the given reference from the database, if {@link
   * Reference#deleted()} is {@code true} and equal to {@code reference}.
   *
   * <p><em>Do not use this function from service implementations, use {@link ReferenceLogic}
   * instead!</em>
   *
   * @throws RefNotFoundException if a reference with the same name does not exist
   * @throws RefConditionFailedException if the existing reference is not marked as deleted or its
   *     pointer is different
   */
  void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException;

  /**
   * Low-level, atomically updates the given reference's {@link Reference#pointer()} to the new
   * value, if and only if the current persisted reference is not marked as {@link
   * Reference#deleted()} and equal to {@code reference}.
   *
   * <p><em>Do not use this function from service implementations, use {@link ReferenceLogic}
   * instead!</em>
   *
   * @return the updated {@link Reference}, if the reference exists, is not marked as {@link
   *     Reference#deleted() deleted} and the {@link Reference#pointer()} update succeeded.
   * @throws RefNotFoundException if a reference with the same name does not exist
   * @throws RefConditionFailedException if the existing reference is marked as deleted or its
   *     pointer is different
   */
  @Nonnull
  Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException;

  /**
   * Low-level, find a reference.
   *
   * <p><em>Do not use this function from service implementations, use {@link ReferenceLogic}
   * instead!</em>
   *
   * @return the reference or {@code null}, if it does not exist
   */
  @Nullable
  Reference fetchReference(@Nonnull String name);

  /**
   * Like {@link #fetchReference(String)}, but finds multiple references by name at once, leveraging
   * bulk queries against databases.
   *
   * <p>Non-existing references are returned as {@code null} elements in the returned array.
   *
   * <p><em>Do not use this function from service implementations, use {@link ReferenceLogic}
   * instead!</em>
   */
  @Nonnull
  Reference[] fetchReferences(@Nonnull String[] names);

  // Objects

  /**
   * Fetches a single object.
   *
   * <p>If the type of the requested object is known, prefer {@link #fetchTypedObj(ObjId, ObjType,
   * Class)}.
   *
   * @return The returned object will be a concrete type according to its {@link ObjType},
   *     (de)serialization is handled by the database specific implementation of {@link Persist}.
   * @throws ObjNotFoundException with the ID for which no {@link Obj objects} exist
   * @see #fetchObjType(ObjId)
   * @see #fetchTypedObj(ObjId, ObjType, Class)
   * @see #fetchObjs(ObjId[])
   */
  @Nonnull
  Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException;

  default Obj getImmediate(@Nonnull ObjId id) {
    return null;
  }

  /**
   * Retrieves the object with ID {@code id}, having the same {@link ObjType type}.
   *
   * @return the object with the requested type
   * @throws ObjNotFoundException with the ID for which no matching {@link Obj objects} exist,
   *     either the no object with the given ID exists or that object is not of the requested type
   * @see #fetchObjType(ObjId)
   * @see #fetchObj(ObjId)
   * @see #fetchObjs(ObjId[])
   */
  @Nonnull
  <T extends Obj> T fetchTypedObj(@Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException;

  /**
   * Retrieves the type of the object with ID {@code id}.
   *
   * @return the object's type
   * @throws ObjNotFoundException with the ID for which no {@link Obj objects} exist
   * @see #fetchObj(ObjId)
   * @see #fetchTypedObj(ObjId, ObjType, Class)
   * @see #fetchObjs(ObjId[])
   */
  @Nonnull
  ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException;

  /**
   * Like {@link #fetchObj(ObjId)}, but finds multiple objects by name at once, leveraging bulk
   * queries against databases.
   *
   * <p>The behavior when providing the same {@link ObjId} multiple times in {@code ids} is
   * undefined, implementations may return the same object or equal objects or throw exceptions. In
   * any case, providing the same ID multiple times is discouraged.
   *
   * @param ids array with {@link ObjId}s to fetch. {@code null} array elements are legal, the
   *     corresponding elements in the returned array will be {@code null} as well.
   * @return The returned objects will be a concrete types according to their {@link ObjType},
   *     (de)serialization is handled by the database specific implementation of {@link Persist}.
   *     Elements are {@code null}, if the corresponding elements in the {@code ids} parameter array
   *     are {@code null}.
   * @throws ObjNotFoundException with the IDs for which no {@link Obj objects} exist
   * @see #fetchObjType(ObjId)
   * @see #fetchTypedObj(ObjId, ObjType, Class)
   * @see #fetchObj(ObjId)
   */
  @Nonnull
  Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException;

  /**
   * Stores the given object as a new record.
   *
   * <p>This is a very low level persist operation. Prefer the persist operations on the various
   * logic interfaces to ensure a stable and deterministic ID generation.
   *
   * @param obj the object to store
   * @return {@code true}, if the object was stored as a new record or {@code false} if an object
   *     with the same ID already exists.
   * @throws ObjTooLargeException thrown when a hard database row/item size limit has been hit, or a
   *     "soft" size restriction in {@link #config()}
   * @see #storeObjs(Obj[])
   */
  default boolean storeObj(@Nonnull Obj obj) throws ObjTooLargeException {
    return storeObj(obj, false);
  }

  /**
   * Stores the given object as a new record, variant of {@link #storeObj(Obj)} that explicitly
   * allows ignoring soft object/attribute size restrictions.
   *
   * <p>This is a very low level persist operation. Prefer the persist operations on the various
   * logic interfaces to ensure a stable and deterministic ID generation.
   *
   * @param obj the object to store
   * @param ignoreSoftSizeRestrictions whether to explicitly ignore soft size restrictions, use
   *     {@code false}, if in doubt
   * @return {@code true}, if the object was stored as a new record or {@code false} if an object
   *     with the same ID already exists.
   * @throws ObjTooLargeException thrown when a hard database row/item size limit has been hit, or,
   *     if {@code ignoreSoftSizeRestrictions} is {@code false}, a "soft" size restriction in {@link
   *     #config()}
   * @see #storeObjs(Obj[])
   */
  boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException;

  /**
   * Like {@link #storeObj(Obj)}, but stores multiple objects at once.
   *
   * <p>Providing the same ID multiple times via the {@code objs} to store is not supported.
   * Implementations may or may not fail, the behavior is undefined in this case.
   *
   * <p>In case an object failed to be stored, it is undefined whether other objects have been
   * stored or not.
   *
   * @param objs array with {@link Obj}s to store. {@code null} array elements are legal, the
   *     corresponding elements in the returned array will be {@code false}.
   * @return an array with {@code boolean}s indicating whether the corresponding objects were
   *     created ({@code true}) or already present ({@code false}), see {@link #storeObj(Obj)}
   * @throws ObjTooLargeException thrown when a hard database row/item size limit has been hit, or a
   *     "soft" size restriction in {@link #config()}
   * @see #storeObj(Obj)
   */
  @Nonnull
  boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException;

  void deleteObj(@Nonnull ObjId id);

  /**
   * Deletes multiple objects,
   *
   * <p>In case an object failed to be deleted, it is undefined whether other objects have been
   * deleted or not.
   *
   * @param ids array with {@link ObjId}s to delete. {@code null} array elements are legal.
   */
  void deleteObjs(@Nonnull ObjId[] ids);

  /**
   * Deletes the object, if the current state in the database is equal to the given state, comparing
   * the {@link UpdateableObj#versionToken()}.
   *
   * @return {@code true}, if the conditional delete succeeded.
   */
  boolean deleteConditional(@Nonnull UpdateableObj obj);

  /**
   * Updates the object, if the current state in the database is equal to the {@code expected}
   * state, comparing the {@link UpdateableObj#versionToken()}.
   *
   * @return {@code true}, if the conditional update succeeded.
   */
  boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException;

  /**
   * Updates an existing object or inserts it as a new object, used only for maintenance operations,
   * never for production code. The "user facing semantics" of an object <em>must not</em> change.
   *
   * @see #upsertObjs (Obj[])
   * @throws ObjTooLargeException thrown when a hard database row/item size limit has been hit
   */
  void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException;

  /**
   * Updates existing objects or inserts those as a new objects, used only for maintenance
   * operations, never for production code. The "user facing semantics" of an object <em>must
   * not</em> change.
   *
   * <p>In case an object failed to be updated, it is undefined whether other objects have been
   * updated or not.
   *
   * @param objs array with {@link Obj}s to upsert. {@code null} array elements are legal.
   * @see #upsertObj( Obj)
   */
  void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException;

  /**
   * Returns an iterator over all objects that match the given predicate.
   *
   * <p>The returned iterator can hold a reference to database resources, like a JDBC connection +
   * statement + result set, or a RocksDB iterator, etc. This means, that extra care must be taken
   * to close the iterator in every case - at best using a try-finally.
   *
   * <p>It is possible that databases have to scan all rows/items in the tables/collections, which
   * can lead to a <em>very</em> long runtime of this method.
   *
   * @return iterator over all objects, must be closed
   */
  @Nonnull
  CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes);

  /**
   * Erases the whole repository.
   *
   * <p>It is possible that databases have to scan all rows/items in the tables/collections, which
   * can lead to a <em>very</em> long runtime of this method.
   */
  void erase();
}
