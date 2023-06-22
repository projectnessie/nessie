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
package org.projectnessie.versioned.storage.common.logic;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

/**
 * Provides the actual logic to access and maintain references.
 *
 * <p>Implementations perform create/drop reference recovery when necessary.
 */
public interface ReferenceLogic {

  /**
   * Find multiple references by names, references that do not exist are returned as {@code null}
   * values in the returned list.
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  List<Reference> getReferences(@Nonnull @jakarta.annotation.Nonnull List<String> references);

  @Nonnull
  @jakarta.annotation.Nonnull
  default Reference getReference(@Nonnull @jakarta.annotation.Nonnull String name)
      throws RefNotFoundException {
    List<Reference> refs = getReferences(Collections.singletonList(name));
    Reference ref = refs.get(0);
    if (ref == null) {
      throw new RefNotFoundException(name);
    }
    return ref;
  }

  /**
   * Performs the query against existing references according to the given {@link ReferencesQuery},
   * which should really depend on the serialized result of the query result in a "public API".
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  PagedResult<Reference, String> queryReferences(
      @Nonnull @jakarta.annotation.Nonnull ReferencesQuery referencesQuery);

  /**
   * Creates a new reference with the given name and pointer.
   *
   * @param name name of the reference to create
   * @param pointer pointer of the reference to create
   * @return the created reference
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  Reference createReference(
      @Nonnull @jakarta.annotation.Nonnull String name,
      @Nonnull @jakarta.annotation.Nonnull ObjId pointer,
      @Nullable @jakarta.annotation.Nullable ObjId extendedInfoObj)
      throws RefAlreadyExistsException, RetryTimeoutException;

  /**
   * Deletes a reference.
   *
   * @param name name of the reference to be deleted
   * @param expectedPointer expected pointer of the reference to be deleted
   */
  void deleteReference(
      @Nonnull @jakarta.annotation.Nonnull String name,
      @Nonnull @jakarta.annotation.Nonnull ObjId expectedPointer)
      throws RefNotFoundException, RefConditionFailedException, RetryTimeoutException;

  /**
   * Atomically updates the given reference's {@link Reference#pointer()} to the new value, if and
   * only if the current persisted reference is not marked as {@link Reference#deleted()} and {@link
   * Reference#pointer()} of the given and persisted values are equal.
   *
   * @return the updated {@link Reference}, if the reference exists, is not marked as {@link
   *     Reference#deleted() deleted} and the {@link Reference#pointer()} update succeeded. Returns
   *     {@code null} otherwise.
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  Reference assignReference(
      @Nonnull @jakarta.annotation.Nonnull Reference current,
      @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException;
}
