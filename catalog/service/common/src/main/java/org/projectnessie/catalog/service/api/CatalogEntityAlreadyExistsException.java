/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.api;

import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

public class CatalogEntityAlreadyExistsException extends NessieReferenceConflictException {

  private final Content.Type conflictingType;
  private final Content.Type existingType;
  private final ContentKey conflictingKey;
  private final ContentKey existingKey;
  private final boolean requirement;
  private final boolean rename;

  public CatalogEntityAlreadyExistsException(
      boolean requirement,
      Content.Type conflictingType,
      ContentKey key,
      Content.Type existingType) {
    this(requirement, false, conflict(existingType, key), conflictingType, key, existingType, key);
  }

  public CatalogEntityAlreadyExistsException(
      Content.Type conflictingType,
      ContentKey conflictingKey,
      Content.Type existingType,
      ContentKey existingKey) {
    this(
        false,
        true,
        conflict(existingType, existingKey),
        conflictingType,
        conflictingKey,
        existingType,
        existingKey);
  }

  private CatalogEntityAlreadyExistsException(
      boolean requirement,
      boolean rename,
      Conflict conflict,
      Content.Type conflictingType,
      ContentKey conflictingKey,
      Content.Type existingType,
      ContentKey existingKey) {
    super(ReferenceConflicts.referenceConflicts(conflict), conflict.message(), null);
    this.requirement = requirement;
    this.rename = rename;
    this.conflictingKey = conflictingKey;
    this.conflictingType = conflictingType;
    this.existingType = existingType;
    this.existingKey = existingKey;
  }

  private static Conflict conflict(Content.Type type, ContentKey key) {
    return Conflict.conflict(
        Conflict.ConflictType.KEY_EXISTS, key, type.name() + " already exists: " + key);
  }

  public Content.Type getConflictingType() {
    return conflictingType;
  }

  public ContentKey getConflictingKey() {
    return conflictingKey;
  }

  public Content.Type getExistingType() {
    return existingType;
  }

  public ContentKey getExistingKey() {
    return existingKey;
  }

  public boolean isRequirement() {
    return requirement;
  }

  public boolean isRename() {
    return rename;
  }
}
