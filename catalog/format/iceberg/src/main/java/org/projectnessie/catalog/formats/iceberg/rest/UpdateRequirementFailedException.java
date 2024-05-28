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
package org.projectnessie.catalog.formats.iceberg.rest;

import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;

/**
 * Non-public subclass of {@link NessieReferenceConflictException} for reporting Iceberg Requirement
 * failures. This exception is mapped to Iceberg responses based solely on the information exposed
 * by its superclass.
 */
class UpdateRequirementFailedException extends NessieReferenceConflictException {
  UpdateRequirementFailedException(ContentKey key, String message) {
    super(
        ReferenceConflicts.referenceConflicts(
            Conflict.conflict(Conflict.ConflictType.VALUE_DIFFERS, key, message)),
        message,
        null);
  }
}
