/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.client.api;

import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

/**
 * Request builder for deleting references.
 *
 * @since {@link NessieApiV2}
 */
public interface DeleteReferenceBuilder extends OnReferenceBuilder<DeleteReferenceBuilder> {

  DeleteReferenceBuilder refType(Reference.ReferenceType referenceType);

  @Override
  default DeleteReferenceBuilder reference(Reference reference) {
    refType(reference.getType());
    return OnReferenceBuilder.super.reference(reference);
  }

  void delete() throws NessieConflictException, NessieNotFoundException;

  /** Deletes the reference and returns its information as it was just before deletion. */
  Reference getAndDelete() throws NessieNotFoundException, NessieConflictException;
}
