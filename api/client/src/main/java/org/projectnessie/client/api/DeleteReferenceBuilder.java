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
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

/**
 * Request builder for deleting references.
 *
 * @since {@link NessieApiV2}
 */
public interface DeleteReferenceBuilder<T>
    extends ChangeReferenceBuilder<DeleteReferenceBuilder<Reference>> {

  @SuppressWarnings("unchecked")
  default DeleteReferenceBuilder<Branch> asBranch() {
    refType(Reference.ReferenceType.BRANCH);
    return (DeleteReferenceBuilder<Branch>) this;
  }

  @SuppressWarnings("unchecked")
  default DeleteReferenceBuilder<Tag> asTag() {
    refType(Reference.ReferenceType.TAG);
    return (DeleteReferenceBuilder<Tag>) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  default <R extends Reference> DeleteReferenceBuilder<R> reference(R reference) {
    return (DeleteReferenceBuilder<R>) ChangeReferenceBuilder.super.reference(reference);
  }

  void delete() throws NessieConflictException, NessieNotFoundException;

  /** Deletes the reference and returns its information as it was just before deletion. */
  T getAndDelete() throws NessieNotFoundException, NessieConflictException;
}
