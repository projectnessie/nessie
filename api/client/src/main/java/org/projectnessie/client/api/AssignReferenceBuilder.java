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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

/**
 * Request builder for assigning references.
 *
 * @since {@link NessieApiV2}
 */
public interface AssignReferenceBuilder<T extends Reference>
    extends ChangeReferenceBuilder<AssignReferenceBuilder<Reference>> {

  @SuppressWarnings("unchecked")
  default AssignReferenceBuilder<Branch> asBranch() {
    refType(Reference.ReferenceType.BRANCH);
    return (AssignReferenceBuilder<Branch>) this;
  }

  @SuppressWarnings("unchecked")
  default AssignReferenceBuilder<Tag> asTag() {
    refType(Reference.ReferenceType.TAG);
    return (AssignReferenceBuilder<Tag>) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  default <R extends Reference> AssignReferenceBuilder<R> reference(R reference) {
    return (AssignReferenceBuilder<R>) ChangeReferenceBuilder.super.reference(reference);
  }

  AssignReferenceBuilder<T> assignTo(
      @Valid @jakarta.validation.Valid @NotNull @jakarta.validation.constraints.NotNull
          Reference assignTo);

  void assign() throws NessieNotFoundException, NessieConflictException;

  /** Assigns the reference to the specified hash and returns its updated information. */
  T assignAndGet() throws NessieNotFoundException, NessieConflictException;
}
