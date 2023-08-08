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
import org.projectnessie.model.Reference;

/**
 * Request builder for assigning references.
 *
 * @since {@link NessieApiV2}
 */
public interface AssignReferenceBuilder extends OnReferenceBuilder<AssignReferenceBuilder> {
  AssignReferenceBuilder assignTo(
      @Valid @jakarta.validation.Valid @NotNull @jakarta.validation.constraints.NotNull
          Reference assignTo);

  AssignReferenceBuilder refType(Reference.ReferenceType referenceType);

  @Override
  default AssignReferenceBuilder reference(Reference reference) {
    refType(reference.getType());
    return OnReferenceBuilder.super.reference(reference);
  }

  void assign() throws NessieNotFoundException, NessieConflictException;

  /** Assigns the reference to the specified hash and returns its updated information. */
  Reference assignAndGet() throws NessieNotFoundException, NessieConflictException;
}
