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

import java.time.Instant;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Validation;

/**
 * Request builder for "create reference".
 *
 * @since {@link NessieApiV1}
 */
public interface CreateReferenceBuilder {
  CreateReferenceBuilder sourceRefName(
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String sourceRefName);

  CreateReferenceBuilder reference(@Valid @NotNull Reference reference);

  CreateReferenceBuilder expireAt(@Valid @NotNull Instant expireAt);

  /**
   * Creates the reference as configured on this builder. Prefer {@link #createAs(Reference)}
   * instead of {@link #reference(Reference)} plus this method.
   *
   * @return the reference with the committed operations, so with the updated commit hash.
   */
  Reference create() throws NessieNotFoundException, NessieConflictException;

  /**
   * Convenience method to return the created reference with the new HEAD.
   *
   * @param reference the reference as it should be created
   * @param <T> reference type - either a {@link org.projectnessie.model.Branch} or {@link
   *     org.projectnessie.model.Transaction} or {@link org.projectnessie.model.Tag}
   * @return reference object pointing to the commit
   */
  default <T extends Reference> T createAs(T reference)
      throws NessieConflictException, NessieNotFoundException {
    reference(reference);
    @SuppressWarnings("unchecked")
    T created = (T) create();
    return created;
  }
}
