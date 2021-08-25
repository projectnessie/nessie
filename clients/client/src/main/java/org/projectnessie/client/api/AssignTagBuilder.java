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
import javax.validation.constraints.Pattern;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;

public interface AssignTagBuilder {
  AssignTagBuilder tagName(
      @NotNull @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String tagName);

  AssignTagBuilder oldHash(
      @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String oldHash);

  /**
   * Convenience for {@link #tagName(String) tagName(tag.getName())}{@code .}{@link #oldHash(String)
   * oldHash(tag.getHash())}.
   */
  default AssignTagBuilder tag(Tag tag) {
    return tagName(tag.getName()).oldHash(tag.getHash());
  }

  AssignTagBuilder assignTo(@Valid @NotNull Tag assignTo);

  void submit() throws NessieNotFoundException, NessieConflictException;
}
