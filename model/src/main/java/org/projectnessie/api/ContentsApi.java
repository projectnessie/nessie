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
package org.projectnessie.api;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.model.Validation;

public interface ContentsApi {

  /**
   * Get the properties of an object.
   */
  Contents getContents(
      @Valid
          ContentsKey key,
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
          String ref
      ) throws NessieNotFoundException;

  MultiGetContentsResponse getMultipleContents(
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
          String ref,
      @Valid
      @NotNull
          MultiGetContentsRequest request)
      throws NessieNotFoundException;

  /**
   * create/update an object on a specific ref.
   */
  void setContents(
      @Valid
      @NotNull
          ContentsKey key,
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branch,
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @NotNull
          String hash,
          String message,
      @Valid
      @NotNull
          Contents contents)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a single object.
   */
  void deleteContents(
      @Valid
          ContentsKey key,
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branch,
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
          String message
      ) throws NessieNotFoundException, NessieConflictException;
}
