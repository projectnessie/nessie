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
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;

/**
 * Request builder for "create reference".
 *
 * @since {@link NessieApiV1}
 */
public interface CreateReferenceBuilder {
  /**
   * Sets the name of the reference that contains the hash of the reference-to-be-created.
   *
   * <p>This reference name will be used for authorization purposes to validate that the caller has
   * read access to the provided hash.
   *
   * <p>If not explicitly set, the default branch will be used as the source reference name.
   *
   * <p>The name and hash of the reference to be created is set via {@link #reference(Reference)}.
   *
   * @param sourceRefName is the name of the reference that contains the hash of the
   *     reference-to-be-created.
   */
  CreateReferenceBuilder sourceRefName(
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String sourceRefName);

  /**
   * Sets the name and hash of the reference-to-be-created.
   *
   * <p>For creating a {@link Tag}, hash cannot be null.
   *
   * <p>For creating a {@link Branch}, If the hash is not specified, hash will be set to reference
   * the beginning of time (aka NO_ANCESTOR hash).
   *
   * <p>If the hash is specified, it should be on the {@link #sourceRefName(String)}
   *
   * @param reference is {@link Branch} or {@link Tag} defining the name and hash for the new
   *     reference-to-be-created.
   */
  CreateReferenceBuilder reference(@Valid @NotNull Reference reference);

  /**
   * Example for creating the reference.
   *
   * <p>1. Create a Tag called 'release' from the 'dev' branch at hash
   * 'c26ce632cea616aabbbda2e1cfa82070514eb8b773fb035eaf668e2f0be8f10d'.
   *
   * <pre>{@code
   * CreateReferenceBuilder builder = ..;
   * builder
   * .sourceRefName("dev")
   * .reference(Tag.of("release","c26ce632cea616aabbbda2e1cfa82070514eb8b773fb035eaf668e2f0be8f10d"))
   * .create();
   * }</pre>
   *
   * <p>2. Create a Branch called 'test' from the 'dev' branch at hash
   * 'c26ce632cea616aabbbda2e1cfa82070514eb8b773fb035eaf668e2f0be8f10d'.
   *
   * <pre>{@code
   * CreateReferenceBuilder builder = ..;
   * builder
   * .sourceRefName("dev")
   * .reference(Branch.of("test","c26ce632cea616aabbbda2e1cfa82070514eb8b773fb035eaf668e2f0be8f10d"))
   * .create();
   * }</pre>
   *
   * <p>3. Create a Branch called 'test' from the default branch 'main' at hash
   * 'c26ce632cea616aabbbda2e1cfa82070514eb8b773fb035eaf668e2f0be8f10d'.
   *
   * <pre>{@code
   * CreateReferenceBuilder builder = ..;
   * builder
   * .reference(Branch.of("test","c26ce632cea616aabbbda2e1cfa82070514eb8b773fb035eaf668e2f0be8f10d"))
   * .create();
   * }</pre>
   *
   * <p>4. Create a Branch called 'test' pointing at the beginning of time (without any commits).
   *
   * <pre>{@code
   * CreateReferenceBuilder builder = ..;
   * builder
   * .reference(Branch.of("test",null))
   * .create();
   * }</pre>
   */
  Reference create() throws NessieNotFoundException, NessieConflictException;
}
