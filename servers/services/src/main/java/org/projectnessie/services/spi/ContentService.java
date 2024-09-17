/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.services.spi;

import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_REGEX;

import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.services.authz.AccessCheckException;
import org.projectnessie.versioned.RequestMeta;

/**
 * Server-side interface to services managing the loading of content objects.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface ContentService {

  /**
   * Retrieves a single content object by its key.
   *
   * @param key the key of the content to retrieve
   * @param namedRef name of the reference
   * @param hashOnRef optional, ID of the commit or a commit specification
   * @param withDocumentation unused, pass {@code false}
   * @param requestMeta if {@code false}, "natural" read access checks will be performed. If {@code
   *     true}, update/create access checks will be performed in addition to the read access checks.
   * @return the content response, if the content object exists
   * @throws NessieNotFoundException if the content object or the reference does not exist
   * @throws AccessCheckException if access checks fail. Note that if the content object does not
   *     exist <em>and</em> the access checks fail, an {@link AccessCheckException} will be thrown,
   *     not a {@link NessieContentNotFoundException}
   */
  ContentResponse getContent(
      @Valid ContentKey key,
      @Valid @Nullable @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String namedRef,
      @Valid
          @Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String hashOnRef,
      boolean withDocumentation,
      RequestMeta requestMeta)
      throws NessieNotFoundException;

  /**
   * Retrieves a one or more content object by key.
   *
   * @param namedRef name of the reference
   * @param hashOnRef optional, ID of the commit or a commit specification
   * @param keys the keys of the content objects to retrieve
   * @param withDocumentation unused, pass {@code false}
   * @param requestMeta if {@code false}, "natural" read access checks will be performed. If {@code
   *     true}, update/create access checks will be performed in addition to the read access checks.
   * @return the existing content objects
   * @throws NessieNotFoundException if the reference does not exist
   * @throws AccessCheckException if access checks fail. Note that if some access check fails, the
   *     function with throw an {@link AccessCheckException} and not return a result.
   */
  GetMultipleContentsResponse getMultipleContents(
      @Valid @Nullable @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String namedRef,
      @Valid
          @Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String hashOnRef,
      @Valid @Size @Size(min = 1) List<ContentKey> keys,
      boolean withDocumentation,
      RequestMeta requestMeta)
      throws NessieNotFoundException;
}
