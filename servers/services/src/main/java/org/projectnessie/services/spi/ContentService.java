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

import java.util.List;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;

/**
 * Server-side interface to services managing the loading of content objects.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface ContentService {

  ContentResponse getContent(
      @Valid @jakarta.validation.Valid ContentKey key,
      @Valid
          @jakarta.validation.Valid
          @Nullable
          @jakarta.annotation.Nullable
          @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = REF_NAME_REGEX,
              message = REF_NAME_MESSAGE)
          String namedRef,
      @Valid
          @jakarta.validation.Valid
          @Nullable
          @jakarta.annotation.Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String hashOnRef,
      boolean withDocumentation)
      throws NessieNotFoundException;

  GetMultipleContentsResponse getMultipleContents(
      @Valid
          @jakarta.validation.Valid
          @Nullable
          @jakarta.annotation.Nullable
          @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = REF_NAME_REGEX,
              message = REF_NAME_MESSAGE)
          String namedRef,
      @Valid
          @jakarta.validation.Valid
          @Nullable
          @jakarta.annotation.Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String hashOnRef,
      @Valid @jakarta.validation.Valid @Size @jakarta.validation.constraints.Size(min = 1)
          List<ContentKey> keys,
      boolean withDocumentation)
      throws NessieNotFoundException;
}
