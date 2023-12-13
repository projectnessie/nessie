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
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import java.util.function.Consumer;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.WithHash;

/**
 * Server-side interface to services providing content differences.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface DiffService {
  <R> R getDiff(
      @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String fromRef,
      @Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String fromHash,
      @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String toRef,
      @Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String toHash,
      @Nullable String pagingToken,
      PagedResponseHandler<R, DiffEntry> pagedResponseHandler,
      Consumer<WithHash<NamedRef>> fromReference,
      Consumer<WithHash<NamedRef>> toReference,
      @Nullable ContentKey minKey,
      @Nullable ContentKey maxKey,
      ContentKey prefixKey,
      @Nullable List<ContentKey> requestedKeys,
      @Nullable String filter)
      throws NessieNotFoundException;
}
