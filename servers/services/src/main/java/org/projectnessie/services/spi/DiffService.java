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

import static org.projectnessie.api.v1.params.DiffParams.HASH_OPTIONAL_REGEX;

import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.Validation;
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
      @NotNull
          @jakarta.validation.constraints.NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.REF_NAME_REGEX,
              message = Validation.REF_NAME_MESSAGE)
          String fromRef,
      @Nullable
          @jakarta.annotation.Nullable
          @Pattern(regexp = HASH_OPTIONAL_REGEX, message = Validation.HASH_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = HASH_OPTIONAL_REGEX,
              message = Validation.HASH_MESSAGE)
          String fromHash,
      @NotNull
          @jakarta.validation.constraints.NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.REF_NAME_REGEX,
              message = Validation.REF_NAME_MESSAGE)
          String toRef,
      @Nullable
          @jakarta.annotation.Nullable
          @Pattern(regexp = HASH_OPTIONAL_REGEX, message = Validation.HASH_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = HASH_OPTIONAL_REGEX,
              message = Validation.HASH_MESSAGE)
          String toHash,
      @Nullable @jakarta.annotation.Nullable String pagingToken,
      PagedResponseHandler<R, DiffEntry> pagedResponseHandler,
      Consumer<WithHash<NamedRef>> fromReference,
      Consumer<WithHash<NamedRef>> toReference,
      @Nullable @jakarta.annotation.Nullable ContentKey minKey,
      @Nullable @jakarta.annotation.Nullable ContentKey maxKey,
      ContentKey prefixKey,
      @Nullable @jakarta.annotation.Nullable List<ContentKey> requestedKeys,
      @Nullable @jakarta.annotation.Nullable String filter)
      throws NessieNotFoundException;
}
