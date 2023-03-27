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

import javax.annotation.Nullable;
import javax.validation.constraints.Pattern;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Validation;

/**
 * Server-side interface to services managing the reflog.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
@Deprecated
public interface RefLogService {
  RefLogResponse getRefLog(
      @Nullable
          @jakarta.annotation.Nullable
          @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.HASH_REGEX,
              message = Validation.HASH_MESSAGE)
          String startHash,
      @Nullable
          @jakarta.annotation.Nullable
          @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.HASH_REGEX,
              message = Validation.HASH_MESSAGE)
          String endHash,
      @Nullable @jakarta.annotation.Nullable String filter,
      @Nullable @jakarta.annotation.Nullable Integer maxRecords,
      @Nullable @jakarta.annotation.Nullable String pageToken)
      throws NessieNotFoundException;
}
