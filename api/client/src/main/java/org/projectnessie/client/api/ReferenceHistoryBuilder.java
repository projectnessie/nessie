/*
 * Copyright (C) 2023 Dremio
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

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ReferenceHistoryResponse;
import org.projectnessie.model.Validation;

public interface ReferenceHistoryBuilder {
  ReferenceHistoryBuilder refName(
      @NotNull
          @jakarta.validation.constraints.NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.REF_NAME_REGEX,
              message = Validation.REF_NAME_MESSAGE)
          String refName);

  ReferenceHistoryBuilder headCommitsToScan(
      @Nullable @jakarta.annotation.Nullable Integer headCommitsToScan);

  ReferenceHistoryResponse get() throws NessieNotFoundException;
}
