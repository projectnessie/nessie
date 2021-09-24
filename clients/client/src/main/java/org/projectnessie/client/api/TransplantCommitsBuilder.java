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

import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Validation;

/**
 * Request builder for "transplant commits".
 *
 * @since {@link NessieApiV1}
 */
public interface TransplantCommitsBuilder extends OnBranchBuilder<TransplantCommitsBuilder> {
  TransplantCommitsBuilder message(String message);

  TransplantCommitsBuilder fromRefName(
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String fromRefName);

  TransplantCommitsBuilder hashesToTransplant(
      @NotNull @Size(min = 1) List<String> hashesToTransplant);

  void transplant() throws NessieNotFoundException, NessieConflictException;
}
