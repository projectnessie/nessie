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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Validation;

/**
 * Request builder for "get reference".
 *
 * @since {@link NessieApiV1}
 */
public interface GetReferenceBuilder {
  GetReferenceBuilder refName(
      @NotNull @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String refName);

  /**
   * Will fetch additional metadata about {@link org.projectnessie.model.Branch} instances, such as
   * number of commits ahead/behind or the common ancestor in relation to the default branch, and
   * the commit metadata for the HEAD commit.
   *
   * @return {@link GetReferenceBuilder}
   * @param fetchOption The option indicating how much info to fetch
   */
  GetReferenceBuilder fetch(FetchOption fetchOption);

  Reference get() throws NessieNotFoundException;
}
