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

import org.projectnessie.model.FetchOption;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;

/**
 * Request builder for "get all references".
 *
 * @since {@link NessieApiV1}
 */
public interface GetAllReferencesBuilder
    extends QueryBuilder<GetAllReferencesBuilder>,
        PagingBuilder<GetAllReferencesBuilder, ReferencesResponse, Reference> {

  /**
   * Will fetch additional metadata about {@link org.projectnessie.model.Branch} / {@link
   * org.projectnessie.model.Tag} instances, such as number of commits ahead/behind or the common
   * ancestor in relation to the default branch, and the commit metadata for the HEAD commit.
   *
   * @return {@link GetAllReferencesBuilder}
   * @param fetchOption The option indicating how much info to fetch
   */
  GetAllReferencesBuilder fetch(FetchOption fetchOption);

  /** Legacy API method for backward compatibility. Use {@link #fetch(FetchOption)} instead. */
  @Deprecated
  default GetAllReferencesBuilder fetch(org.projectnessie.api.params.FetchOption fetchOption) {
    return fetch(fetchOption.toModel());
  }

  // Mandatory override (must maintain function signature w/ previous versions)
  @Override
  ReferencesResponse get();
}
