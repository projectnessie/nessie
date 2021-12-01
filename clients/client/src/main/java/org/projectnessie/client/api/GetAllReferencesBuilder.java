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

import org.projectnessie.model.ReferencesResponse;

/**
 * Request builder for "get all references".
 *
 * @since {@link NessieApiV1}
 */
public interface GetAllReferencesBuilder
    extends QueryBuilder<GetAllReferencesBuilder>, PagingBuilder<GetAllReferencesBuilder> {

  /**
   * Will fetch additional metadata about {@link org.projectnessie.model.Branch} / {@link
   * org.projectnessie.model.Tag} instances, such as number of commits ahead/behind or the common
   * ancestor in relation to the default branch, and the commit metadata for the HEAD commit.
   *
   * @return {@link GetAllReferencesBuilder}
   */
  GetAllReferencesBuilder fetchAdditionalInfo(boolean fetchAdditionalInfo);

  /**
   * Fetches all references and returns them in a {@link ReferencesResponse} instance.
   *
   * @return Fetches all references and returns them in a {@link ReferencesResponse} instance.
   */
  ReferencesResponse get();
}
