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

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Pattern;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Validation;

/**
 * Request builder for "get commit log".
 *
 * @since {@link NessieApiV1}
 */
public interface GetCommitLogBuilder
    extends QueryBuilder<GetCommitLogBuilder>,
        PagingBuilder<GetCommitLogBuilder, LogResponse, LogResponse.LogEntry>,
        OnReferenceBuilder<GetCommitLogBuilder> {

  /**
   * Will fetch additional metadata about each commit like operations in a commit and parent hash.
   *
   * @return {@link GetAllReferencesBuilder}
   * @param fetchOption The option indicating how much info to fetch
   */
  GetCommitLogBuilder fetch(FetchOption fetchOption);

  /** Legacy API method for backward compatibility. Use {@link #fetch(FetchOption)} instead. */
  @Deprecated
  default GetCommitLogBuilder fetch(org.projectnessie.api.params.FetchOption fetchOption) {
    return fetch(fetchOption.toModel());
  }

  GetCommitLogBuilder untilHash(
      @Nullable @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String untilHash);

  @Override // kept for byte-code compatibility
  LogResponse get() throws NessieNotFoundException;
}
