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
package org.projectnessie.api;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.projectnessie.api.params.RefLogParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;

public interface RefLogApi {

  // Note: When substantial changes in Nessie API (this and related interfaces) are made
  // the API version number reported by NessieConfiguration.getMaxSupportedApiVersion()
  // should be increased as well.

  /**
   * Retrieve the reflog, potentially truncated by the backend.
   *
   * <p>Retrieves up to {@code maxRecords} refLog-entries starting at the HEAD of current-reflog.
   * The backend <em>may</em> respect the given {@code max} records hint, but return less or more
   * entries. Backends may also cap the returned entries at a hard-coded limit, the default REST
   * server implementation has such a hard-coded limit.
   *
   * <p>Invoking {@code getRefLog()} does <em>not</em> guarantee to return all refLog entries
   * because the result can be truncated by the backend.
   *
   * <p>To implement paging, check {@link RefLogResponse#isHasMore() RefLogResponse.isHasMore()}
   * and, if {@code true}, pass the value of {@link RefLogResponse#getToken()
   * RefLogResponse.getToken()} in the next invocation of {@code getRefLog()} as the {@code
   * pageToken} parameter.
   *
   * <p>See {@code org.projectnessie.client.StreamingUtil} in {@code nessie-client}.
   *
   * @return {@link RefLogResponse}
   */
  RefLogResponse getRefLog(@Valid @NotNull RefLogParams params) throws NessieNotFoundException;
}
