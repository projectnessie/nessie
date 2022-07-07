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

import java.util.stream.Stream;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.error.NessieNotFoundException;

public interface PagingBuilder<R extends PagingBuilder<R, RESP, ENTRY>, RESP, ENTRY> {

  /**
   * Sets the maximum number of records to be returned in a <em>ingle</em>s response object from the
   * {@link #get()} method. When using stream() methods this parameter must not be set.
   *
   * <p>Only for manual paging via {@link #get()} - do <em>not</em> call when using any of the
   * {@link #stream()} functions.
   *
   * <p>This setter reflects the OpenAPI parameter {@code maxRecords} in a paged request, for
   * example {@link org.projectnessie.api.params.CommitLogParams} for {@link
   * org.projectnessie.api.TreeApi#getCommitLog(String, CommitLogParams)}.
   */
  R maxRecords(int maxRecords);

  /**
   * Sets the page token from the previous' {@link #get()} method invocation. When using {@link
   * #stream()} methods this parameter must not be set.
   *
   * <p>Only for manual paging via {@link #get()} - do <em>not</em> call when using any of the
   * {@link #stream()} functions.
   *
   * <p>This setter reflects the OpenAPI parameter {@code pageToken} in a paged request, for example
   * {@link org.projectnessie.api.params.CommitLogParams} for {@link
   * org.projectnessie.api.TreeApi#getCommitLog(String, CommitLogParams)}.
   */
  R pageToken(String pageToken);

  /**
   * Advanced usage, for <em>manual</em> paging: fetches a response chunk (might be one page or
   * complete response depending on use case and parameters), but callers must implement paging on
   * their own, if necessary. If in doubt, use {@link #stream()} instead.
   */
  RESP get() throws NessieNotFoundException;

  /**
   * Retrieve entries/results as a Java {@link Stream}, uses automatic paging.
   *
   * <p>Callers must neither use {@link #maxRecords(int)} nor {@link #pageToken(String)}.
   */
  Stream<ENTRY> stream() throws NessieNotFoundException;
}
