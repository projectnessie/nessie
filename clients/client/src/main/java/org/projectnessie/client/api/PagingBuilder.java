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

import java.util.OptionalInt;
import java.util.stream.Stream;
import org.projectnessie.error.NessieNotFoundException;

public interface PagingBuilder<R extends PagingBuilder<R, RESP, ENTRY>, RESP, ENTRY> {

  /**
   * When using the manual paging, it is recommended to set this parameter, this parameter is
   * configured when necessary via {@link #stream(OptionalInt)}.
   */
  R maxRecords(int maxRecords);

  /**
   * When using the manual paging, pass the {@code token} from the previous request, otherwise don't
   * call this method.
   */
  R pageToken(String pageToken);

  /**
   * Fetches a response chunk (might be one page or complete response depending on use case and
   * parameters), but callers must implement paging on their own, if necessary. If in doubt, use
   * {@link #stream()} instead.
   */
  RESP get() throws NessieNotFoundException;

  /**
   * This is a convenience function that is equivalent to {@link #stream(OptionalInt)} with default
   * page-size-hint and no limit on the number of returned entries.
   */
  default Stream<ENTRY> stream() throws NessieNotFoundException {
    return stream(OptionalInt.empty());
  }

  /**
   * Retrieve entries/results as a Java {@link Stream}, uses automatic paging.
   *
   * @param maxTotalRecords {@link OptionalInt#empty()} for unlimited amount of results or a limit
   */
  Stream<ENTRY> stream(OptionalInt maxTotalRecords) throws NessieNotFoundException;
}
