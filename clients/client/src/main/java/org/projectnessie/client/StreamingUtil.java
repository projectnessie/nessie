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
package org.projectnessie.client;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.projectnessie.api.TreeApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.LogResponse;

/** Helper and utility methods around streaming of {@link TreeApi} et al. */
public final class StreamingUtil {

  private StreamingUtil() {
    // intentionally blank
  }

  /**
   * Default implementation to return a stream of objects for a ref, functionally equivalent to
   * calling {@link TreeApi#getEntries(String, Integer, String, List)} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getEntriesStream(ref, OptionalInt.empty())}, if not
   * limited, returns all commit-log entries.
   *
   * @param ref a named reference (branch or tag name) or a commit-hash
   * @param pageSizeHint page-size hint for the backend
   * @param types types to filter on. Empty list means no filtering.
   * @return stream of {@link Entry} objects
   */
  public static Stream<Entry> getEntriesStream(
      @NotNull TreeApi treeApi, @NotNull String ref, OptionalInt pageSizeHint, List<String> types)
      throws NessieNotFoundException {
    return new ResultStreamPaginator<>(
            EntriesResponse::getEntries,
            (ref1, pageSize, token) -> treeApi.getEntries(ref1, pageSize, token, types))
        .generateStream(ref, pageSizeHint);
  }

  /**
   * Default implementation to return a stream of commit-log entries, functionally equivalent to
   * calling {@link TreeApi#getCommitLog(String, Integer, String)} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getCommitLogStream(ref, OptionalInt.empty())}, if not
   * limited, returns all commit-log entries.
   *
   * @param ref a named reference (branch or tag name) or a commit-hash
   * @param pageSizeHint page-size hint for the backend
   * @return stream of {@link CommitMeta} objects
   */
  public static Stream<CommitMeta> getCommitLogStream(
      @NotNull TreeApi treeApi, @NotNull String ref, OptionalInt pageSizeHint)
      throws NessieNotFoundException {
    return new ResultStreamPaginator<>(LogResponse::getOperations, treeApi::getCommitLog)
        .generateStream(ref, pageSizeHint);
  }
}
