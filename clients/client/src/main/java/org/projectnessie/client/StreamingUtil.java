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

import java.util.OptionalInt;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.PagingBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;

/** Helper and utility methods around streaming of {@link NessieApiV1} et al. */
public final class StreamingUtil {

  private StreamingUtil() {
    // intentionally blank
  }

  /**
   * Default implementation to return a stream of references, functionally equivalent to calling
   * {@link NessieApiV1#getAllReferences()} with manual paging.
   *
   * @return stream of {@link Reference} objects
   */
  public static Stream<Reference> getAllReferencesStream(
      @NotNull NessieApiV1 api, OptionalInt maxRecords, @Nullable String filter)
      throws NessieNotFoundException {

    return new ResultStreamPaginator<>(
            ReferencesResponse::getReferences,
            (r, pageSize, token) ->
                builderWithPaging(api.getAllReferences().filter(filter), pageSize, token).get())
        .generateStream(null, maxRecords);
  }

  /**
   * Default implementation to return a stream of objects for a ref, functionally equivalent to
   * calling {@link NessieApiV1#getEntries()} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getEntriesStream(ref, OptionalInt.empty())}, if not
   * limited, returns all commit-log entries.
   *
   * @param ref a named reference (branch or tag name) or a commit-hash
   * @return stream of {@link Entry} objects
   */
  public static Stream<Entry> getEntriesStream(
      @NotNull NessieApiV1 api,
      @NotNull String ref,
      @Nullable String hashOnRef,
      @Nullable String filter,
      OptionalInt maxRecords)
      throws NessieNotFoundException {

    return new ResultStreamPaginator<>(
            EntriesResponse::getEntries,
            (ref1, pageSize, token) ->
                builderWithPaging(api.getEntries(), pageSize, token)
                    .refName(ref1)
                    .hashOnRef(hashOnRef)
                    .filter(filter)
                    .get())
        .generateStream(ref, maxRecords);
  }

  /**
   * Default implementation to return a stream of commit-log entries, functionally equivalent to
   * calling {@link NessieApiV1#getCommitLog()} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getCommitLogStream(ref, OptionalInt.empty())}, if not
   * limited, returns all commit-log entries.
   *
   * @param api The {@link NessieApiV1} to use
   * @param ref a named reference (branch or tag name) or a commit-hash
   * @return stream of {@link CommitMeta} objects
   */
  public static Stream<LogEntry> getCommitLogStream(
      @NotNull NessieApiV1 api,
      @NotNull String ref,
      @Nullable String hashOnRef,
      @Nullable String untilHash,
      @Nullable String filter,
      OptionalInt maxRecords,
      boolean fetchAdditionalInfo)
      throws NessieNotFoundException {
    FetchOption fetchOption = fetchAdditionalInfo ? FetchOption.ALL : FetchOption.MINIMAL;
    return new ResultStreamPaginator<>(
            LogResponse::getLogEntries,
            (reference, pageSize, token) ->
                builderWithPaging(api.getCommitLog().fetch(fetchOption), pageSize, token)
                    .refName(reference)
                    .hashOnRef(hashOnRef)
                    .filter(filter)
                    .untilHash(untilHash)
                    .get())
        .generateStream(ref, maxRecords);
  }

  private static <B extends PagingBuilder<B>> B builderWithPaging(
      B builder, Integer pageSize, String token) {
    if (pageSize != null) {
      builder.maxRecords(pageSize);
    }
    if (token != null) {
      builder.pageToken(token);
    }
    return builder;
  }
}
