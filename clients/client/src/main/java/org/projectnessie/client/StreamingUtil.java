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
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetRefLogBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.PagingBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.RefLogResponse;
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
   * @param api The {@link NessieApiV1} to use
   * @param builderCustomizer a Consumer of the {@link GetAllReferencesBuilder} that is passed to
   *     the caller of this method. It allows the caller to customize the GetAllReferencesBuilder
   *     with additional parameters (e.g.: filter).
   * @param maxRecords - a maximum number of records in the stream. If it is {@code
   *     Optional.empty()} the stream will be unbounded.
   * @return stream of {@link Reference} objects
   */
  public static Stream<Reference> getAllReferencesStream(
      @NotNull NessieApiV1 api,
      @NotNull Consumer<GetAllReferencesBuilder> builderCustomizer,
      @NotNull OptionalInt maxRecords)
      throws NessieNotFoundException {

    return new ResultStreamPaginator<>(
            ReferencesResponse::getReferences,
            (r, pageSize, token) -> {
              GetAllReferencesBuilder allReferencesBuilder = api.getAllReferences();
              builderCustomizer.accept(allReferencesBuilder);
              return builderWithPaging(allReferencesBuilder, pageSize, token).get();
            })
        .generateStream(null, maxRecords);
  }

  /**
   * Default implementation to return a stream of objects for a ref, functionally equivalent to
   * calling {@link NessieApiV1#getEntries()} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getEntriesStream(ref, OptionalInt.empty())}, if not
   * limited, returns all commit-log entries.
   *
   * @param api The {@link NessieApiV1} to use
   * @param ref a named reference (branch or tag name) or a commit-hash
   * @param builderCustomizer a Consumer of the {@link GetEntriesBuilder} that is passed to the
   *     caller of this method. It allows the caller to customize the GetEntriesBuilder with
   *     additional parameters (e.g.: hashOnRef, filter).
   * @param maxRecords - a maximum number of records in the stream. If it is {@code
   *     Optional.empty()} the stream will be unbounded.
   * @return stream of {@link Entry} objects
   */
  public static Stream<Entry> getEntriesStream(
      @NotNull NessieApiV1 api,
      @NotNull String ref,
      @NotNull Consumer<GetEntriesBuilder> builderCustomizer,
      @NotNull OptionalInt maxRecords)
      throws NessieNotFoundException {

    return new ResultStreamPaginator<>(
            EntriesResponse::getEntries,
            (reference, pageSize, token) -> {
              GetEntriesBuilder builder =
                  builderWithPaging(api.getEntries(), pageSize, token).refName(reference);
              builderCustomizer.accept(builder);
              return builder.get();
            })
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
   * @param builderCustomizer a Consumer of the {@link GetCommitLogBuilder} that is passed to the
   *     caller of this method. It allows the caller to customize the GetCommitLogBuilder with
   *     additional parameters (e.g.: hashOnRef, untilHash, filter).
   * @param maxRecords - a maximum number of records in the stream. If it is {@code
   *     Optional.empty()} the stream will be unbounded.
   * @param fetchAdditionalInfo - specify whether it should fetch {@link FetchOption#ALL} or {@link
   *     FetchOption#MINIMAL}
   * @return stream of {@link CommitMeta} objects
   */
  public static Stream<LogEntry> getCommitLogStream(
      @NotNull NessieApiV1 api,
      String ref,
      @NotNull Consumer<GetCommitLogBuilder> builderCustomizer,
      @NotNull OptionalInt maxRecords,
      boolean fetchAdditionalInfo)
      throws NessieNotFoundException {
    FetchOption fetchOption = fetchAdditionalInfo ? FetchOption.ALL : FetchOption.MINIMAL;
    return new ResultStreamPaginator<>(
            LogResponse::getLogEntries,
            (reference, pageSize, token) -> {
              GetCommitLogBuilder getCommitLogBuilder =
                  builderWithPaging(api.getCommitLog().fetch(fetchOption), pageSize, token)
                      .refName(reference);
              builderCustomizer.accept(getCommitLogBuilder);
              return getCommitLogBuilder.get();
            })
        .generateStream(ref, maxRecords);
  }

  /**
   * Default implementation to return a stream of reflog entries, functionally equivalent to calling
   * {@link NessieApiV1#getRefLog()} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getReflogStream(OptionalInt.empty())}, if not limited,
   * returns all reflog entries.
   *
   * @param api The {@link NessieApiV1} to use
   * @param builderCustomizer a Consumer of the {@link GetRefLogBuilder} that is passed to the
   *     caller of this method. It allows the caller to customize the GetRefLogBuilder with
   *     additional parameters (e.g.: fromHash, untilHash, filter).
   * @param maxRecords - a maximum number of records in the stream. If it is {@code
   *     Optional.empty()} the stream will be unbounded.
   * @return stream of {@link RefLogResponse.RefLogResponseEntry} objects
   */
  public static Stream<RefLogResponse.RefLogResponseEntry> getReflogStream(
      @NotNull NessieApiV1 api,
      @NotNull Consumer<GetRefLogBuilder> builderCustomizer,
      @NotNull OptionalInt maxRecords)
      throws NessieNotFoundException {
    return new ResultStreamPaginator<>(
            RefLogResponse::getLogEntries,
            (reference, pageSize, token) -> {
              GetRefLogBuilder builder = builderWithPaging(api.getRefLog(), pageSize, token);
              builderCustomizer.accept(builder);
              return builder.get();
            })
        .generateStream(null, maxRecords);
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
