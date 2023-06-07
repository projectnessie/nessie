/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.builder;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
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
import org.projectnessie.model.PaginatedResponse;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;

/**
 * Helper and utility methods around streaming of {@link NessieApiV1} et al.
 *
 * <p>Use the functions that return {@link Stream}s in the builders that implement {@link
 * PagingBuilder}.
 */
public final class StreamingUtil {

  private StreamingUtil() {
    // intentionally blank
  }

  /**
   * Default implementation to return a stream of references, functionally equivalent to calling
   * {@link NessieApiV1#getAllReferences()} with manual paging.
   *
   * @param api The {@link NessieApiV1} to use
   * @param builderCustomizer a Function that takes and returns {@link GetAllReferencesBuilder} that
   *     is passed to the caller of this method. It allows the caller to customize the
   *     GetAllReferencesBuilder with additional parameters (e.g.: filter).
   * @param pageSizeHint A hint sent to the Nessie server to return no more than this amount of
   *     entries in a single response, the server may decide to ignore this hint or return more or
   *     less results. Use {@link OptionalInt#empty()} to omit the hint.
   * @return stream of {@link Reference} objects
   * @deprecated Use {@link GetAllReferencesBuilder#stream()}
   */
  @Deprecated
  public static Stream<Reference> getAllReferencesStream(
      @NotNull @jakarta.validation.constraints.NotNull NessieApiV1 api,
      @NotNull @jakarta.validation.constraints.NotNull
          Function<GetAllReferencesBuilder, GetAllReferencesBuilder> builderCustomizer,
      @NotNull @jakarta.validation.constraints.NotNull OptionalInt pageSizeHint)
      throws NessieNotFoundException {
    GetAllReferencesBuilder builder = builderCustomizer.apply(api.getAllReferences());
    Integer pageSize = pageSizeHint.isPresent() ? pageSizeHint.getAsInt() : null;
    return new ResultStreamPaginator<>(
            ReferencesResponse::getReferences,
            token -> builderWithPaging(builder, pageSize, token).get())
        .generateStream();
  }

  /**
   * Default implementation to return a stream of objects for a ref, functionally equivalent to
   * calling {@link NessieApiV1#getEntries()} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getEntriesStream(ref, OptionalInt.empty())}, if not
   * limited, returns all commit-log entries.
   *
   * @param api The {@link NessieApiV1} to use
   * @param builderCustomizer a Function that takes and returns {@link GetEntriesBuilder} that is
   *     passed to the caller of this method. It allows the caller to customize the
   *     GetEntriesBuilder with additional parameters (e.g.: hashOnRef, filter).
   * @param pageSizeHint A hint sent to the Nessie server to return no more than this amount of
   *     entries in a single response, the server may decide to ignore this hint or return more or
   *     less results. Use {@link OptionalInt#empty()} to omit the hint.
   * @return stream of {@link Entry} objects
   * @deprecated Use {@link GetEntriesBuilder#stream()}
   */
  @Deprecated
  public static Stream<Entry> getEntriesStream(
      @NotNull @jakarta.validation.constraints.NotNull NessieApiV1 api,
      @NotNull @jakarta.validation.constraints.NotNull
          Function<GetEntriesBuilder, GetEntriesBuilder> builderCustomizer,
      @NotNull @jakarta.validation.constraints.NotNull OptionalInt pageSizeHint)
      throws NessieNotFoundException {
    GetEntriesBuilder builder = builderCustomizer.apply(api.getEntries());
    Integer pageSize = pageSizeHint.isPresent() ? pageSizeHint.getAsInt() : null;
    return new ResultStreamPaginator<>(
            EntriesResponse::getEntries, token -> builderWithPaging(builder, pageSize, token).get())
        .generateStream();
  }

  /**
   * Default implementation to return a stream of commit-log entries, functionally equivalent to
   * calling {@link NessieApiV1#getCommitLog()} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getCommitLogStream(ref, OptionalInt.empty())}, if not
   * limited, returns all commit-log entries.
   *
   * @param api The {@link NessieApiV1} to use
   * @param builderCustomizer a Function that takes and returns {@link GetCommitLogBuilder} that is
   *     passed to the caller of this method. It allows the caller to customize the
   *     GetCommitLogBuilder with additional parameters (e.g.: hashOnRef, untilHash, filter,
   *     fetchOption).
   * @param pageSizeHint A hint sent to the Nessie server to return no more than this amount of
   *     entries in a single response, the server may decide to ignore this hint or return more or
   *     less results. Use {@link OptionalInt#empty()} to omit the hint.
   * @return stream of {@link CommitMeta} objects
   * @deprecated Use {@link GetCommitLogBuilder#stream()}
   */
  @Deprecated
  public static Stream<LogEntry> getCommitLogStream(
      @NotNull @jakarta.validation.constraints.NotNull NessieApiV1 api,
      @NotNull @jakarta.validation.constraints.NotNull
          Function<GetCommitLogBuilder, GetCommitLogBuilder> builderCustomizer,
      @NotNull @jakarta.validation.constraints.NotNull OptionalInt pageSizeHint)
      throws NessieNotFoundException {
    GetCommitLogBuilder builder = builderCustomizer.apply(api.getCommitLog());
    Integer pageSize = pageSizeHint.isPresent() ? pageSizeHint.getAsInt() : null;
    return new ResultStreamPaginator<>(
            LogResponse::getLogEntries, token -> builderWithPaging(builder, pageSize, token).get())
        .generateStream();
  }

  /**
   * Default implementation to return a stream of reflog entries, functionally equivalent to calling
   * {@link NessieApiV1#getRefLog()} with manual paging.
   *
   * <p>The {@link Stream} returned by {@code getReflogStream(OptionalInt.empty())}, if not limited,
   * returns all reflog entries.
   *
   * @param api The {@link NessieApiV1} to use
   * @param builderCustomizer a Function that takes and returns {@link GetRefLogBuilder} that is
   *     passed to the caller of this method. It allows the caller to customize the GetRefLogBuilder
   *     with additional parameters (e.g.: fromHash, untilHash, filter).
   * @param pageSizeHint A hint sent to the Nessie server to return no more than this amount of
   *     entries in a single response, the server may decide to ignore this hint or return more or
   *     less results. Use {@link OptionalInt#empty()} to omit the hint.
   * @return stream of {@link RefLogResponse.RefLogResponseEntry} objects
   * @deprecated Use {@link GetRefLogBuilder#stream()}
   */
  @Deprecated
  public static Stream<RefLogResponse.RefLogResponseEntry> getReflogStream(
      @NotNull @jakarta.validation.constraints.NotNull NessieApiV1 api,
      @NotNull @jakarta.validation.constraints.NotNull
          Function<GetRefLogBuilder, GetRefLogBuilder> builderCustomizer,
      @NotNull @jakarta.validation.constraints.NotNull OptionalInt pageSizeHint)
      throws NessieNotFoundException {
    GetRefLogBuilder builder = builderCustomizer.apply(api.getRefLog());
    Integer pageSize = pageSizeHint.isPresent() ? pageSizeHint.getAsInt() : null;
    return new ResultStreamPaginator<>(
            RefLogResponse::getLogEntries,
            token -> builderWithPaging(builder, pageSize, token).get())
        .generateStream();
  }

  private static <B extends PagingBuilder<B, ?, ?>> B builderWithPaging(
      B builder, Integer pageSize, String token) {
    if (pageSize != null) {
      builder.maxRecords(pageSize);
    }
    if (token != null) {
      builder.pageToken(token);
    }
    return builder;
  }

  @FunctionalInterface
  public interface NextPage<R> {
    R getPage(String pageToken) throws NessieNotFoundException;
  }

  public static <ENTRY, RESP extends PaginatedResponse> Stream<ENTRY> generateStream(
      Function<RESP, List<ENTRY>> entriesExtractor, NextPage<RESP> pageFetcher)
      throws NessieNotFoundException {
    return new ResultStreamPaginator<>(entriesExtractor, pageFetcher::getPage).generateStream();
  }
}
