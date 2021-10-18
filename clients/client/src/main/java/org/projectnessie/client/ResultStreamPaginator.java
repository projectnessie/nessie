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
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.validation.constraints.NotNull;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.PaginatedResponse;

/**
 * Internal helper class to implement paging for a result stream.
 *
 * @param <R> REST result page type
 * @param <E> entry type
 */
final class ResultStreamPaginator<R extends PaginatedResponse, E> {

  @FunctionalInterface
  interface Fetcher<R> {
    /**
     * Fetch the first/next response.
     *
     * @param ref reference
     * @param pageSize page size
     * @param token paging token
     * @return response object
     * @throws NessieNotFoundException if the ref does not exist
     */
    R fetch(@NotNull String ref, Integer pageSize, String token) throws NessieNotFoundException;
  }

  private final Function<R, List<E>> entriesFromResponse;
  private final Fetcher<R> fetcher;

  ResultStreamPaginator(Function<R, List<E>> entriesFromResponse, Fetcher<R> fetcher) {
    this.entriesFromResponse = entriesFromResponse;
    this.fetcher = fetcher;
  }

  /**
   * Constructs the stream that uses paging under the covers.
   *
   * <p>This implementation fetches the first page eagerly to propagate {@link
   * NessieNotFoundException}.
   *
   * @param ref reference
   * @param pageSizeHint page size hint
   * @return stream of entries
   * @throws NessieNotFoundException propagated from {@link Fetcher#fetch(String, Integer, String)}
   */
  Stream<E> generateStream(@NotNull String ref, OptionalInt pageSizeHint)
      throws NessieNotFoundException {
    R firstPage =
        fetcher.fetch(ref, pageSizeHint.isPresent() ? pageSizeHint.getAsInt() : null, null);
    Spliterator<E> spliterator =
        new Spliterator<E>() {
          private String pageToken;
          private boolean eof;
          private int offsetInPage;
          private R currentPage = firstPage;

          @Override
          public boolean tryAdvance(Consumer<? super E> action) {
            if (eof) {
              return false;
            }

            if (currentPage != null
                && entriesFromResponse.apply(currentPage).size() == offsetInPage) {
              // Already returned the last entry in the current page
              if (!currentPage.isHasMore()) {
                eof = true;
                return false;
              }
              pageToken = currentPage.getToken();
              currentPage = null;
              offsetInPage = 0;
            }

            if (currentPage == null) {
              try {
                currentPage =
                    fetcher.fetch(
                        ref, pageSizeHint.isPresent() ? pageSizeHint.getAsInt() : null, pageToken);
                offsetInPage = 0;
                // an empty returned page is probably an error, let's assume something went wrong
                if (entriesFromResponse.apply(currentPage).isEmpty()) {
                  if (currentPage.isHasMore()) {
                    throw new IllegalStateException(
                        "Backend returned empty page, but indicates there are more results");
                  }
                  eof = true;
                  return false;
                }
              } catch (NessieNotFoundException e) {
                eof = true;
                throw new RuntimeException(e);
              }
            }

            action.accept(entriesFromResponse.apply(currentPage).get(offsetInPage++));
            return true;
          }

          @Override
          public Spliterator<E> trySplit() {
            return null;
          }

          @Override
          public long estimateSize() {
            return 0;
          }

          @Override
          public int characteristics() {
            return 0;
          }
        };
    return StreamSupport.stream(spliterator, false);
  }
}
