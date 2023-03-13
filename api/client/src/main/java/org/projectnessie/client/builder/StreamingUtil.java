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
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.PagingBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.PaginatedResponse;

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

  @FunctionalInterface
  public interface PageFetcher<R> {
    R fetchPage(String pageToken) throws NessieNotFoundException;
  }

  public static <ENTRY, RESP extends PaginatedResponse> Stream<ENTRY> generateStream(
      Function<RESP, List<ENTRY>> entriesExtractor, PageFetcher<RESP> pageFetcher)
      throws NessieNotFoundException {
    return new ResultStreamPaginator<>(entriesExtractor, pageFetcher).generateStream();
  }

  /**
   * Internal helper class to implement continuation token driven paging for a result stream.
   *
   * @param <R> REST result page type
   * @param <E> entry type
   */
  static final class ResultStreamPaginator<R extends PaginatedResponse, E> {

    private final Function<R, List<E>> entriesFromResponse;
    private final PageFetcher<R> fetcher;

    ResultStreamPaginator(Function<R, List<E>> entriesFromResponse, PageFetcher<R> fetcher) {
      this.entriesFromResponse = entriesFromResponse;
      this.fetcher = fetcher;
    }

    /**
     * Constructs the stream that uses paging under the covers.
     *
     * <p>This implementation fetches the first page eagerly to propagate {@link
     * NessieNotFoundException}.
     *
     * @return stream of entries
     * @throws NessieNotFoundException propagated from {@link PageFetcher#fetchPage(String)}
     */
    Stream<E> generateStream() throws NessieNotFoundException {
      R firstPage = fetcher.fetchPage(null);
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
                  currentPage = fetcher.fetchPage(pageToken);
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
}
