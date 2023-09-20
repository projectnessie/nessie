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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.builder.StreamingUtil.ResultStreamPaginator;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.PaginatedResponse;

class TestResultStreamPaginator {
  @Test
  void testNotFoundException() {
    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> {
              throw new NessieReferenceNotFoundException("Ref not found");
            });
    assertThatThrownBy(paginator::generateStream)
        .isInstanceOf(NessieReferenceNotFoundException.class)
        .hasMessage("Ref not found");
  }

  @Test
  void testNoPageSizeHint1Page() throws Exception {
    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> {
              assertNull(token);
              return new MockPaginatedResponse(false, null, Arrays.asList("1", "2", "3"));
            });
    assertThat(paginator.generateStream()).containsExactly("1", "2", "3");
  }

  @Test
  void testNoPageSizeHint2Pages() throws Exception {
    Iterator<String> expectedTokens = Arrays.asList(null, "token").iterator();
    Iterator<MockPaginatedResponse> responses =
        Arrays.asList(
                new MockPaginatedResponse(true, "token", Arrays.asList("1", "2", "3")),
                new MockPaginatedResponse(false, null, Arrays.asList("4", "5", "6")))
            .iterator();

    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> {
              assertEquals(expectedTokens.next(), token);
              return responses.next();
            });
    assertThat(paginator.generateStream()).containsExactly("1", "2", "3", "4", "5", "6");
  }

  @Test
  void testPageSizeHint1Page() throws Exception {
    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> {
              assertNull(token);
              return new MockPaginatedResponse(false, null, Arrays.asList("1", "2", "3"));
            });
    assertThat(paginator.generateStream()).containsExactly("1", "2", "3");
  }

  @Test
  void testPageSizeHint2Pages() throws Exception {
    Iterator<String> expectedTokens = Arrays.asList(null, "token").iterator();
    Iterator<MockPaginatedResponse> responses =
        Arrays.asList(
                new MockPaginatedResponse(true, "token", Arrays.asList("1", "2", "3")),
                new MockPaginatedResponse(false, null, Arrays.asList("4", "5", "6")))
            .iterator();

    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> {
              assertEquals(expectedTokens.next(), token);
              return responses.next();
            });
    assertThat(paginator.generateStream()).containsExactly("1", "2", "3", "4", "5", "6");
  }

  @Test
  void testEmptyResult() throws Exception {
    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> new MockPaginatedResponse(false, null, Collections.emptyList()));
    assertThat(paginator.generateStream()).isEmpty();
  }

  @Test
  void testEmptyResultInPage() throws Exception {
    Iterator<String> expectedTokens = Arrays.asList(null, "token").iterator();
    Iterator<MockPaginatedResponse> responses =
        Arrays.asList(
                new MockPaginatedResponse(true, "token", Arrays.asList("1", "2", "3")),
                new MockPaginatedResponse(false, null, Collections.emptyList()))
            .iterator();

    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> {
              assertEquals(expectedTokens.next(), token);
              return responses.next();
            });
    assertThat(paginator.generateStream()).containsExactly("1", "2", "3");
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void testEmptyResultButMoreInPage() {
    Iterator<String> expectedTokens = Arrays.asList(null, "token").iterator();
    Iterator<MockPaginatedResponse> responses =
        Arrays.asList(
                new MockPaginatedResponse(true, "token", Arrays.asList("1", "2", "3")),
                new MockPaginatedResponse(true, "wtf", Collections.emptyList()))
            .iterator();

    ResultStreamPaginator<MockPaginatedResponse, String> paginator =
        new ResultStreamPaginator<>(
            MockPaginatedResponse::getElements,
            token -> {
              assertEquals(expectedTokens.next(), token);
              return responses.next();
            });
    assertThatThrownBy(() -> paginator.generateStream().collect(Collectors.toList()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Backend returned empty page, but indicates there are more results");
  }

  private static class MockPaginatedResponse implements PaginatedResponse {
    private final boolean more;
    private final String token;
    private final List<String> elements;

    public MockPaginatedResponse(boolean more, String token, List<String> elements) {
      this.more = more;
      this.token = token;
      this.elements = elements;
    }

    List<String> getElements() {
      return elements;
    }

    @Override
    public boolean isHasMore() {
      return more;
    }

    @Nullable
    @jakarta.annotation.Nullable
    @Override
    public String getToken() {
      return token;
    }
  }
}
