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
package org.projectnessie.api.params;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CommitLogParamsTest {

  @Test
  public void testValidation() {
    assertThatThrownBy(() -> CommitLogParams.builder().authors(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("authors must be non-null");
    assertThatThrownBy(
            () ->
                CommitLogParams.builder().authors(Collections.emptyList()).committers(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("committers must be non-null");
  }

  @Test
  public void testBuilder() {
    Instant after = Instant.now();
    Instant before = Instant.now();
    List<String> authors = Arrays.asList("author1", "author2");
    List<String> committers = Arrays.asList("committer1", "committer2");
    Integer maxRecords = 23;
    String pageToken = "aabbcc";
    CommitLogParams params =
        CommitLogParams.builder()
            .authors(authors)
            .committers(committers)
            .after(after)
            .before(before)
            .maxRecords(maxRecords)
            .pageToken(pageToken)
            .build();

    assertThat(params.getAfter()).isEqualTo(after);
    assertThat(params.getBefore()).isEqualTo(before);
    assertThat(params.getAuthors()).containsExactlyElementsOf(authors);
    assertThat(params.getCommitters()).containsExactlyElementsOf(committers);
    assertThat(params.getPageToken()).isEqualTo(pageToken);
    assertThat(params.getMaxRecords()).isEqualTo(maxRecords);
  }

  @Test
  public void testEmpty() {
    CommitLogParams params = CommitLogParams.empty();
    assertThat(params).isNotNull();
    assertThat(params.getMaxRecords()).isNull();
    assertThat(params.getPageToken()).isNull();
    assertThat(params.getCommitters()).isEmpty();
    assertThat(params.getAuthors()).isEmpty();
    assertThat(params.getAfter()).isNull();
    assertThat(params.getBefore()).isNull();
  }
}
