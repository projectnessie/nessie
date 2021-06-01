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
import java.util.List;

import org.junit.jupiter.api.Test;

public class CommitLogParamsTest {

  @Test
  public void testWithNullRef() {
    assertThatThrownBy(() -> CommitLogParams.builder().build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("ref must be set");
  }

  @Test
  public void testWithRefOnly() {
    CommitLogParams params = CommitLogParams.builder().ref("some_ref").build();
    assertThat(params.getRef()).isEqualTo("some_ref");
    assertThat(params.getAfter()).isNull();
    assertThat(params.getBefore()).isNull();
    assertThat(params.getCommitters()).isNotNull().isEmpty();
    assertThat(params.getAuthors()).isNotNull().isEmpty();
    assertThat(params.getPageToken()).isNull();
    assertThat(params.getMaxRecords()).isNull();
  }

  @Test
  public void testBuilder() {
    Instant after = Instant.now();
    Instant before = Instant.now();
    List<String> authors = Arrays.asList("author1", "author2");
    List<String> committers = Arrays.asList("committer1", "committer2");
    String ref = "some_ref";
    Integer maxRecords = 23;
    String pageToken = "aabbcc";
    CommitLogParams params = CommitLogParams.builder()
        .ref(ref)
        .authors(authors)
        .committers(committers)
        .after(after)
        .before(before)
        .maxRecords(maxRecords)
        .pageToken(pageToken)
        .build();

    assertThat(params.getRef()).isEqualTo(ref);
    assertThat(params.getAfter()).isEqualTo(after);
    assertThat(params.getBefore()).isEqualTo(before);
    assertThat(params.getAuthors()).containsExactlyElementsOf(authors);
    assertThat(params.getCommitters()).containsExactlyElementsOf(committers);
    assertThat(params.getPageToken()).isEqualTo(pageToken);
    assertThat(params.getMaxRecords()).isEqualTo(maxRecords);
  }
}
