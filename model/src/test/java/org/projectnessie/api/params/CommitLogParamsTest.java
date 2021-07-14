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

import org.junit.jupiter.api.Test;

public class CommitLogParamsTest {

  @Test
  public void testBuilder() {
    Integer maxRecords = 23;
    String startHash = "1234567890123456";
    String endHash = "00000";
    String pageToken = "aabbcc";
    String queryExpression = "some_expression";
    CommitLogParams params =
        CommitLogParams.builder()
            .expression(queryExpression)
            .maxRecords(maxRecords)
            .pageToken(pageToken)
            .startHash(startHash)
            .endHash(endHash)
            .build();

    assertThat(params.pageToken()).isEqualTo(pageToken);
    assertThat(params.maxRecords()).isEqualTo(maxRecords);
    assertThat(params.queryExpression()).isEqualTo(queryExpression);
    assertThat(params.startHash()).isEqualTo(startHash);
    assertThat(params.endHash()).isEqualTo(endHash);
  }

  @Test
  public void testEmpty() {
    CommitLogParams params = CommitLogParams.empty();
    assertThat(params).isNotNull();
    assertThat(params.maxRecords()).isNull();
    assertThat(params.pageToken()).isNull();
    assertThat(params.queryExpression()).isNull();
    assertThat(params.startHash()).isNull();
    assertThat(params.endHash()).isNull();
  }
}
