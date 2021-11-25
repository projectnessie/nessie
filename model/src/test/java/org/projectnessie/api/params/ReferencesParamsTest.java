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

public class ReferencesParamsTest {

  @Test
  public void testBuilder() {
    ReferencesParams params =
        ReferencesParams.builder()
            .maxRecords(23)
            .pageToken("abc")
            .fetchAdditionalInfo(true)
            .build();
    assertThat(params.maxRecords()).isEqualTo(23);
    assertThat(params.pageToken()).isEqualTo("abc");
    assertThat(params.isFetchAdditionalInfo()).isTrue();
  }

  @Test
  public void testEmpty() {
    ReferencesParams params = ReferencesParams.empty();
    assertThat(params).isNotNull();
    assertThat(params.isFetchAdditionalInfo()).isFalse();
    assertThat(params.pageToken()).isNull();
    assertThat(params.maxRecords()).isNull();
  }
}
