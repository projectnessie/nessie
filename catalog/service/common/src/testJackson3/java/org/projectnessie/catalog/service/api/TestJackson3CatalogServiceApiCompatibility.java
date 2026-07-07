/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.catalog.service.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TestJackson3CatalogServiceApiCompatibility {
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  @Test
  void catalogCommitRoundTrip() throws Exception {
    CatalogCommit commit = CatalogCommit.builder().build();

    String json = MAPPER.writeValueAsString(commit);
    assertThat(MAPPER.readValue(json, CatalogCommit.class)).isEqualTo(commit);
  }
}
