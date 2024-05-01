/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api.v1.params;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class RefLogParamsTest {

  @Test
  public void testBuilder() {
    Integer maxRecords = 23;
    String startHash = "1234567890123456";
    String endHash = "00000";
    String pageToken = "aabbcc";
    String filter = "some_expression";

    Supplier<RefLogParams> generator =
        () ->
            RefLogParams.builder()
                .filter(filter)
                .maxRecords(maxRecords)
                .pageToken(pageToken)
                .startHash(startHash)
                .endHash(endHash)
                .build();

    verify(maxRecords, startHash, endHash, pageToken, filter, generator);
  }

  @Test
  public void testEmpty() {
    verify(null, null, null, null, null, RefLogParams::empty);
  }

  private void verify(
      Integer maxRecords,
      String startHash,
      String endHash,
      String pageToken,
      String filter,
      Supplier<RefLogParams> generator) {
    assertThat(generator.get())
        .isEqualTo(generator.get())
        .extracting(
            RefLogParams::pageToken,
            RefLogParams::maxRecords,
            RefLogParams::filter,
            RefLogParams::startHash,
            RefLogParams::endHash,
            RefLogParams::hashCode)
        .containsExactly(
            pageToken, maxRecords, filter, startHash, endHash, generator.get().hashCode());
  }
}
