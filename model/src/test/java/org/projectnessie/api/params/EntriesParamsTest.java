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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Contents.Type;

public class EntriesParamsTest {

  @Test
  public void testValidation() {
    assertThatThrownBy(() -> EntriesParams.builder().types(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("types must be non-null");
  }

  @Test
  public void testBuilder() {
    Integer maxRecords = 23;
    String pageToken = "aabbcc";
    String namespace = "a.b.c.d";
    List<String> types =
        Arrays.asList(
            Type.UNKNOWN.toString(),
            Type.ICEBERG_TABLE.toString(),
            Type.DELTA_LAKE_TABLE.toString());
    EntriesParams params =
        EntriesParams.builder()
            .maxRecords(maxRecords)
            .pageToken(pageToken)
            .namespace(namespace)
            .types(types)
            .build();

    assertThat(params.getPageToken()).isEqualTo(pageToken);
    assertThat(params.getMaxRecords()).isEqualTo(maxRecords);
    assertThat(params.getNamespace()).isEqualTo(namespace);
    assertThat(params.getTypes()).isNotNull().isEqualTo(types);
  }

  @Test
  public void testEmpty() {
    EntriesParams params = EntriesParams.empty();
    assertThat(params).isNotNull();
    assertThat(params.getMaxRecords()).isNull();
    assertThat(params.getPageToken()).isNull();
    assertThat(params.getTypes()).isEmpty();
    assertThat(params.getNamespace()).isNull();
  }
}
