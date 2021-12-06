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

import org.junit.jupiter.api.Test;

public class GetReferenceParamsTest {

  @Test
  public void testBuilder() {
    GetReferenceParams params =
        GetReferenceParams.builder().refName("xx").fetch(FetchOption.ALL).build();
    assertThat(params.getRefName()).isEqualTo("xx");
    assertThat(params.fetchOption()).isEqualTo(FetchOption.ALL);

    params = GetReferenceParams.builder().refName("xx").build();
    assertThat(params.getRefName()).isEqualTo("xx");
    assertThat(params.fetchOption()).isNull();
  }

  @Test
  public void testValidation() {
    assertThatThrownBy(() -> GetReferenceParams.builder().build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("refName must be non-null");

    assertThatThrownBy(() -> GetReferenceParams.builder().refName(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("refName must be non-null");
  }
}
