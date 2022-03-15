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
import org.projectnessie.model.Namespace;

public class NamespacesParamsTest {

  @Test
  public void testBuilder() {
    NamespacesParams params = NamespacesParams.builder().refName("xx").build();
    assertThat(params.getRefName()).isEqualTo("xx");
    assertThat(params.getNamespace()).isNull();
    assertThat(params.getHashOnRef()).isNull();
  }

  @Test
  public void testValidation() {
    assertThatThrownBy(() -> NamespacesParams.builder().build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build NamespacesParams, some of required attributes are not set [refName]");

    assertThatThrownBy(() -> NamespacesParams.builder().namespace(Namespace.of("x")).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build NamespacesParams, some of required attributes are not set [refName]");

    assertThatThrownBy(() -> NamespacesParams.builder().hashOnRef("x").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build NamespacesParams, some of required attributes are not set [refName]");

    assertThatThrownBy(() -> NamespacesParams.builder().refName(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("refName");
  }
}
