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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.projectnessie.model.Namespace;

@Execution(ExecutionMode.CONCURRENT)
public class NamespaceParamsTest {

  @Test
  public void testBuilder() {
    Namespace namespace = Namespace.parse("a.b.c");
    NamespaceParams params = NamespaceParams.builder().refName("xx").namespace(namespace).build();
    assertThat(params.getRefName()).isEqualTo("xx");
    assertThat(params.getNamespace()).isEqualTo(namespace);
    assertThat(params.getHashOnRef()).isNull();
  }

  @Test
  public void testValidation() {
    assertThatThrownBy(() -> NamespaceParams.builder().build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build NamespaceParams, some of required attributes are not set [refName, namespace]");

    assertThatThrownBy(() -> NamespaceParams.builder().refName("x").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build NamespaceParams, some of required attributes are not set [namespace]");

    assertThatThrownBy(() -> NamespaceParams.builder().namespace(Namespace.of("x")).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build NamespaceParams, some of required attributes are not set [refName]");

    assertThatThrownBy(() -> NamespaceParams.builder().hashOnRef("x").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build NamespaceParams, some of required attributes are not set [refName, namespace]");

    assertThatThrownBy(() -> NamespaceParams.builder().refName(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("refName");

    assertThatThrownBy(() -> NamespaceParams.builder().namespace(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("namespace");
  }
}
