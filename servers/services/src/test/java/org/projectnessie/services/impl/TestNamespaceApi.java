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
package org.projectnessie.services.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.Namespace.Empty.EMPTY_NAMESPACE;
import static org.projectnessie.services.authz.ApiContext.apiContext;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;

import org.junit.jupiter.api.Test;

public class TestNamespaceApi {

  @Test
  public void emptyNamespaceCreation() {
    NamespaceApiImpl api = new NamespaceApiImpl(null, null, null, null, apiContext("Nessie", 2));
    assertThatThrownBy(() -> api.createNamespace("main", EMPTY_NAMESPACE, API_WRITE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace name must not be empty");
  }
}
