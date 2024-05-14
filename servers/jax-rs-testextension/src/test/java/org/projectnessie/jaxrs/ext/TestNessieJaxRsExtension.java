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
package org.projectnessie.jaxrs.ext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith(PersistExtension.class)
@NessieBackend(InmemoryBackendTestFactory.class)
class TestNessieJaxRsExtension {

  @NessiePersist static Persist persist;

  @RegisterExtension static NessieJaxRsExtension server = jaxRsExtension(() -> persist);

  private static NessieApiV1 api;

  @BeforeAll
  static void setupClient(NessieClientFactory clientFactory) {
    assertThat(persist).isNotNull();
    api = clientFactory.make();
  }

  private void checkServer() throws NessieNotFoundException {
    assertThat(api.getDefaultBranch().getName()).isEqualTo("main");
  }

  @Test
  void topLevel() throws NessieNotFoundException {
    checkServer();
  }

  @Nested
  class Nested1 {
    @Test
    void nestedTest() throws NessieNotFoundException {
      checkServer();
    }

    @Nested
    class NestedTwice {
      @Test
      void nestedTest() throws NessieNotFoundException {
        checkServer();
      }
    }
  }

  @Nested
  class Nested2 {
    @Test
    void nestedTest() throws NessieNotFoundException {
      checkServer();
    }
  }
}
