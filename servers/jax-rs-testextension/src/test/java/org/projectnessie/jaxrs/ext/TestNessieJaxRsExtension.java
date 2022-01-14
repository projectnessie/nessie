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

import java.net.URI;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
class TestNessieJaxRsExtension {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static NessieJaxRsExtension server = new NessieJaxRsExtension(() -> databaseAdapter);

  private static NessieApiV1 api;

  @BeforeAll
  static void setupClient(@NessieUri URI uri) {
    assertThat(databaseAdapter).isNotNull();
    api = HttpClientBuilder.builder().withUri(uri).build(NessieApiV1.class);
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
